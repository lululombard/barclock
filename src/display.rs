//! Panel power (DRM DPMS through the fd Slint already owns), optional sysfs backlight,
//! NTP synchronization check and brightness persistence.

use std::cell::Cell;
use std::fs;
use std::os::fd::{AsFd, BorrowedFd, RawFd};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use drm::control::{connector, property, Device as ControlDevice};
use tracing::{debug, info, warn};

use crate::types::LightState;

/// A DRM device seen through a file descriptor we don't own (Slint keeps it open for the
/// whole life of the process).
struct Card(RawFd);

impl AsFd for Card {
    fn as_fd(&self) -> BorrowedFd<'_> {
        // Safety: the fd belongs to Slint's linuxkms backend, which never closes it before exit.
        unsafe { BorrowedFd::borrow_raw(self.0) }
    }
}

impl drm::Device for Card {}
impl ControlDevice for Card {}

/// The connected connector's legacy `DPMS` property.
pub struct Dpms {
    card: Card,
    connector: connector::Handle,
    prop: property::Handle,
    pub connector_name: String,
    on: Cell<bool>,
}

impl Dpms {
    /// Must run after the Slint window exists (the backend opens `/dev/dri/cardN` then).
    pub fn discover() -> Result<Self> {
        let mut cards = Vec::new();
        for entry in fs::read_dir("/proc/self/fd").context("listing /proc/self/fd")? {
            let entry = entry?;
            let Some(fd) = entry.file_name().to_str().and_then(|s| s.parse::<RawFd>().ok()) else {
                continue;
            };
            if let Ok(target) = fs::read_link(entry.path()) {
                if target.to_string_lossy().starts_with("/dev/dri/card") {
                    cards.push((fd, target));
                }
            }
        }
        if cards.is_empty() {
            bail!("no /dev/dri/card* descriptor open in this process (linuxkms backend not active?)");
        }
        for (fd, path) in cards {
            let card = Card(fd);
            let resources = match card.resource_handles() {
                Ok(r) => r,
                Err(e) => {
                    debug!("fd {fd} ({}): not a KMS device: {e}", path.display());
                    continue;
                }
            };
            for &ch in resources.connectors() {
                let Ok(info) = card.get_connector(ch, false) else { continue };
                if info.state() != connector::State::Connected {
                    continue;
                }
                let Ok(props) = card.get_properties(ch) else { continue };
                let (handles, values) = props.as_props_and_values();
                for (&ph, &value) in handles.iter().zip(values.iter()) {
                    let Ok(pinfo) = card.get_property(ph) else { continue };
                    if pinfo.name().to_str().ok() != Some("DPMS") {
                        continue;
                    }
                    let connector_name =
                        format!("{}-{}", info.interface().as_str(), info.interface_id());
                    info!(
                        "DPMS control ready on {connector_name} via {} (fd {fd}), currently {}",
                        path.display(),
                        if value == 0 { "on" } else { "off" }
                    );
                    return Ok(Self { card, connector: ch, prop: ph, connector_name, on: Cell::new(value == 0) });
                }
            }
        }
        bail!("no connected DRM connector exposes a DPMS property")
    }

    /// `On` (0) or `Off` (3). Needs DRM master, which Slint's fd has.
    pub fn set_on(&self, on: bool) -> Result<()> {
        if self.on.get() == on {
            return Ok(());
        }
        let value: property::RawValue = if on { 0 } else { 3 };
        self.card
            .set_property(self.connector, self.prop, value)
            .with_context(|| format!("setting DPMS {} on {}", if on { "On" } else { "Off" }, self.connector_name))?;
        self.on.set(on);
        info!("panel {} (DPMS {})", if on { "on" } else { "off" }, self.connector_name);
        Ok(())
    }

    pub fn is_on(&self) -> bool {
        self.on.get()
    }
}

/// `/sys/class/backlight/<first>` when the panel exposes one (the ZeroMOD HDMI bar doesn't).
pub struct Backlight {
    dir: PathBuf,
    max: u32,
    has_bl_power: bool,
}

impl Backlight {
    pub fn discover() -> Option<Self> {
        let mut dirs: Vec<PathBuf> =
            fs::read_dir("/sys/class/backlight").ok()?.filter_map(|e| e.ok().map(|e| e.path())).collect();
        dirs.sort();
        let dir = dirs.into_iter().next()?;
        let max: u32 = fs::read_to_string(dir.join("max_brightness")).ok()?.trim().parse().ok()?;
        let has_bl_power = dir.join("bl_power").exists();
        info!("backlight {} (max {max})", dir.display());
        Some(Self { dir, max: max.max(1), has_bl_power })
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// 0 turns the backlight off, 1..=255 is scaled to the hardware range (never below 1).
    pub fn set(&self, level: u8) -> Result<()> {
        if level == 0 {
            if self.has_bl_power {
                fs::write(self.dir.join("bl_power"), "4")?;
            }
            fs::write(self.dir.join("brightness"), "0")?;
            return Ok(());
        }
        if self.has_bl_power {
            fs::write(self.dir.join("bl_power"), "0")?;
        }
        let hw = ((f64::from(level) / 255.0) * f64::from(self.max)).round().max(1.0) as u32;
        fs::write(self.dir.join("brightness"), hw.to_string())
            .with_context(|| format!("writing {}/brightness", self.dir.display()))
    }
}

/// True once the kernel clock is NTP-disciplined (systemd-timesyncd/chrony clear `STA_UNSYNC`).
pub fn time_is_synced() -> bool {
    // Safety: adjtimex with modes = 0 only reads the kernel state, timex is plain data.
    let mut tx: libc::timex = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::adjtimex(&mut tx) };
    if rc >= 0 {
        return rc != libc::TIME_ERROR && (tx.status & libc::STA_UNSYNC) == 0;
    }
    Path::new("/run/systemd/timesync/synchronized").exists()
}

/// File format: `on 255` / `off 120`.
pub fn load_light_state(path: &Path) -> Option<LightState> {
    let text = fs::read_to_string(path).ok()?;
    let mut it = text.split_whitespace();
    let on = match it.next()? {
        "on" => true,
        "off" => false,
        _ => return None,
    };
    let brightness: u8 = it.next()?.parse().ok()?;
    if brightness == 0 {
        return None;
    }
    Some(LightState { on, brightness })
}

pub fn save_light_state(path: &Path, state: LightState) {
    let text = format!("{} {}\n", if state.on { "on" } else { "off" }, state.brightness);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let tmp = path.with_extension("tmp");
    if let Err(e) = fs::write(&tmp, text).and_then(|_| fs::rename(&tmp, path)) {
        warn!("cannot save light state to {}: {e}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn light_state_round_trip() {
        let dir = std::env::temp_dir().join(format!("barclock-test-{}", std::process::id()));
        let path = dir.join("brightness");
        let s = LightState { on: false, brightness: 42 };
        save_light_state(&path, s);
        assert_eq!(load_light_state(&path), Some(s));
        fs::write(&path, "on 0").unwrap();
        assert_eq!(load_light_state(&path), None);
        fs::write(&path, "garbage").unwrap();
        assert_eq!(load_light_state(&path), None);
        let _ = fs::remove_dir_all(dir);
    }
}
