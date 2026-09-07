//! `barclock --preview out.png [--preview-state normal|connecting|dim|busy|paused|error|idle|toast|touchtest]`
//! Renders the UI headlessly with the software renderer so the layout can be checked on any
//! machine (no display, no DRM). Uses sample data.

use std::path::Path;
use std::rc::Rc;

use anyhow::{anyhow, Context, Result};
use chrono::TimeZone;
use slint::platform::software_renderer::{MinimalSoftwareWindow, RepaintBufferType};
use slint::Rgb8Pixel;
use slint::{ComponentHandle, ModelRc, VecModel};

use crate::config::Config;
use crate::{AppWindow, LoadState, ToggleState, ZoneTime};

const W: u32 = 1920;
const H: u32 = 480;

struct HeadlessPlatform {
    window: Rc<MinimalSoftwareWindow>,
}

impl slint::platform::Platform for HeadlessPlatform {
    fn create_window_adapter(
        &self,
    ) -> std::result::Result<Rc<dyn slint::platform::WindowAdapter>, slint::PlatformError> {
        Ok(self.window.clone())
    }
}

pub fn render(cfg: &Config, out: &Path, state: &str) -> Result<()> {
    let window = MinimalSoftwareWindow::new(RepaintBufferType::NewBuffer);
    slint::platform::set_platform(Box::new(HeadlessPlatform { window: window.clone() }))
        .map_err(|e| anyhow!("set_platform: {e:?}"))?;
    let app = AppWindow::new().context("creating the window")?;
    window.set_size(slint::PhysicalSize::new(W, H));
    app.show().context("show")?;
    populate(&app, cfg, state);

    slint::platform::update_timers_and_animations();
    let mut pixels = vec![Rgb8Pixel { r: 0, g: 0, b: 0 }; (W * H) as usize];
    let drawn = window.draw_if_needed(|renderer| {
        renderer.render(&mut pixels, W as usize);
    });
    anyhow::ensure!(drawn, "nothing was drawn");

    let file = std::fs::File::create(out).with_context(|| format!("creating {}", out.display()))?;
    let mut encoder = png::Encoder::new(std::io::BufWriter::new(file), W, H);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header()?;
    let bytes: Vec<u8> = pixels.iter().flat_map(|p| [p.r, p.g, p.b]).collect();
    writer.write_image_data(&bytes)?;
    writer.finish()?;
    println!("wrote {} ({state})", out.display());
    Ok(())
}

fn on(on: bool) -> ToggleState {
    ToggleState { on, pending: false, available: true }
}

fn populate(app: &AppWindow, cfg: &Config, state: &str) {
    app.set_version_text(format!("v{}", env!("CARGO_PKG_VERSION")).into());
    app.set_time_hm("15:04:".into());
    app.set_time_s("09".into());
    app.set_date_text("05/09/2026".into());
    app.set_weekday_text("SAMEDI".into());
    // Derive the zone times from the same instant as the big clock, so adding a zone to the
    // config shows up here instead of being silently dropped by a fixed-length list.
    let sample = chrono::Utc.with_ymd_and_hms(2026, 9, 5, 19, 4, 9).unwrap();
    let tzs = cfg.zone_tzs().unwrap_or_default();
    let local_tz = cfg.local_tz().unwrap_or(chrono_tz::UTC);
    let zones: Vec<ZoneTime> = crate::clock::render(sample, local_tz, &tzs, &cfg.general.language)
        .zones
        .into_iter()
        .map(|(label, time)| ZoneTime { label: label.to_uppercase().into(), time: time.into() })
        .collect();
    app.set_zones(ModelRc::new(VecModel::from(zones)));
    app.set_weather_line("12.3 °C   55 %   8 km/h".into());
    app.set_weather_condition(cfg.weather_label("partlycloudy").into());
    app.set_weather_icon("partlycloudy".into());
    app.set_weather_place("SEPT-ÎLES".into());
    app.set_house_power_text("1 234 W".into());
    app.set_servers_power_text("954 W".into());
    let load = |on: bool, power: &str| LoadState {
        on,
        available: true,
        configured: true,
        power: power.into(),
        badge: "".into(),
    };
    app.set_load_stove(load(false, "0 W"));
    app.set_load_car(LoadState { badge: "81.1 %".into(), ..load(false, "0 W") });
    app.set_load_water_heater(load(false, "0 W"));
    app.set_load_washer(load(false, "1 W"));
    app.set_load_dryer(load(false, "0 W"));
    app.set_indoor_line("21.9 °C   56 %".into());
    app.set_fan_available(true);
    app.set_fan_on(true);
    app.set_fan_percentage(45);
    app.set_preset_active(2);
    app.set_printer_status(cfg.printer_label(crate::config::PrinterStatus::Off).into());
    app.set_printer_mode("off".into());
    app.set_light_ceiling(on(true));
    app.set_light_monitor(on(false));
    app.set_kiosk_grafana(on(true));
    app.set_kiosk_cam_street(on(false));
    app.set_kiosk_cam_gallery(on(false));
    app.set_kiosk_display(on(true));
    app.set_kiosk_display_mode("ON".into());
    app.set_lights_summary("PLAFONNIER 100 % · ÉCRAN OFF".into());
    app.set_ha_online(true);
    app.set_connecting(false);
    app.set_dim_opacity(0.0);

    match state {
        "connecting" => {
            app.set_connecting(true);
            app.set_connecting_detail("HOME ASSISTANT · HORLOGE NTP".into());
            app.set_ha_online(false);
        }
        "dim" => app.set_dim_opacity(0.6),
        "busy" => {
            app.set_load_stove(load(true, "37 W"));
            app.set_load_car(LoadState { badge: "62.4 %".into(), ..load(true, "3 300 W") });
            app.set_load_water_heater(load(true, "2 953 W"));
            app.set_load_dryer(load(true, "2 480 W"));
            app.set_fan_percentage(100);
            app.set_servers_power_text("1 234 W".into());
            app.set_printer_active(true);
            app.set_printer_status(cfg.printer_label(crate::config::PrinterStatus::Printing).into());
            app.set_printer_mode("printing".into());
            app.set_printer_progress_text("100 %".into());
            app.set_printer_time_left_text("1234 min".into());
            app.set_light_monitor(ToggleState { on: false, pending: true, available: true });
            app.set_preset_pending(2);
            app.set_kiosk_display(ToggleState { on: true, pending: true, available: true });
            app.set_kiosk_cam_gallery(ToggleState { on: false, pending: false, available: false });
        }
        "paused" | "error" | "idle" => {
            use crate::config::PrinterStatus as P;
            let st = match state { "paused" => P::Paused, "error" => P::Error, _ => P::Idle };
            app.set_printer_status(cfg.printer_label(st).into());
            app.set_printer_mode(st.key().into());
            app.set_printer_active(st == P::Paused);
            app.set_printer_progress_text("42 %".into());
            app.set_printer_time_left_text("118 min".into());
        }
        "toast" => {
            app.set_toast_text("Échec : light.turn_on, appareil injoignable".into());
            app.set_toast_visible(true);
        }
        "touchtest" => app.set_touch_test(true),
        _ => {}
    }
}
