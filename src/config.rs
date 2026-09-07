//! `/etc/barclock.toml` schema with defaults. Secrets never live here (environment only).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{Context, Result};
use chrono_tz::Tz;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrinterStatus {
    /// Power switch off, or Klipper shut down with the printer not known to be powered.
    Off,
    /// Every printer entity is unavailable (Moonraker / integration down) but the power isn't off.
    Unknown,
    Idle,
    Printing,
    Paused,
    Error,
}

impl PrinterStatus {
    /// Key used by the UI for colours: "off" | "idle" | "printing" | "paused" | "error".
    pub fn key(self) -> &'static str {
        match self {
            PrinterStatus::Off | PrinterStatus::Unknown => "off",
            PrinterStatus::Idle => "idle",
            PrinterStatus::Printing => "printing",
            PrinterStatus::Paused => "paused",
            PrinterStatus::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub general: General,
    pub clock: Clock,
    pub homeassistant: HomeAssistant,
    pub mqtt: Mqtt,
    pub display: Display,
    pub entities: Entities,
    pub thresholds: Thresholds,
    /// Optional overrides for the weather condition text, keyed by the HA condition
    /// (`sunny`, `partlycloudy`, ...).
    pub weather_labels: HashMap<String, String>,    /// Direct polling of the IoTaWatt (house total + servers). Empty url = use the HA sensors.
    pub iotawatt: IotaWatt,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct General {
    /// IANA zone for the big clock and the date.
    pub timezone: String,
    /// `"fr"` or `"en"`: weekday names and weather condition labels.
    pub language: String,
    /// Caption of the indoor temperature/humidity box under the weather (uppercased on screen).
    pub indoor_label: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Clock {
    pub zones: Vec<Zone>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Zone {
    pub label: String,
    pub tz: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HomeAssistant {
    /// `wss://host/api/websocket` (or `ws://host:8123/api/websocket`).
    pub url: String,
    /// Seconds without a connection before the full-screen "Connecting..." state comes back.
    pub connecting_after_s: u64,
    /// Seconds to wait for the result of a `call_service` before showing an error toast.
    pub call_timeout_s: u64,
    pub ping_interval_s: u64,
    pub backoff_min_s: u64,
    pub backoff_max_s: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mqtt {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub keepalive_s: u64,
    pub discovery_prefix: String,
    /// Topic prefix for state/set/availability.
    pub base_topic: String,
    pub unique_id: String,
    pub device_name: String,
    pub suggested_area: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Display {
    /// Used only when `SLINT_KMS_ROTATION` isn't set in the environment (0, 90, 180, 270).
    pub rotation: u32,
    /// Brightness used when nothing was ever saved/retained (1..=255).
    pub default_brightness: u8,
    /// How long a touch keeps the screen readable while the light is off/dimmed.
    pub wake_seconds: u64,
    /// Brightness during that wake window (1..=255).
    pub wake_brightness: u8,
    /// Below this brightness a touch wakes the screen instead of operating a control.
    pub readable_threshold: u8,
    /// Turn the HDMI output off (DRM DPMS) when the light is off.
    pub dpms_off: bool,
    /// Delay between showing the black frame and cutting the signal.
    pub dpms_delay_ms: u64,
    /// Never dim more than this with the software overlay, so brightness 1 stays readable up close.
    pub max_dim_opacity: f32,
    /// Where the last light state is remembered between restarts.
    pub state_file: PathBuf,
    /// Seconds an error toast stays visible.
    pub toast_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Entities {
    pub weather: String,
    pub house_power: String,
    pub stove_power: String,
    pub printer_progress: String,
    pub printer_time_left: String,
    /// Optional: the printer's power switch (off -> status OFF). Empty = not used.
    pub printer_power: String,
    /// Optional: Moonraker `current_print_state` (standby/printing/paused/complete/cancelled/error).
    pub printer_print_state: String,
    /// Optional: Moonraker `printer_state` (ready/startup/shutdown/error).
    pub printer_klipper_state: String,
    pub light_ceiling: String,
    pub light_monitor: String,
    pub kiosk_grafana: String,
    /// One `select.*` entity choosing which camera the kiosk shows. The two camera buttons drive
    /// it, so they are mutually exclusive by construction (it replaced a switch each).
    pub kiosk_camera: String,
    /// Options of `kiosk_camera` for each button, and the one meaning "no camera".
    pub kiosk_camera_street_option: String,
    pub kiosk_camera_gallery_option: String,
    pub kiosk_camera_off_option: String,
    pub kiosk_display_power: String,
    /// Option sent to `kiosk_display_power` when the MONITEUR toggle is switched off.
    pub kiosk_display_off_option: String,
    /// Power strip under the house total (each optional, "" hides the icon). Power sensors in W,
    /// except the car charger which Home Assistant reports in kW.
    pub car_charge_power: String,
    /// Car battery level, shown next to the AUTO column's name. Optional.
    pub car_battery: String,
    pub water_heater_power: String,
    pub washer_power: String,
    pub dryer_power: String,
    /// Always-shown wattage labelled SERVEURS (the technical room). Optional.
    pub servers_power: String,
    /// `fan.*` entity driven by the VENTILATEUR slider with `fan.set_percentage`. Optional.
    pub office_fan: String,
    /// Indoor temperature / humidity shown under the weather. Optional.
    pub office_temperature: String,
    pub office_humidity: String,
}

/// Direct polling of an IoTaWatt energy monitor on the LAN, bypassing Home Assistant's 30 s
/// refresh for the two values that are worth having live. See `src/iotawatt.rs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IotaWatt {
    /// `http://host[:port]/`. Empty disables polling and both values come from Home Assistant.
    pub url: String,
    /// Name of the device *output* holding the house total (`HydroQuebec` = `@13+@14`).
    pub house_output: String,
    /// Name of the device *input* holding the servers' draw. Resolved to a channel at runtime.
    pub servers_input: String,
    /// How often to poll. 1 s is what the device's own web page uses.
    pub poll_interval_ms: u64,
    /// Whole-request timeout (connect, send, read).
    pub timeout_ms: u64,
    /// Consecutive failed polls before falling back to the Home Assistant sensors.
    pub failures_before_fallback: u32,
}

impl Default for IotaWatt {
    fn default() -> Self {
        Self {
            url: String::new(),
            house_output: "HydroQuebec".into(),
            servers_input: "Serveurs".into(),
            poll_interval_ms: 1000,
            timeout_ms: 2000,
            failures_before_fallback: 3,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Thresholds {
    /// Flame icon when the stove draws more than this many watts.
    pub stove_flame_w: f64,
    /// Car icon lit above this charge power (the sensor is in kW).
    pub car_charging_kw: f64,
    /// Water-heater icon lit above this many watts (the element draws ~3 kW when heating).
    pub water_heater_w: f64,
    /// Washer icon lit above this many watts (standby is under 1 W).
    pub washer_w: f64,
    /// Dryer icon lit above this many watts (standby is under 1 W).
    pub dryer_w: f64,
}

impl Default for General {
    fn default() -> Self {
        Self { timezone: "America/Toronto".into(), language: "fr".into(), indoor_label: "Bureau Lulu".into() }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self {
            zones: vec![
                Zone { label: "UTC".into(), tz: "UTC".into() },
                Zone { label: "London".into(), tz: "Europe/London".into() },
                Zone { label: "Paris/Skopje".into(), tz: "Europe/Paris".into() },
            ],
        }
    }
}

impl Default for HomeAssistant {
    fn default() -> Self {
        Self {
            url: "wss://homeassistant.local/api/websocket".into(),
            connecting_after_s: 10,
            call_timeout_s: 8,
            ping_interval_s: 30,
            backoff_min_s: 1,
            backoff_max_s: 30,
        }
    }
}

impl Default for Mqtt {
    fn default() -> Self {
        Self {
            host: "homeassistant.local".into(),
            port: 1883,
            client_id: "barclock".into(),
            keepalive_s: 30,
            discovery_prefix: "homeassistant".into(),
            base_topic: "barclock/display".into(),
            unique_id: "barclock_display".into(),
            device_name: "Bar display".into(),
            suggested_area: "Bureau Lulu".into(),
        }
    }
}

impl Default for Display {
    fn default() -> Self {
        Self {
            rotation: 90,
            default_brightness: 255,
            wake_seconds: 30,
            wake_brightness: 160,
            readable_threshold: 100,
            dpms_off: true,
            dpms_delay_ms: 400,
            max_dim_opacity: 0.94,
            state_file: PathBuf::from("/var/lib/barclock/brightness"),
            toast_seconds: 5,
        }
    }
}

impl Default for Entities {
    fn default() -> Self {
        Self {
            weather: "weather.sept_iles".into(),
            house_power: "sensor.hydroquebec_w".into(),
            stove_power: "sensor.poele_w".into(),
            printer_progress: "sensor.bureau_lulu_klipper_lulu_progress".into(),
            printer_time_left: "sensor.bureau_lulu_klipper_lulu_print_time_left".into(),
            printer_power: "switch.bureau_lulu_klipper_lulu_ender3".into(),
            printer_print_state: "sensor.bureau_lulu_klipper_lulu_current_print_state".into(),
            printer_klipper_state: "sensor.bureau_lulu_klipper_lulu_printer_state".into(),
            light_ceiling: "light.plafonnier_bureau_lulu".into(),
            light_monitor: "light.lumieres_ecran_lumiere_bureau".into(),
            kiosk_grafana: "switch.bureau_lulu_kiosk_grafana_kiosk".into(),
            kiosk_camera: "select.bureau_lulu_kiosk_camera".into(),
            kiosk_camera_street_option: "Rue".into(),
            kiosk_camera_gallery_option: "Galerie".into(),
            kiosk_camera_off_option: "off".into(),
            kiosk_display_power: "select.bureau_lulu_kiosk_display_power_mode".into(),
            kiosk_display_off_option: "suspend".into(),
            car_charge_power: "sensor.chevrolet_volt_charge_power".into(),
            car_battery: "sensor.chevrolet_volt_ev_battery_level".into(),
            water_heater_power: "sensor.chauffe_eau_w".into(),
            washer_power: "sensor.laveuse_power".into(),
            dryer_power: "sensor.secheuse_w".into(),
            servers_power: "sensor.serveurs_w".into(),
            office_fan: "fan.office_fan_office_fan".into(),
            office_temperature: "sensor.thermometre_chambre_gabie_temperature".into(),
            office_humidity: "sensor.thermometre_chambre_gabie_humidity".into(),
        }
    }
}

impl Default for Thresholds {
    fn default() -> Self {
        Self { stove_flame_w: 4.0, car_charging_kw: 0.1, water_heater_w: 50.0, washer_w: 10.0, dryer_w: 10.0 }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            general: General::default(),
            clock: Clock::default(),
            homeassistant: HomeAssistant::default(),
            mqtt: Mqtt::default(),
            display: Display::default(),
            entities: Entities::default(),
            thresholds: Thresholds::default(),
            weather_labels: HashMap::new(),
            iotawatt: IotaWatt::default(),
        }
    }
}

impl Config {
    /// Reads the TOML file. A missing file yields the defaults (with a warning).
    pub fn load(path: &Path) -> Result<Self> {
        let cfg = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str::<Config>(&text)
                .with_context(|| format!("parsing {}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("{} not found, using built-in defaults", path.display());
                Config::default()
            }
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        self.local_tz()?;
        self.zone_tzs()?;
        anyhow::ensure!(self.display.default_brightness > 0, "display.default_brightness must be 1..=255");
        anyhow::ensure!(self.display.wake_brightness > 0, "display.wake_brightness must be 1..=255");
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.display.max_dim_opacity),
            "display.max_dim_opacity must be within 0..=1"
        );
        if !self.entities.kiosk_camera.trim().is_empty() {
            let (street, gallery, off) = (
                self.entities.kiosk_camera_street_option.trim(),
                self.entities.kiosk_camera_gallery_option.trim(),
                self.entities.kiosk_camera_off_option.trim(),
            );
            anyhow::ensure!(
                !street.is_empty() && !gallery.is_empty() && !off.is_empty(),
                "entities.kiosk_camera_{{street,gallery,off}}_option must all name an option of {:?}",
                self.entities.kiosk_camera
            );
            anyhow::ensure!(
                street != gallery && street != off && gallery != off,
                "entities.kiosk_camera_{{street,gallery,off}}_option must differ from each other, got {street:?}, {gallery:?}, {off:?}"
            );
        }
        anyhow::ensure!(
            matches!(self.display.rotation, 0 | 90 | 180 | 270),
            "display.rotation must be 0, 90, 180 or 270"
        );
        if !self.iotawatt.url.trim().is_empty() {
            anyhow::ensure!(
                (200..=300_000).contains(&self.iotawatt.poll_interval_ms),
                "iotawatt.poll_interval_ms must be between 200 and 300000, got {}",
                self.iotawatt.poll_interval_ms
            );
            anyhow::ensure!(
                (200..=30_000).contains(&self.iotawatt.timeout_ms),
                "iotawatt.timeout_ms must be between 200 and 30000, got {}",
                self.iotawatt.timeout_ms
            );
            anyhow::ensure!(
                (1..=100).contains(&self.iotawatt.failures_before_fallback),
                "iotawatt.failures_before_fallback must be between 1 and 100, got {}",
                self.iotawatt.failures_before_fallback
            );
            anyhow::ensure!(
                !self.iotawatt.house_output.trim().is_empty()
                    || !self.iotawatt.servers_input.trim().is_empty(),
                "iotawatt.url is set but neither house_output nor servers_input names anything to read"
            );
        }
        anyhow::ensure!(
            !self.entities.kiosk_display_off_option.trim().is_empty() && self.entities.kiosk_display_off_option != "on",
            "entities.kiosk_display_off_option must be a select option other than \"on\" (standby, suspend or off), got {:?}",
            self.entities.kiosk_display_off_option
        );
        // Every one of these becomes `Instant + Duration` in ha_ws / main: an absurd value would
        // overflow at runtime (abort under `panic = "abort"`) instead of failing here with a name.
        let ha = &self.homeassistant;
        anyhow::ensure!(
            (5..=3600).contains(&ha.ping_interval_s),
            "homeassistant.ping_interval_s must be within 5..=3600 (got {})",
            ha.ping_interval_s
        );
        anyhow::ensure!(
            (1..=3600).contains(&ha.backoff_min_s),
            "homeassistant.backoff_min_s must be within 1..=3600 (got {})",
            ha.backoff_min_s
        );
        anyhow::ensure!(
            (ha.backoff_min_s..=3600).contains(&ha.backoff_max_s),
            "homeassistant.backoff_max_s must be within backoff_min_s..=3600 (got {}, backoff_min_s is {})",
            ha.backoff_max_s,
            ha.backoff_min_s
        );
        anyhow::ensure!(
            (1..=600).contains(&ha.call_timeout_s),
            "homeassistant.call_timeout_s must be within 1..=600 (got {})",
            ha.call_timeout_s
        );
        anyhow::ensure!(
            ha.connecting_after_s <= 600,
            "homeassistant.connecting_after_s must be within 0..=600 (got {})",
            ha.connecting_after_s
        );
        Ok(())
    }

    pub fn local_tz(&self) -> Result<Tz> {
        Tz::from_str(&self.general.timezone)
            .map_err(|e| anyhow::anyhow!("general.timezone {:?}: {e}", self.general.timezone))
    }

    pub fn zone_tzs(&self) -> Result<Vec<(String, Tz)>> {
        self.clock
            .zones
            .iter()
            .map(|z| {
                Tz::from_str(&z.tz)
                    .map(|tz| (z.label.clone(), tz))
                    .map_err(|e| anyhow::anyhow!("clock.zones {:?}: {e}", z.tz))
            })
            .collect()
    }

    /// Every entity we subscribe to, in a stable order.
    pub fn entity_ids(&self) -> Vec<String> {
        let e = &self.entities;
        [
            &e.weather,
            &e.house_power,
            &e.stove_power,
            &e.printer_progress,
            &e.printer_time_left,
            &e.printer_power,
            &e.printer_print_state,
            &e.printer_klipper_state,
            &e.light_ceiling,
            &e.light_monitor,
            &e.kiosk_grafana,
            &e.kiosk_camera,
            &e.kiosk_display_power,
            &e.car_charge_power,
            &e.car_battery,
            &e.water_heater_power,
            &e.washer_power,
            &e.dryer_power,
            &e.servers_power,
            &e.office_fan,
            &e.office_temperature,
            &e.office_humidity,
        ]
        .into_iter()
        .filter(|id| !id.trim().is_empty())
        .cloned()
        .collect()
    }

    /// Label for the 3D printer module.
    pub fn printer_label(&self, status: PrinterStatus) -> &'static str {
        let fr = self.general.language == "fr";
        match status {
            PrinterStatus::Off => "OFF",
            PrinterStatus::Unknown => "—",
            PrinterStatus::Idle => if fr { "INACTIVE" } else { "IDLE" },
            PrinterStatus::Printing => if fr { "IMPRESSION" } else { "PRINTING" },
            PrinterStatus::Paused => if fr { "EN PAUSE" } else { "PAUSED" },
            PrinterStatus::Error => if fr { "ERREUR" } else { "ERROR" },
        }
    }

    /// Human text for a Home Assistant weather condition.
    pub fn weather_label(&self, condition: &str) -> String {
        if let Some(s) = self.weather_labels.get(condition) {
            return s.clone();
        }
        let fr = self.general.language == "fr";
        let s = match condition {
            "sunny" => if fr { "Ensoleillé" } else { "Sunny" },
            "clear-night" => if fr { "Ciel dégagé" } else { "Clear night" },
            "partlycloudy" => if fr { "Partiellement nuageux" } else { "Partly cloudy" },
            "cloudy" => if fr { "Nuageux" } else { "Cloudy" },
            "rainy" => if fr { "Pluie" } else { "Rain" },
            "pouring" => if fr { "Forte pluie" } else { "Pouring" },
            "snowy" => if fr { "Neige" } else { "Snow" },
            "snowy-rainy" => if fr { "Neige et pluie" } else { "Snow and rain" },
            "fog" => if fr { "Brouillard" } else { "Fog" },
            "hail" => if fr { "Grêle" } else { "Hail" },
            "lightning" => if fr { "Orage" } else { "Lightning" },
            "lightning-rainy" => if fr { "Orage et pluie" } else { "Thunderstorm" },
            "windy" => if fr { "Venteux" } else { "Windy" },
            "windy-variant" => if fr { "Venteux et nuageux" } else { "Windy and cloudy" },
            "exceptional" => if fr { "Exceptionnel" } else { "Exceptional" },
            "unavailable" => if fr { "Indisponible" } else { "Unavailable" },
            "unknown" | "" => if fr { "Inconnu" } else { "Unknown" },
            other => return other.replace('-', " "),
        };
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let cfg = Config::default();
        cfg.validate().unwrap();
        assert_eq!(cfg.entity_ids().len(), 22);
        assert_eq!(cfg.weather_label("sunny"), "Ensoleillé");
    }

    #[test]
    fn parses_partial_file_and_overrides() {
        let cfg: Config = toml::from_str(
            r#"
            [general]
            language = "en"
            [mqtt]
            host = "broker.local"
            [weather_labels]
            sunny = "Soleil"
            "#,
        )
        .unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.mqtt.host, "broker.local");
        assert_eq!(cfg.mqtt.port, 1883);
        assert_eq!(cfg.weather_label("sunny"), "Soleil");
        assert_eq!(cfg.weather_label("cloudy"), "Cloudy");
    }

    #[test]
    fn rejects_overlapping_kiosk_camera_options() {
        let cfg: Config = toml::from_str(
            "[entities]\nkiosk_camera_street_option = \"off\"\n",
        )
        .unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("must differ"), "{err}");
        let cfg: Config = toml::from_str("[entities]\nkiosk_camera_gallery_option = \"\"\n").unwrap();
        assert!(cfg.validate().unwrap_err().to_string().contains("must all name"));
        // not checked when no select is configured
        let cfg: Config =
            toml::from_str("[entities]\nkiosk_camera = \"\"\nkiosk_camera_street_option = \"off\"\n").unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_bad_iotawatt_settings() {
        // not checked at all while polling is off
        let cfg: Config = toml::from_str("[iotawatt]\npoll_interval_ms = 1\n").unwrap();
        cfg.validate().unwrap();
        for (key, val) in [("poll_interval_ms", "1"), ("timeout_ms", "0"), ("failures_before_fallback", "0")] {
            let cfg: Config =
                toml::from_str(&format!("[iotawatt]\nurl = \"http://h/\"\n{key} = {val}\n")).unwrap();
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(key), "{key}: {err}");
        }
        let cfg: Config =
            toml::from_str("[iotawatt]\nurl = \"http://h/\"\nhouse_output = \"\"\nservers_input = \"\"\n").unwrap();
        assert!(cfg.validate().unwrap_err().to_string().contains("neither"));
        let cfg: Config = toml::from_str("[iotawatt]\nurl = \"http://iotawatt.local/\"\n").unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_bad_kiosk_display_off_option() {
        for bad in ["\"on\"", "\"\"", "\"  \""] {
            let cfg: Config = toml::from_str(&format!("[entities]\nkiosk_display_off_option = {bad}\n")).unwrap();
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains("kiosk_display_off_option"), "{err}");
        }
        let cfg: Config = toml::from_str("[entities]\nkiosk_display_off_option = \"off\"\n").unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_keys() {
        assert!(toml::from_str::<Config>("[general]\nbogus = 1\n").is_err());
    }

    /// Parses `[homeassistant]` with `line` and returns the validation error text.
    fn ha_validation_error(line: &str) -> String {
        let cfg: Config = toml::from_str(&format!("[homeassistant]\n{line}\n")).unwrap();
        cfg.validate().expect_err(line).to_string()
    }

    #[test]
    fn rejects_out_of_range_homeassistant_timings() {
        // A huge ping interval used to overflow `Instant + Duration` at runtime.
        let e = ha_validation_error("ping_interval_s = 9223372036854775807");
        assert!(e.contains("homeassistant.ping_interval_s"), "{e}");
        assert!(e.contains("5..=3600"), "{e}");
        assert!(ha_validation_error("ping_interval_s = 4").contains("ping_interval_s"));
        assert!(ha_validation_error("backoff_min_s = 0").contains("homeassistant.backoff_min_s"));
        assert!(ha_validation_error("backoff_min_s = 3601").contains("homeassistant.backoff_min_s"));
        let e = ha_validation_error("backoff_min_s = 10\nbackoff_max_s = 5");
        assert!(e.contains("homeassistant.backoff_max_s"), "{e}");
        assert!(e.contains("backoff_min_s is 10"), "{e}");
        assert!(ha_validation_error("backoff_max_s = 3601").contains("homeassistant.backoff_max_s"));
        assert!(ha_validation_error("call_timeout_s = 0").contains("homeassistant.call_timeout_s"));
        assert!(ha_validation_error("call_timeout_s = 601").contains("homeassistant.call_timeout_s"));
        assert!(ha_validation_error("connecting_after_s = 601").contains("homeassistant.connecting_after_s"));
    }

    #[test]
    fn accepts_homeassistant_timing_bounds() {
        let cfg: Config = toml::from_str(
            "[homeassistant]\nping_interval_s = 3600\nbackoff_min_s = 30\nbackoff_max_s = 30\n\
             call_timeout_s = 600\nconnecting_after_s = 0\n",
        )
        .unwrap();
        cfg.validate().unwrap();
        let cfg: Config = toml::from_str("[homeassistant]\nping_interval_s = 5\nbackoff_min_s = 1\nbackoff_max_s = 1\n").unwrap();
        cfg.validate().unwrap();
    }
}
