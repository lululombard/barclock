//! barclock, a 1920x480 touch bar dashboard for Home Assistant (Slint on DRM/KMS).
//!
//! Thread model: the Slint event loop owns the main thread (DRM output, libinput touch), and
//! a tokio current_thread runtime on a second thread owns the Home Assistant WebSocket and
//! MQTT.

mod clock;
mod config;
mod display;
mod ha_ws;
mod iotawatt;
mod mqtt_light;
mod preview;
mod types;

slint::include_modules!();

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use chrono_tz::Tz;
use serde_json::{json, Value};
use slint::{ComponentHandle, Model, ModelRc, Timer, TimerMode, VecModel};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use config::{Config, PrinterStatus};
use iotawatt::PowerEvent;
use display::{Backlight, Dpms};
use types::{EntityState, HaEvent, LightCommand, LightState, ServiceCall};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_CONFIG: &str = "/etc/barclock.toml";
const USAGE: &str = "\
barclock [--config /etc/barclock.toml] [--touch-test]
barclock --preview out.png [--preview-state normal|connecting|dim|busy|paused|error|idle|toast|touchtest]
barclock --version | --help

Environment: HA_TOKEN, MQTT_USER, MQTT_PASSWORD, BARCLOCK_CONFIG, RUST_LOG,
             SLINT_BACKEND (default linuxkms-software), SLINT_KMS_ROTATION (default from config)";

struct Cli {
    config: PathBuf,
    touch_test: bool,
    preview: Option<(PathBuf, String)>,
}

fn parse_cli() -> Result<Cli> {
    let mut cli = Cli {
        config: std::env::var_os("BARCLOCK_CONFIG").map(PathBuf::from).unwrap_or_else(|| DEFAULT_CONFIG.into()),
        touch_test: false,
        preview: None,
    };
    let mut preview_state = "normal".to_string();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" | "-c" => cli.config = args.next().context("--config needs a path")?.into(),
            "--touch-test" => cli.touch_test = true,
            "--preview" => {
                let p = args.next().context("--preview needs an output .png path")?;
                cli.preview = Some((p.into(), String::new()));
            }
            "--preview-state" => preview_state = args.next().context("--preview-state needs a value")?,
            "--version" | "-V" => {
                println!("barclock {VERSION}");
                std::process::exit(0);
            }
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument {other:?}\n{USAGE}"),
        }
    }
    if let Some((_, s)) = cli.preview.as_mut() {
        *s = preview_state;
    }
    Ok(cli)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .without_time()
        .with_ansi(false)
        .with_target(false)
        .init();

    let cli = parse_cli()?;
    let mut cfg = Config::load(&cli.config)?;
    apply_env_overrides(&mut cfg);
    if let Some((path, state)) = cli.preview {
        return preview::render(&cfg, &path, &state);
    }

    // Backend selection stays overridable from the environment file.
    if std::env::var_os("SLINT_BACKEND").is_none() {
        std::env::set_var("SLINT_BACKEND", "linuxkms-software");
    }
    if std::env::var_os("SLINT_KMS_ROTATION").is_none() {
        std::env::set_var("SLINT_KMS_ROTATION", cfg.display.rotation.to_string());
    }
    info!(
        "barclock {VERSION} starting (SLINT_BACKEND={}, SLINT_KMS_ROTATION={}, config {})",
        std::env::var("SLINT_BACKEND").unwrap_or_default(),
        std::env::var("SLINT_KMS_ROTATION").unwrap_or_default(),
        cli.config.display()
    );

    wait_for_connected_display();
    let app = AppWindow::new().context(
        "creating the Slint window (is /dev/dri readable, /dev/input accessible, SLINT_BACKEND=linuxkms-*?)",
    )?;
    app.set_version_text(format!("v{VERSION}").into());
    app.set_indoor_label(cfg.general.indoor_label.to_uppercase().into());
    app.set_touch_test(cli.touch_test);

    let dpms = if cfg.display.dpms_off {
        match Dpms::discover() {
            Ok(d) => Some(d),
            Err(e) => {
                warn!("panel power control unavailable, \"off\" will be a black screen only: {e:#}");
                None
            }
        }
    } else {
        None
    };
    let backlight = Backlight::discover();
    if backlight.is_none() {
        info!("no /sys/class/backlight node: brightness is a software overlay");
    }

    let initial_light = display::load_light_state(&cfg.display.state_file).unwrap_or(LightState {
        on: true,
        brightness: cfg.display.default_brightness,
    });
    info!("initial light state: {initial_light:?}");

    let (ha_tx, mut ha_rx) = mpsc::unbounded_channel::<HaEvent>();
    let (call_tx, call_rx) = mpsc::unbounded_channel::<ServiceCall>();
    let (light_cmd_tx, mut light_cmd_rx) = mpsc::unbounded_channel::<LightCommand>();
    let (light_state_tx, light_state_rx) = watch::channel(initial_light);
    let (power_tx, mut power_rx) = mpsc::unbounded_channel::<PowerEvent>();

    let token = std::env::var("HA_TOKEN").unwrap_or_default();
    let ha_token_missing = token.trim().is_empty();
    if ha_token_missing {
        error!("HA_TOKEN is empty: create a long-lived access token in Home Assistant and put HA_TOKEN=… in /etc/barclock.env, then restart barclock");
    }
    let mqtt_creds = match (std::env::var("MQTT_USER"), std::env::var("MQTT_PASSWORD")) {
        (Ok(u), Ok(p)) if !u.is_empty() => Some((u, p)),
        _ => {
            warn!("MQTT_USER/MQTT_PASSWORD not set in /etc/barclock.env: connecting to the broker anonymously");
            None
        }
    };

    let ctl = Rc::new(UiCtl::new(app, cfg.clone(), call_tx, light_state_tx, dpms, backlight, initial_light, ha_token_missing)?);
    UI.with(|u| *u.borrow_mut() = Some(ctl.clone()));
    ctl.wire();
    ctl.start();

    // ---- networking thread ----
    let net_cfg = cfg.clone();
    let entity_ids = cfg.entity_ids();
    std::thread::Builder::new()
        .name("net".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                tokio::task::spawn_local(async move {
                    while let Some(ev) = ha_rx.recv().await {
                        let _ = slint::invoke_from_event_loop(move || with_ui(|c| c.on_ha_event(ev)));
                    }
                });
                tokio::task::spawn_local(async move {
                    while let Some(cmd) = light_cmd_rx.recv().await {
                        let _ = slint::invoke_from_event_loop(move || with_ui(|c| c.on_light_command(cmd)));
                    }
                });
                if !ha_token_missing {
                    tokio::task::spawn_local(ha_ws::run(
                        net_cfg.homeassistant.clone(),
                        token,
                        entity_ids,
                        ha_tx,
                        call_rx,
                    ));
                }
                tokio::task::spawn_local(async move {
                    while let Some(ev) = power_rx.recv().await {
                        let _ = slint::invoke_from_event_loop(move || with_ui(|c| c.on_power_event(ev)));
                    }
                });
                tokio::task::spawn_local(iotawatt::run(net_cfg.iotawatt.clone(), power_tx));
                tokio::task::spawn_local(mqtt_light::run(net_cfg.mqtt.clone(), mqtt_creds, light_cmd_tx, light_state_rx));
                tokio::task::spawn_local(async {
                    loop {
                        if display::time_is_synced() {
                            info!("system clock is NTP-synchronized");
                            let _ = slint::invoke_from_event_loop(|| with_ui(|c| c.set_time_synced()));
                            return;
                        }
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                });
                let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
                let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).expect("SIGINT handler");
                tokio::select! {
                    _ = term.recv() => info!("SIGTERM received, quitting"),
                    _ = int.recv() => info!("SIGINT received, quitting"),
                }
                let _ = slint::quit_event_loop();
            });
        })
        .context("spawning the networking thread")?;

    let outcome = ctl.app.run().context("Slint event loop");
    ctl.shutdown();
    if let Err(e) = outcome {
        error!("{e:#}");
        std::process::exit(1);
    }
    // Slint's linuxkms teardown can block on the DRM fd after the loop has quit (seen as a
    // 10 s stop-sigterm timeout followed by SIGKILL), everything worth flushing is done.
    info!("bye");
    std::process::exit(0);
}

/// Connection endpoints may also come from /etc/barclock.env (next to the secrets):
/// `HA_URL` (https://host, wss://host or a full .../api/websocket URL), `MQTT_HOST`, `MQTT_PORT`.
fn apply_env_overrides(cfg: &mut Config) {
    if let Some(url) = std::env::var("HA_URL").ok().filter(|s| !s.trim().is_empty()) {
        cfg.homeassistant.url = normalize_ha_url(url.trim());
        info!("HA_URL from the environment: {}", cfg.homeassistant.url);
    }
    if let Some(host) = std::env::var("MQTT_HOST").ok().filter(|s| !s.trim().is_empty()) {
        cfg.mqtt.host = host.trim().to_string();
        info!("MQTT_HOST from the environment: {}", cfg.mqtt.host);
    }
    if let Some(port) = std::env::var("MQTT_PORT").ok().filter(|s| !s.trim().is_empty()) {
        match port.trim().parse::<u16>() {
            Ok(p) => cfg.mqtt.port = p,
            Err(_) => warn!("MQTT_PORT={port:?} is not a port number, keeping {}", cfg.mqtt.port),
        }
    }
}

/// `https://ha.example` -> `wss://ha.example/api/websocket`, explicit ws(s) URLs with a path are kept.
fn normalize_ha_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    let (scheme, rest) = match url.split_once("://") {
        Some(("https", rest)) | Some(("wss", rest)) => ("wss", rest),
        Some(("http", rest)) | Some(("ws", rest)) => ("ws", rest),
        Some((other, rest)) => {
            warn!("HA_URL scheme {other:?} is not http(s)/ws(s); assuming wss");
            ("wss", rest)
        }
        None => ("wss", url),
    };
    if rest.contains('/') {
        format!("{scheme}://{rest}")
    } else {
        format!("{scheme}://{rest}/api/websocket")
    }
}

thread_local! {
    static UI: RefCell<Option<Rc<UiCtl>>> = const { RefCell::new(None) };
}

/// Runs `f` with the UI controller (main thread only).
fn with_ui(f: impl FnOnce(&UiCtl)) {
    let ctl = UI.with(|u| u.borrow().clone());
    if let Some(ctl) = ctl {
        f(&ctl);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PendingKey {
    Preset(i32),
    Toggle(&'static str),
    /// The VENTILATEUR slider (`fan.set_percentage`).
    Fan,
}

struct Inflight {
    key: PendingKey,
    /// Entities whose next state change clears this pending marker.
    entities: Vec<String>,
    /// Home Assistant answered `success: true`. We are only waiting for the state to follow.
    confirmed: bool,
    _timeout: Timer,
}

struct St {
    ha_connected: bool,
    time_synced: bool,
    entities: HashMap<String, EntityState>,
    light: LightState,
    awake: bool,
    /// The CRTC is (about to be) off: no Slint property may be written until `thaw()`.
    frozen: bool,
    /// Something changed while frozen, refresh everything on thaw.
    dirty: bool,
    next_request: u64,
    inflight: HashMap<u64, Inflight>,
    last_clock: Option<clock::ClockText>,
    /// The IoTaWatt is answering, so its watts win over the Home Assistant sensors.
    iotawatt_live: bool,
}

/// All UI state and timers. Lives in a thread-local on the Slint thread.
struct UiCtl {
    app: AppWindow,
    cfg: Config,
    tz: Tz,
    zones: Vec<(String, Tz)>,
    zones_model: Rc<VecModel<ZoneTime>>,
    calls: mpsc::UnboundedSender<ServiceCall>,
    light_tx: watch::Sender<LightState>,
    dpms: Option<Dpms>,
    backlight: Option<Backlight>,
    ha_token_missing: bool,
    st: RefCell<St>,
    clock_timer: Timer,
    wake_timer: Timer,
    dpms_timer: Timer,
    toast_timer: Timer,
    disconnect_timer: Timer,
}

impl UiCtl {
    #[allow(clippy::too_many_arguments)]
    fn new(
        app: AppWindow,
        cfg: Config,
        calls: mpsc::UnboundedSender<ServiceCall>,
        light_tx: watch::Sender<LightState>,
        dpms: Option<Dpms>,
        backlight: Option<Backlight>,
        light: LightState,
        ha_token_missing: bool,
    ) -> Result<Self> {
        let tz = cfg.local_tz()?;
        let zones = cfg.zone_tzs()?;
        let zones_model = Rc::new(VecModel::from(
            zones
                .iter()
                .map(|(label, _)| ZoneTime { label: label.to_uppercase().into(), time: "--:--".into() })
                .collect::<Vec<_>>(),
        ));
        app.set_zones(ModelRc::from(zones_model.clone()));
        Ok(Self {
            app,
            cfg,
            tz,
            zones,
            zones_model,
            calls,
            light_tx,
            dpms,
            backlight,
            ha_token_missing,
            st: RefCell::new(St {
                ha_connected: false,
                time_synced: false,
                entities: HashMap::new(),
                light,
                awake: false,
                frozen: false,
                dirty: false,
                next_request: 1,
                inflight: HashMap::new(),
                last_clock: None,
                iotawatt_live: false,
            }),
            clock_timer: Timer::default(),
            wake_timer: Timer::default(),
            dpms_timer: Timer::default(),
            toast_timer: Timer::default(),
            disconnect_timer: Timer::default(),
        })
    }

    fn wire(&self) {
        self.app.on_preset_clicked(|i| with_ui(|c| c.preset(i)));
        self.app.on_toggle_clicked(|k| with_ui(|c| c.toggle(k.as_str())));
        self.app.on_fan_changed(|p| with_ui(|c| c.set_fan(p)));
        self.app.on_wake_touch(|| with_ui(|c| c.wake()));
        self.app.on_touch_test_point(|x, y| info!("touch-test: x={x:.0} y={y:.0} (window 1920x480 after rotation)"));
        self.app.global::<Activity>().on_touch(|| with_ui(|c| c.activity()));
    }

    fn start(&self) {
        self.refresh_connecting();
        self.tick();
        self.light_changed(false);
    }

    fn shutdown(&self) {
        if let Some(d) = &self.dpms {
            if let Err(e) = d.set_on(true) {
                warn!("could not turn the panel back on at exit: {e:#}");
            }
        }
    }

    /// The only way to write UI properties: dropped while the CRTC is off (see `thaw`).
    fn ui(&self, f: impl FnOnce(&AppWindow)) {
        let frozen = {
            let mut st = self.st.borrow_mut();
            if st.frozen {
                st.dirty = true;
            }
            st.frozen
        };
        if !frozen {
            f(&self.app);
        }
    }

    // ---------------- clock ----------------

    fn tick(&self) {
        let now = Utc::now();
        let text = clock::render(now, self.tz, &self.zones, &self.cfg.general.language);
        let changed = {
            let mut st = self.st.borrow_mut();
            let changed = st.last_clock.as_ref() != Some(&text);
            let frozen = st.frozen;
            if !frozen {
                st.last_clock = Some(text.clone());
            }
            changed && !frozen
        };
        if changed {
            let app = &self.app;
            let (hm, sec) = text.time.split_at(text.time.len().saturating_sub(2));
            if app.get_time_hm().as_str() != hm {
                app.set_time_hm(hm.into());
            }
            app.set_time_s(sec.into());
            if app.get_date_text().as_str() != text.date {
                app.set_date_text(text.date.as_str().into());
            }
            let weekday = text.weekday.to_uppercase();
            if app.get_weekday_text().as_str() != weekday {
                app.set_weekday_text(weekday.into());
            }
            for (i, (_, t)) in text.zones.iter().enumerate() {
                if let Some(row) = self.zones_model.row_data(i) {
                    if row.time.as_str() != t {
                        self.zones_model.set_row_data(i, ZoneTime { label: row.label, time: t.as_str().into() });
                    }
                }
            }
        }
        self.clock_timer.start(TimerMode::SingleShot, clock::until_next_second(Utc::now()), || {
            with_ui(|c| c.tick())
        });
    }

    fn set_time_synced(&self) {
        self.st.borrow_mut().time_synced = true;
        self.refresh_connecting();
    }

    fn refresh_connecting(&self) {
        let (ha, ts) = {
            let st = self.st.borrow();
            (st.ha_connected, st.time_synced)
        };
        let connecting = !(ha && ts);
        let mut missing = Vec::new();
        if !ha {
            missing.push(if self.ha_token_missing { "HA_TOKEN MANQUANT DANS /etc/barclock.env" } else { "HOME ASSISTANT" });
        }
        if !ts {
            missing.push("HORLOGE NTP");
        }
        let detail = missing.join(" · ");
        if self.app.get_connecting() != connecting {
            info!("UI: {}", if connecting { format!("Connecting… screen (waiting for {detail})") } else { "dashboard visible".to_string() });
        }
        self.ui(|app| {
            app.set_connecting(connecting);
            app.set_connecting_detail(detail.as_str().into());
            app.set_ha_online(ha);
        });
    }

    // ---------------- Home Assistant ----------------

    /// Watts straight from the IoTaWatt. While it answers, these win over the Home Assistant
    /// sensors for the house total and the servers, on `Down` those two go back to Home Assistant.
    fn on_power_event(&self, ev: PowerEvent) {
        let (house_id, servers_id) =
            (self.cfg.entities.house_power.clone(), self.cfg.entities.servers_power.clone());
        match ev {
            PowerEvent::Live { house, servers } => {
                let was_live = std::mem::replace(&mut self.st.borrow_mut().iotawatt_live, true);
                if !was_live {
                    info!("house and servers now come straight from the IoTaWatt");
                }
                if let Some(w) = house {
                    let text = fmt_watts(w);
                    self.ui(move |app| app.set_house_power_text(text.as_str().into()));
                }
                if let Some(w) = servers {
                    let text = fmt_watts(w);
                    self.ui(move |app| app.set_servers_power_text(text.as_str().into()));
                }
                // A configured value the device didn't return keeps using its own HA sensor.
                if house.is_none() {
                    self.refresh_entity(&house_id);
                }
                if servers.is_none() && !servers_id.trim().is_empty() {
                    self.refresh_entity(&servers_id);
                }
            }
            PowerEvent::Down => {
                let was_live = std::mem::replace(&mut self.st.borrow_mut().iotawatt_live, false);
                if was_live {
                    info!("house and servers fall back to the Home Assistant sensors");
                }
                self.refresh_entity(&house_id);
                if !servers_id.trim().is_empty() {
                    self.refresh_entity(&servers_id);
                }
            }
        }
    }

    fn on_ha_event(&self, ev: HaEvent) {
        match ev {
            HaEvent::Connected => {
                info!("Home Assistant connected");
                self.disconnect_timer.stop();
                self.st.borrow_mut().ha_connected = true;
                self.refresh_connecting();
            }
            HaEvent::Disconnected { reason } => {
                warn!("Home Assistant disconnected: {reason}");
                let grace = Duration::from_secs(self.cfg.homeassistant.connecting_after_s);
                self.disconnect_timer.start(TimerMode::SingleShot, grace, || {
                    with_ui(|c| {
                        c.st.borrow_mut().ha_connected = false;
                        c.refresh_connecting();
                    })
                });
            }
            HaEvent::StateChanged { entity_id, state } => {
                debug!("{entity_id} = {} {:?}", state.state, state.attributes.keys().collect::<Vec<_>>());
                let pending_cleared = {
                    let mut st = self.st.borrow_mut();
                    st.entities.insert(entity_id.clone(), state);
                    let before = st.inflight.len();
                    st.inflight.retain(|_, inf| !inf.entities.iter().any(|e| *e == entity_id));
                    before != st.inflight.len()
                };
                if pending_cleared {
                    self.refresh_pending();
                }
                self.refresh_entity(&entity_id);
            }
            HaEvent::StateRemoved { entity_id } => {
                self.st.borrow_mut().entities.remove(&entity_id);
                self.refresh_entity(&entity_id);
            }
            HaEvent::CallResult { request, success, error } => {
                let failed = {
                    let mut st = self.st.borrow_mut();
                    match st.inflight.get_mut(&request) {
                        Some(inf) if success => {
                            inf.confirmed = true;
                            None
                        }
                        Some(_) => {
                            st.inflight.remove(&request);
                            Some(error.unwrap_or_else(|| "erreur inconnue".into()))
                        }
                        None => None,
                    }
                };
                if let Some(msg) = failed {
                    warn!("service call {request} failed: {msg}");
                    self.toast(format!("Échec : {msg}"));
                    self.refresh_pending();
                }
            }
        }
    }

    fn send(&self, mut call: ServiceCall, key: PendingKey) {
        let id = {
            let mut st = self.st.borrow_mut();
            st.next_request += 1;
            st.next_request
        };
        call.request = id;
        let entities = call.entity_ids.clone();
        let timeout = Timer::default();
        timeout.start(
            TimerMode::SingleShot,
            Duration::from_secs(self.cfg.homeassistant.call_timeout_s),
            move || with_ui(|c| c.call_timeout(id)),
        );
        self.st.borrow_mut().inflight.insert(id, Inflight { key, entities, confirmed: false, _timeout: timeout });
        info!("call_service {}.{} {:?} {}", call.domain, call.service, call.entity_ids, call.service_data);
        if self.calls.send(call).is_err() {
            self.st.borrow_mut().inflight.remove(&id);
            self.toast("Home Assistant indisponible");
        }
        self.refresh_pending();
    }

    fn call_timeout(&self, id: u64) {
        let unconfirmed = {
            let mut st = self.st.borrow_mut();
            st.inflight.remove(&id).map(|inf| !inf.confirmed)
        };
        match unconfirmed {
            Some(true) => {
                warn!("service call {id}: no answer from Home Assistant");
                self.toast("Pas de réponse de Home Assistant");
            }
            Some(false) => debug!("service call {id}: confirmed but the state did not change"),
            None => return,
        }
        self.refresh_pending();
    }

    fn preset(&self, i: i32) {
        let e = &self.cfg.entities;
        let both = vec![e.light_ceiling.clone(), e.light_monitor.clone()];
        let call = |service: &str, data: Value, ids: Vec<String>| ServiceCall {
            request: 0,
            domain: "light".into(),
            service: service.into(),
            service_data: data,
            entity_ids: ids,
        };
        let calls = match i {
            0 => vec![
                call("turn_on", json!({"brightness": 1, "transition": 1}), vec![e.light_ceiling.clone()]),
                call("turn_on", json!({"brightness_pct": 33, "transition": 1}), vec![e.light_monitor.clone()]),
            ],
            1 => vec![call("turn_on", json!({"brightness": 127, "transition": 1}), both)],
            2 => vec![call("turn_on", json!({"brightness": 255, "transition": 1}), both)],
            3 => vec![call("turn_off", json!({"transition": 1}), both)],
            _ => return,
        };
        for c in calls {
            self.send(c, PendingKey::Preset(i));
        }
    }

    fn toggle(&self, key: &str) {
        let e = &self.cfg.entities;
        if key == "display" {
            // The kiosk PC's monitor: a select entity driven as an on/off toggle.
            let is_on = self.st.borrow().entities.get(&e.kiosk_display_power).is_some_and(EntityState::is_on);
            let option = if is_on { e.kiosk_display_off_option.clone() } else { "on".to_string() };
            self.send(
                ServiceCall {
                    request: 0,
                    domain: "select".into(),
                    service: "select_option".into(),
                    service_data: json!({ "option": option }),
                    entity_ids: vec![e.kiosk_display_power.clone()],
                },
                PendingKey::Toggle("display"),
            );
            return;
        }
        if key == "cam_street" || key == "cam_gallery" {
            // One select picks the camera, so choosing one drops the other. Pressing the active
            // one selects the "off" option instead.
            let (key, want) = if key == "cam_street" {
                ("cam_street", e.kiosk_camera_street_option.clone())
            } else {
                ("cam_gallery", e.kiosk_camera_gallery_option.clone())
            };
            let showing = self.camera_option_active(&want);
            let option = if showing { e.kiosk_camera_off_option.clone() } else { want };
            self.send(
                ServiceCall {
                    request: 0,
                    domain: "select".into(),
                    service: "select_option".into(),
                    service_data: json!({ "option": option }),
                    entity_ids: vec![e.kiosk_camera.clone()],
                },
                PendingKey::Toggle(key),
            );
            return;
        }
        let (key, entity, domain) = match key {
            "ceiling" => ("ceiling", &e.light_ceiling, "light"),
            "monitor" => ("monitor", &e.light_monitor, "light"),
            "grafana" => ("grafana", &e.kiosk_grafana, "switch"),
            other => {
                warn!("unknown toggle {other:?}");
                return;
            }
        };
        let service = if domain == "light" {
            "toggle"
        } else {
            let is_on = self.st.borrow().entities.get(entity).is_some_and(EntityState::is_on);
            if is_on { "turn_off" } else { "turn_on" }
        };
        self.send(
            ServiceCall {
                request: 0,
                domain: domain.into(),
                service: service.into(),
                service_data: Value::Null,
                entity_ids: vec![entity.clone()],
            },
            PendingKey::Toggle(key),
        );
    }

    /// The VENTILATEUR slider was released: one `fan.set_percentage` per gesture. 0 % is the
    /// fan's floor, not off (the operator's fan keeps turning), so no turn_on/turn_off here.
    fn set_fan(&self, pct: i32) {
        let e = &self.cfg.entities;
        if e.office_fan.trim().is_empty() {
            return;
        }
        let pct = pct.clamp(0, 100);
        self.send(
            ServiceCall {
                request: 0,
                domain: "fan".into(),
                service: "set_percentage".into(),
                service_data: json!({ "percentage": pct }),
                entity_ids: vec![e.office_fan.clone()],
            },
            PendingKey::Fan,
        );
    }

    fn refresh_pending(&self) {
        let e = self.cfg.entities.clone();
        for id in [&e.light_ceiling, &e.light_monitor, &e.kiosk_grafana, &e.kiosk_camera, &e.kiosk_display_power, &e.office_fan] {
            self.refresh_entity(id);
        }
        let preset = {
            let st = self.st.borrow();
            st.inflight
                .values()
                .find_map(|inf| match inf.key {
                    PendingKey::Preset(i) => Some(i),
                    _ => None,
                })
                .unwrap_or(-1)
        };
        self.ui(|app| app.set_preset_pending(preset));
    }

    /// Is the camera select currently sitting on this option?
    fn camera_option_active(&self, option: &str) -> bool {
        let e = &self.cfg.entities;
        self.st
            .borrow()
            .entities
            .get(&e.kiosk_camera)
            .filter(|x| x.is_available())
            .is_some_and(|x| x.state.eq_ignore_ascii_case(option.trim()))
    }

    /// A camera button's state: lit while the select shows that camera.
    fn camera_toggle_state(&self, option: &str, key: &'static str) -> ToggleState {
        let e = &self.cfg.entities;
        let st = self.st.borrow();
        let ent = st.entities.get(&e.kiosk_camera);
        let available = ent.is_some_and(EntityState::is_available);
        let on = available
            && ent.is_some_and(|x| x.state.eq_ignore_ascii_case(option.trim()));
        ToggleState {
            on,
            pending: st.inflight.values().any(|inf| inf.key == PendingKey::Toggle(key)),
            available,
        }
    }

    fn toggle_state(&self, entity: &str, key: &'static str) -> ToggleState {
        let st = self.st.borrow();
        let ent = st.entities.get(entity);
        ToggleState {
            on: ent.is_some_and(EntityState::is_on),
            pending: st.inflight.values().any(|inf| inf.key == PendingKey::Toggle(key) || matches!(inf.key, PendingKey::Preset(_)) && inf.entities.iter().any(|e| e == entity)),
            available: ent.is_some_and(EntityState::is_available),
        }
    }

    fn refresh_entity(&self, id: &str) {
        let e = &self.cfg.entities;
        let ent = self.st.borrow().entities.get(id).cloned();
        if id == e.weather {
            let place = ent
                .as_ref()
                .and_then(|w| w.attr_str("friendly_name"))
                .map(|n| n.to_uppercase())
                .unwrap_or_default();
            let (line, condition, icon) = match &ent {
                Some(w) if w.is_available() => {
                    let num = |v: Option<f64>| v.map_or("—".to_string(), |x| format!("{}", x.round() as i64));
                    let temp = w.attr_f64("temperature").map_or("—".to_string(), |t| format!("{t:.1}"));
                    (
                        format!(
                            "{} °C   {} %   {} km/h",
                            temp,
                            num(w.attr_f64("humidity")),
                            num(w.attr_f64("wind_speed"))
                        ),
                        self.cfg.weather_label(&w.state),
                        w.state.clone(),
                    )
                }
                _ => ("— °C   — %   — km/h".to_string(), self.cfg.weather_label("unavailable"), String::new()),
            };
            self.ui(|app| {
                app.set_weather_line(line.as_str().into());
                app.set_weather_condition(condition.as_str().into());
                app.set_weather_icon(icon.as_str().into());
                app.set_weather_place(place.as_str().into());
            });
        } else if id == e.house_power {
            if !self.st.borrow().iotawatt_live {
                let text = ent.as_ref().and_then(EntityState::state_f64).map_or("— W".to_string(), fmt_watts);
                self.ui(|app| app.set_house_power_text(text.as_str().into()));
            }
        } else if !e.office_fan.trim().is_empty() && id == e.office_fan {
            let pending = self.st.borrow().inflight.values().any(|inf| inf.key == PendingKey::Fan);
            let available = ent.as_ref().is_some_and(EntityState::is_available);
            let on = ent.as_ref().is_some_and(EntityState::is_on);
            let pct = ent.as_ref().and_then(|f| f.attr_f64("percentage")).map_or(0, |p| p.round().clamp(0.0, 100.0) as i32);
            self.ui(move |app| {
                app.set_fan_available(available);
                app.set_fan_on(on);
                app.set_fan_pending(pending);
                app.set_fan_percentage(pct);
            });
        } else if !e.servers_power.trim().is_empty() && id == e.servers_power {
            if !self.st.borrow().iotawatt_live {
                let text = ent.as_ref().and_then(EntityState::state_f64).map_or("— W".to_string(), fmt_watts);
                self.ui(|app| app.set_servers_power_text(text.as_str().into()));
            }
        } else if self.is_load(id) {
            self.refresh_loads();
        } else if (!e.office_temperature.trim().is_empty() && id == e.office_temperature)
            || (!e.office_humidity.trim().is_empty() && id == e.office_humidity)
        {
            self.refresh_indoor();
        } else if id == e.printer_progress
            || id == e.printer_time_left
            || id == e.printer_power
            || id == e.printer_print_state
            || id == e.printer_klipper_state
        {
            let (status, progress_text, left_text) = self.printer_status();
            let active = matches!(status, PrinterStatus::Printing | PrinterStatus::Paused);
            let label = self.cfg.printer_label(status);
            self.ui(|app| {
                app.set_printer_status(label.into());
                app.set_printer_mode(status.key().into());
                app.set_printer_active(active);
                app.set_printer_progress_text(progress_text.as_str().into());
                app.set_printer_time_left_text(left_text.as_str().into());
            });
        } else if id == e.light_ceiling || id == e.light_monitor {
            self.refresh_preset_active();
            let ceiling = self.toggle_state(&e.light_ceiling, "ceiling");
            let monitor = self.toggle_state(&e.light_monitor, "monitor");
            let summary = {
                let st = self.st.borrow();
                let pct = |ent: Option<&EntityState>| {
                    ent.filter(|x| x.is_on())
                        .and_then(|x| x.attr_f64("brightness"))
                        .map(|b| format!("{} %", (b / 255.0 * 100.0).round() as i64))
                        .unwrap_or_else(|| "OFF".into())
                };
                format!("PLAFONNIER {} · ÉCRAN {}", pct(st.entities.get(&e.light_ceiling)), pct(st.entities.get(&e.light_monitor)))
            };
            self.ui(|app| {
                app.set_light_ceiling(ceiling);
                app.set_light_monitor(monitor);
                app.set_lights_summary(summary.as_str().into());
            });
        } else if id == e.kiosk_grafana {
            let s = self.toggle_state(id, "grafana");
            self.ui(|app| app.set_kiosk_grafana(s));
        } else if !e.kiosk_camera.trim().is_empty() && id == e.kiosk_camera {
            let street = self.camera_toggle_state(&e.kiosk_camera_street_option, "cam_street");
            let gallery = self.camera_toggle_state(&e.kiosk_camera_gallery_option, "cam_gallery");
            self.ui(move |app| {
                app.set_kiosk_cam_street(street);
                app.set_kiosk_cam_gallery(gallery);
            });
        } else if id == e.kiosk_display_power {
            let s = self.toggle_state(id, "display");
            let mode = match &ent {
                Some(x) if x.is_available() => x.state.to_uppercase(),
                _ => String::new(),
            };
            self.ui(|app| {
                app.set_kiosk_display(s);
                app.set_kiosk_display_mode(mode.as_str().into());
            });
        }
    }

    fn printer_status(&self) -> (PrinterStatus, String, String) {
        let e = &self.cfg.entities;
        let st = self.st.borrow();
        let get = |id: &String| if id.trim().is_empty() { None } else { st.entities.get(id) };
        let progress = get(&e.printer_progress).and_then(EntityState::state_f64);
        let hours = get(&e.printer_time_left).and_then(EntityState::state_f64);
        let power = get(&e.printer_power).filter(|x| x.is_available()).map(EntityState::is_on);
        let klipper = get(&e.printer_klipper_state).filter(|x| x.is_available()).map(|x| x.state.clone());
        let print = get(&e.printer_print_state).filter(|x| x.is_available()).map(|x| x.state.clone());
        let any_available = [
            get(&e.printer_progress),
            get(&e.printer_time_left),
            get(&e.printer_print_state),
            get(&e.printer_klipper_state),
            get(&e.printer_power),
        ]
        .iter()
        .any(|x| x.is_some_and(EntityState::is_available));
        let status = derive_printer_status(power, klipper.as_deref(), print.as_deref(), progress, hours, any_available);
        let progress_text = progress.map_or(String::new(), |p| format!("{} %", p.round() as i64));
        let left_text = hours.map_or(String::new(), |h| format!("{} min", (h * 60.0).round() as i64));
        (status, progress_text, left_text)
    }

    /// Which light preset matches the two lights' current states (-1 = none). Tolerances absorb
    /// Home Assistant's rounding (a group asked for 127 reports 128, 33 % is 84).
    fn refresh_preset_active(&self) {
        let e = &self.cfg.entities;
        let st = self.st.borrow();
        let light = |id: &String| st.entities.get(id).filter(|x| x.is_available()).map(|x| (x.is_on(), x.attr_f64("brightness").unwrap_or(0.0)));
        let (ceiling, monitor) = (light(&e.light_ceiling), light(&e.light_monitor));
        let near = |v: f64, target: f64| (v - target).abs() <= 3.0;
        let active = match (ceiling, monitor) {
            (Some((false, _)), Some((false, _))) => 3,
            (Some((true, c)), Some((true, m))) if c >= 252.0 && m >= 252.0 => 2,
            (Some((true, c)), Some((true, m))) if near(c, 127.0) && near(m, 127.0) => 1,
            (Some((true, c)), Some((true, m))) if c <= 3.0 && near(m, 84.0) => 0,
            _ => -1,
        };
        drop(st);
        self.ui(move |app| app.set_preset_active(active));
    }

    fn is_load(&self, id: &str) -> bool {
        let e = &self.cfg.entities;
        [
            &e.stove_power,
            &e.car_charge_power,
            &e.car_battery,
            &e.water_heater_power,
            &e.washer_power,
            &e.dryer_power,
        ]
            .into_iter()
            .any(|x| !x.trim().is_empty() && id == x)
    }

    /// The load columns under the house total (stove, car, water heater, washer, dryer): icon,
    /// name and watts, lit when the load draws more than its threshold, dim when idle, hidden
    /// when not configured. `scale` converts the sensor's unit to watts (the car reports kW).
    fn refresh_loads(&self) {
        let e = &self.cfg.entities;
        let t = &self.cfg.thresholds;
        let st = self.st.borrow();
        let load = |id: &String, threshold: f64, scale: f64| -> LoadState {
            if id.trim().is_empty() {
                return LoadState { on: false, available: false, configured: false, power: "".into(), badge: "".into() };
            }
            let ent = st.entities.get(id);
            let available = ent.is_some_and(EntityState::is_available);
            let watts = ent.and_then(EntityState::state_f64).map(|v| v * scale);
            let on = watts.is_some_and(|w| w > threshold * scale);
            let power = watts.map_or("—".to_string(), fmt_watts);
            LoadState { on, available, configured: true, power: power.into(), badge: "".into() }
        };
        let stove = load(&e.stove_power, t.stove_flame_w, 1.0);
        let mut car = load(&e.car_charge_power, t.car_charging_kw, 1000.0);
        // the car's battery level rides along on the AUTO column's name line
        if !e.car_battery.trim().is_empty() {
            if let Some(pct) = st
                .entities
                .get(&e.car_battery)
                .filter(|x| x.is_available())
                .and_then(EntityState::state_f64)
            {
                car.badge = format!("{:.1} %", pct.clamp(0.0, 100.0)).into();
            }
        }
        let water = load(&e.water_heater_power, t.water_heater_w, 1.0);
        let washer = load(&e.washer_power, t.washer_w, 1.0);
        let dryer = load(&e.dryer_power, t.dryer_w, 1.0);
        drop(st);
        self.ui(move |app| {
            app.set_load_stove(stove);
            app.set_load_car(car);
            app.set_load_water_heater(water);
            app.set_load_washer(washer);
            app.set_load_dryer(dryer);
        });
    }

    /// Indoor line under the weather: "21.9 °C   56 %" from the office thermometer.
    fn refresh_indoor(&self) {
        let e = &self.cfg.entities;
        let st = self.st.borrow();
        let get = |id: &String| {
            if id.trim().is_empty() {
                None
            } else {
                st.entities.get(id).filter(|x| x.is_available()).and_then(EntityState::state_f64)
            }
        };
        let t = get(&e.office_temperature).map_or("—".to_string(), |v| format!("{v:.1} °C"));
        let h = get(&e.office_humidity).map_or("—".to_string(), |v| format!("{} %", v.round() as i64));
        drop(st);
        let line = format!("{t}   {h}");
        self.ui(move |app| app.set_indoor_line(line.as_str().into()));
    }

    fn refresh_all_entities(&self) {
        for id in self.cfg.entity_ids() {
            self.refresh_entity(&id);
        }
    }

    fn refresh_everything(&self) {
        self.refresh_connecting();
        self.tick();
        self.refresh_all_entities();
        self.refresh_pending();
    }

    fn toast(&self, msg: impl Into<String>) {
        let msg: String = msg.into();
        self.ui(|app| {
            app.set_toast_text(msg.as_str().into());
            app.set_toast_visible(true);
        });
        self.toast_timer.start(TimerMode::SingleShot, Duration::from_secs(self.cfg.display.toast_seconds), || {
            with_ui(|c| c.ui(|app| app.set_toast_visible(false)))
        });
    }

    // ---------------- brightness / panel power ----------------

    fn on_light_command(&self, cmd: LightCommand) {
        match cmd {
            LightCommand::Set { on, brightness, transition } => {
                debug!("light command: on={on} brightness={brightness:?} transition={transition:?}");
                let mut st = self.st.borrow_mut();
                st.light.on = on;
                if let Some(b) = brightness {
                    st.light.brightness = b.max(1);
                }
            }
            LightCommand::Restore(state) => {
                info!("restored light state from the broker: {state:?}");
                self.st.borrow_mut().light = state;
            }
        }
        self.light_changed(true);
    }

    fn light_changed(&self, save: bool) {
        let light = self.st.borrow().light;
        if save {
            display::save_light_state(&self.cfg.display.state_file, light);
        }
        let _ = self.light_tx.send(light);
        self.refresh_screen();
    }

    fn effective_brightness(&self) -> (u8, bool) {
        let st = self.st.borrow();
        let d = &self.cfg.display;
        let eff = if st.awake {
            if st.light.on { st.light.brightness.max(d.wake_brightness) } else { d.wake_brightness }
        } else if st.light.on {
            st.light.brightness
        } else {
            0
        };
        (eff, !st.awake && eff < d.readable_threshold)
    }

    fn refresh_screen(&self) {
        let (eff, guard) = self.effective_brightness();
        self.dpms_timer.stop();
        if eff == 0 {
            if self.st.borrow().frozen {
                return;
            }
            if let Some(bl) = &self.backlight {
                if let Err(e) = bl.set(0) {
                    warn!("backlight off failed: {e:#}");
                }
            }
            self.app.set_dim_opacity(1.0);
            self.app.set_wake_guard(true);
            if self.dpms.is_some() {
                // Step 1: let the black frame reach the panel and every animation finish.
                let delay = Duration::from_millis(self.cfg.display.dpms_delay_ms);
                self.dpms_timer.start(TimerMode::SingleShot, delay, || with_ui(|c| c.freeze()));
            }
        } else {
            self.thaw();
            let opacity = if self.backlight.is_some() {
                0.0
            } else {
                (1.0 - f32::from(eff) / 255.0).min(self.cfg.display.max_dim_opacity)
            };
            if let Some(bl) = &self.backlight {
                if let Err(e) = bl.set(eff) {
                    warn!("backlight set failed: {e:#}");
                }
            }
            self.app.set_dim_opacity(opacity);
            self.app.set_wake_guard(guard);
            debug!("screen: brightness {eff}/255 overlay {opacity:.2} guard {guard}");
        }
    }

    /// Step 2: stop writing to the UI, so no page flip can be queued after the signal is cut.
    fn freeze(&self) {
        self.st.borrow_mut().frozen = true;
        self.dpms_timer.start(TimerMode::SingleShot, Duration::from_millis(1000), || with_ui(|c| c.cut_signal()));
    }

    /// Step 3: DPMS Off. Only reachable through `freeze()`.
    fn cut_signal(&self) {
        if let Some(d) = &self.dpms {
            if let Err(e) = d.set_on(false) {
                warn!("DPMS off failed, keeping the black screen: {e:#}");
                self.thaw();
            }
        }
    }

    /// Turn the signal back on (blocking modeset), then catch the UI up.
    fn thaw(&self) {
        let was_frozen = {
            let mut st = self.st.borrow_mut();
            let was = st.frozen;
            st.frozen = false;
            st.dirty = false;
            was
        };
        if !was_frozen {
            return;
        }
        if let Some(d) = &self.dpms {
            if let Err(e) = d.set_on(true) {
                error!("DPMS on failed: {e:#}");
            }
        }
        self.refresh_everything();
    }

    fn wake(&self) {
        info!("touch while dimmed/off: waking for {} s", self.cfg.display.wake_seconds);
        self.st.borrow_mut().awake = true;
        self.arm_wake_timer();
        self.refresh_screen();
    }

    fn arm_wake_timer(&self) {
        self.wake_timer.start(TimerMode::SingleShot, Duration::from_secs(self.cfg.display.wake_seconds), || {
            with_ui(|c| c.end_wake())
        });
    }

    fn end_wake(&self) {
        self.st.borrow_mut().awake = false;
        self.refresh_screen();
    }

    /// A touch on a control: keep the wake window open.
    fn activity(&self) {
        if self.st.borrow().awake {
            self.arm_wake_timer();
        }
    }
}

/// The 3D printer module's status.
/// * `power`: the power switch (`Some(true)` on, `Some(false)` off, `None` unavailable/unconfigured)
/// * `klipper`: Moonraker `printer_state` (ready/startup/shutdown/error), `print`: `current_print_state`
///   (standby/printing/paused/complete/cancelled/error), `None` when unavailable/unconfigured.
///
/// OFF only when the switch says off, or when Klipper is shut down and the switch doesn't say on
/// (a powered printer in emergency shutdown is an ERROR, not off). Klipper `error`/`shutdown`
/// beats a stale print state. Without a print-state sensor, a running print is inferred from
/// progress > 0 with time left > 0.
fn derive_printer_status(
    power: Option<bool>,
    klipper: Option<&str>,
    print: Option<&str>,
    progress: Option<f64>,
    hours: Option<f64>,
    any_available: bool,
) -> PrinterStatus {
    let powered_on = power == Some(true);
    if power == Some(false) || (!powered_on && klipper == Some("shutdown")) {
        return PrinterStatus::Off;
    }
    if !any_available {
        return PrinterStatus::Unknown;
    }
    if matches!(klipper, Some("shutdown" | "error")) {
        return PrinterStatus::Error;
    }
    match print {
        Some("printing") => PrinterStatus::Printing,
        Some("paused") => PrinterStatus::Paused,
        Some("error") => PrinterStatus::Error,
        Some(_) => PrinterStatus::Idle,
        None if progress.is_some_and(|p| p > 0.0 && p < 100.0) && hours.is_some_and(|h| h > 0.0) => {
            PrinterStatus::Printing
        }
        None => PrinterStatus::Idle,
    }
}

/// Block until some DRM connector reports `connected`. Slint's linuxkms backend panics inside its
/// generated code when no display is attached (it falls back to /dev/fb* and gives up), which would
/// turn an unplugged cable into a 2 s crash loop under systemd. Only for linuxkms backends.
fn wait_for_connected_display() {
    if !std::env::var("SLINT_BACKEND").unwrap_or_default().starts_with("linuxkms") {
        return;
    }
    let connected = || {
        std::fs::read_dir("/sys/class/drm")
            .map(|dir| {
                dir.flatten().any(|e| {
                    std::fs::read_to_string(e.path().join("status")).is_ok_and(|s| s.trim() == "connected")
                })
            })
            .unwrap_or(true) // no sysfs: let Slint report the real error
    };
    let mut waited = 0u64;
    while !connected() {
        if waited % 30 == 0 {
            warn!("no display connected on any DRM connector, waiting (plug the HDMI cable back in)");
        }
        std::thread::sleep(Duration::from_secs(2));
        waited += 2;
    }
    if waited > 0 {
        info!("display connected after {waited} s");
        // give the kernel a moment to read the EDID and publish the modes
        std::thread::sleep(Duration::from_millis(1500));
    }
}

/// `1234.5` -> `"1 235 W"`
fn fmt_watts(v: f64) -> String {
    let n = v.round() as i64;
    let digits = n.abs().to_string();
    let mut out = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(' ');
        }
        out.push(ch);
    }
    format!("{}{} W", if n < 0 { "-" } else { "" }, out)
}

#[cfg(test)]
mod tests {
    use super::{derive_printer_status, fmt_watts, normalize_ha_url};
    use crate::config::PrinterStatus as P;

    #[test]
    fn printer_status_table() {
        let d = derive_printer_status;
        // the observed powered-off signature: switch off, klipper shutdown, stale "complete"
        assert_eq!(d(Some(false), Some("shutdown"), Some("complete"), Some(0.0), Some(0.0), true), P::Off);
        // switch off always wins, even over a stale "printing" with progress
        assert_eq!(d(Some(false), Some("ready"), Some("printing"), Some(42.0), Some(2.0), true), P::Off);
        // no switch configured: shutdown means off
        assert_eq!(d(None, Some("shutdown"), Some("standby"), None, None, true), P::Off);
        // powered printer in emergency shutdown / error is an error, not off
        assert_eq!(d(Some(true), Some("shutdown"), Some("error"), Some(37.2), Some(1.0), true), P::Error);
        assert_eq!(d(Some(true), Some("error"), Some("complete"), Some(0.0), Some(0.0), true), P::Error);
        // normal states
        assert_eq!(d(Some(true), Some("ready"), Some("printing"), Some(42.0), Some(1.97), true), P::Printing);
        assert_eq!(d(Some(true), Some("ready"), Some("paused"), Some(42.0), Some(1.97), true), P::Paused);
        assert_eq!(d(Some(true), Some("ready"), Some("error"), None, None, true), P::Error);
        for idle in ["standby", "complete", "cancelled"] {
            assert_eq!(d(Some(true), Some("ready"), Some(idle), Some(100.0), Some(0.0), true), P::Idle, "{idle}");
        }
        // start-up sequence never flashes OFF while the switch is on
        assert_eq!(d(Some(true), Some("startup"), Some("standby"), None, None, true), P::Idle);
        // everything unavailable (Moonraker restart) with the switch unknown: Unknown, not Off
        assert_eq!(d(None, None, None, None, None, false), P::Unknown);
        // switch on but Moonraker down: Unknown as well
        assert_eq!(d(Some(true), None, None, None, None, true), P::Idle);
        // switch-only configuration: on -> idle, off -> off
        assert_eq!(d(Some(true), None, None, None, None, true), P::Idle);
        assert_eq!(d(Some(false), None, None, None, None, true), P::Off);
        // no print-state sensor: infer printing from progress + time left, not from a finished file
        assert_eq!(d(Some(true), Some("ready"), None, Some(42.0), Some(1.5), true), P::Printing);
        assert_eq!(d(Some(true), Some("ready"), None, Some(100.0), Some(0.0), true), P::Idle);
        assert_eq!(d(Some(true), Some("ready"), None, Some(0.0), Some(0.0), true), P::Idle);
    }

    #[test]
    fn ha_url_normalization() {
        assert_eq!(normalize_ha_url("https://homeassistant.local"), "wss://homeassistant.local/api/websocket");
        assert_eq!(normalize_ha_url("https://homeassistant.local/"), "wss://homeassistant.local/api/websocket");
        assert_eq!(normalize_ha_url("http://192.168.1.10:8123"), "ws://192.168.1.10:8123/api/websocket");
        assert_eq!(normalize_ha_url("wss://ha.example/api/websocket"), "wss://ha.example/api/websocket");
        assert_eq!(normalize_ha_url("ha.example"), "wss://ha.example/api/websocket");
    }

    #[test]
    fn watts_grouping() {
        assert_eq!(fmt_watts(0.0), "0 W");
        assert_eq!(fmt_watts(999.4), "999 W");
        assert_eq!(fmt_watts(1234.5), "1 235 W");
        assert_eq!(fmt_watts(12345678.0), "12 345 678 W");
        assert_eq!(fmt_watts(-1500.0), "-1 500 W");
    }
}
