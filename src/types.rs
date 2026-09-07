//! Message types exchanged between the networking tasks (tokio thread) and the UI (Slint thread).

use std::collections::HashMap;

use serde_json::Value;

/// One Home Assistant entity, as merged from the `subscribe_entities` compressed stream.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EntityState {
    /// `"on"`, `"off"`, `"12.5"`, `"sunny"`, `"unavailable"`, ...
    pub state: String,
    pub attributes: HashMap<String, Value>,
}

impl EntityState {
    pub fn state_f64(&self) -> Option<f64> {
        self.state.trim().parse().ok()
    }

    pub fn attr_f64(&self, key: &str) -> Option<f64> {
        let v = self.attributes.get(key)?;
        v.as_f64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
    }

    pub fn attr_str(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).and_then(Value::as_str)
    }

    /// `false` for `unavailable` / `unknown` (and for an empty state).
    pub fn is_available(&self) -> bool {
        !matches!(self.state.as_str(), "" | "unavailable" | "unknown")
    }

    pub fn is_on(&self) -> bool {
        self.state == "on"
    }
}

/// From the Home Assistant task to the UI.
#[derive(Clone, Debug, PartialEq)]
pub enum HaEvent {
    /// Authenticated and subscribed. The initial snapshot follows as `StateChanged` events.
    Connected,
    /// Connection dropped. The task keeps reconnecting on its own.
    Disconnected { reason: String },
    /// Full merged state of an entity after an add or a change.
    StateChanged { entity_id: String, state: EntityState },
    /// The entity disappeared from the subscription.
    StateRemoved { entity_id: String },
    /// Outcome of a `ServiceCall` we sent (`request` echoes `ServiceCall::request`).
    CallResult { request: u64, success: bool, error: Option<String> },
}

/// From the UI to the Home Assistant task: one `call_service` command.
#[derive(Clone, Debug, PartialEq)]
pub struct ServiceCall {
    /// Correlation id chosen by the UI, echoed back in `HaEvent::CallResult`.
    pub request: u64,
    pub domain: String,
    pub service: String,
    /// JSON object, or `Value::Null` when there's no service data.
    pub service_data: Value,
    /// `target.entity_id`
    pub entity_ids: Vec<String>,
}

/// The "Bar display" light as seen by Home Assistant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LightState {
    pub on: bool,
    /// 1..=255. Kept at its last value while `on == false`.
    pub brightness: u8,
}

impl Default for LightState {
    fn default() -> Self {
        Self { on: true, brightness: 255 }
    }
}

impl LightState {
    pub fn with_brightness(mut self, b: u8) -> Self {
        self.brightness = b.max(1);
        self
    }
}

/// From the MQTT task to the UI.
#[derive(Clone, Debug, PartialEq)]
pub enum LightCommand {
    /// A command from Home Assistant (`<base_topic>/set`).
    Set { on: bool, brightness: Option<u8>, transition: Option<f32> },
    /// The retained state found on the broker at startup.
    Restore(LightState),
}
