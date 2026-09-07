//! MQTT discovery light ("Bar display") task, so the panel shows up in Home Assistant.
//!
//! One rumqttc `AsyncClient` + `EventLoop`, driven by a single `select!` loop that also watches
//! the `LightState` channel. Every publish/subscribe goes through `try_*`: the loop that drives
//! `poll()` must never wait on the bounded request channel, or a dead broker would deadlock it.
//! Nothing is queued while the broker is unreachable either, rumqttc moves the request channel
//! into an unbounded `pending` deque on every connection error, and a months-long outage must not
//! grow it. Every `ConnAck` republishes config, availability and the current state anyway.
//! Connection errors are retried with an exponential backoff (2 s doubling to 60 s) and the
//! outage is warned about once, then every five minutes, so a permanently refused CONNECT neither
//! hammers the broker nor goes silent in the journal.

use std::fmt;
use std::time::Duration;

use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event, LastWill, MqttOptions, Outgoing,
    Packet, QoS, SubscribeReasonCode,
};
use serde_json::{json, Value};
use tokio::sync::{mpsc::UnboundedSender, watch};
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use crate::config::Mqtt;
use crate::types::{LightCommand, LightState};

const SW_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Home Assistant's birth/will topic (`online` / `offline`). Independent of the discovery prefix.
const HA_STATUS_TOPIC: &str = "homeassistant/status";
/// rumqttc request channel capacity (publishes + subscribes waiting for the event loop).
const REQUEST_CAPACITY: usize = 64;
/// rumqttc panics on a zero keepalive. Anything below this is also pointless on a LAN.
const MIN_KEEPALIVE_S: u64 = 5;
/// The CONNECT packet carries the keepalive as a u16. rumqttc truncates anything above silently
/// (65536 goes on the wire as 0 = "no keepalive", 65537 as 1 s) while pinging at the full value.
const MAX_KEEPALIVE_S: u64 = u16::MAX as u64;
/// How long the first session waits for the broker's retained state before publishing ours.
const RESTORE_WINDOW: Duration = Duration::from_millis(1500);
/// Pause after the first connection error of an outage. Doubles on every further failure.
const RETRY_DELAY: Duration = Duration::from_secs(2);
/// Longest pause between two reconnect attempts.
const RETRY_DELAY_MAX: Duration = Duration::from_secs(60);
/// A still-failing outage is warned about again this often (the first failure warns at once).
const OUTAGE_REMINDER: Duration = Duration::from_secs(5 * 60);
/// Longest payload excerpt that ends up in a log line.
const LOG_PAYLOAD_CHARS: usize = 120;
/// Subscriptions issued after every ConnAck (see `Topics::subscriptions`).
const SUBSCRIPTIONS: usize = 3;

/// The MQTT topics derived from the configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Topics {
    /// `<discovery_prefix>/light/<unique_id>/config` (retained discovery payload).
    config: String,
    /// `<base_topic>/state` (retained, our own publishes only).
    state: String,
    /// `<base_topic>/set` (commands from Home Assistant, we only ever subscribe to it, a
    /// publish of ours would come straight back as a command).
    command: String,
    /// `<base_topic>/availability` (retained `online` / LWT `offline`).
    availability: String,
}

impl Topics {
    fn from_cfg(cfg: &Mqtt) -> Self {
        let base = cfg.base_topic.trim_end_matches('/');
        Self {
            config: format!(
                "{}/light/{}/config",
                cfg.discovery_prefix.trim_end_matches('/'),
                cfg.unique_id
            ),
            state: format!("{base}/state"),
            command: format!("{base}/set"),
            availability: format!("{base}/availability"),
        }
    }

    /// What every session subscribes to, in the order the subscribes are issued. The SubAcks
    /// are mapped back to this order (`SubscribePkids`), so it must not change casually.
    fn subscriptions(&self) -> [&str; SUBSCRIPTIONS] {
        [&self.command, &self.state, HA_STATUS_TOPIC]
    }
}

/// What the light loses when the subscription at `index` (in `Topics::subscriptions` order) is
/// refused by the broker.
fn refusal_consequence(index: usize) -> &'static str {
    match index {
        0 => "commands from Home Assistant will never arrive",
        1 => "the retained state cannot be restored at startup",
        2 => "a Home Assistant restart will not trigger a discovery republish",
        _ => "unknown subscription",
    }
}

/// Maps the SubAcks of one session back to the topics subscribed to, in issue order: the event
/// loop assigns the packet ids and reports them as `Outgoing::Subscribe(pkid)` in request order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SubscribePkids {
    pkids: [Option<u16>; SUBSCRIPTIONS],
    sent: usize,
}

impl SubscribePkids {
    fn reset(&mut self) {
        *self = Self::default();
    }

    /// Records the packet id of the next subscribe the event loop sent, `false` when more were
    /// sent than this session issued (nothing is recorded then).
    fn sent(&mut self, pkid: u16) -> bool {
        match self.pkids.get_mut(self.sent) {
            Some(slot) => {
                *slot = Some(pkid);
                self.sent += 1;
                true
            }
            None => false,
        }
    }

    /// Index (in `Topics::subscriptions` order) of the subscribe a SubAck with `pkid` answers.
    fn index_of(&self, pkid: u16) -> Option<usize> {
        self.pkids.iter().position(|p| *p == Some(pkid))
    }
}

/// `true` when a SubAck refuses the subscription (rumqttc itself never looks at the codes). A
/// SubAck without any return code is a protocol violation and counts as refused too.
fn subscription_refused(codes: &[SubscribeReasonCode]) -> bool {
    codes.is_empty() || codes.iter().any(|c| matches!(c, SubscribeReasonCode::Failure))
}

/// The keepalive actually used for a configured `keepalive_s`, within what rumqttc and the
/// CONNECT packet can carry.
fn clamp_keepalive(secs: u64) -> u64 {
    secs.clamp(MIN_KEEPALIVE_S, MAX_KEEPALIVE_S)
}

/// Pause before the attempt that follows `failures` consecutive failures: 2 s, 4 s, 8 s, ...
/// capped at `RETRY_DELAY_MAX`.
fn retry_delay(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(8);
    RETRY_DELAY.saturating_mul(1 << doublings).min(RETRY_DELAY_MAX)
}

/// How loud to log one more failed connection attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutageLog {
    /// First failure of this outage: warn.
    First,
    /// The outage has gone on for another `OUTAGE_REMINDER`: warn again.
    Reminder,
    /// Retrying quietly (debug).
    Quiet,
}

/// Reconnect pacing for one outage: exponential backoff plus a periodic reminder in the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Outage {
    /// Consecutive failed attempts since the last ConnAck.
    failures: u32,
    /// When the outage started (its first failure).
    since: Option<Instant>,
    /// When it was last warned about.
    warned: Option<Instant>,
}

impl Outage {
    const fn new() -> Self {
        Self { failures: 0, since: None, warned: None }
    }

    /// Records one more failure at `now`: the pause before the next attempt and how to log it.
    fn failed(&mut self, now: Instant) -> (Duration, OutageLog) {
        self.failures = self.failures.saturating_add(1);
        self.since.get_or_insert(now);
        let log = match self.warned {
            None => OutageLog::First,
            Some(at) if now.saturating_duration_since(at) >= OUTAGE_REMINDER => {
                OutageLog::Reminder
            }
            Some(_) => OutageLog::Quiet,
        };
        if log != OutageLog::Quiet {
            self.warned = Some(now);
        }
        (retry_delay(self.failures), log)
    }

    /// How long the outage has lasted.
    fn elapsed(&self, now: Instant) -> Duration {
        self.since.map_or(Duration::ZERO, |since| now.saturating_duration_since(since))
    }

    /// Back to "no outage" (on ConnAck).
    fn reset(&mut self) {
        *self = Self::new();
    }
}

/// A decoded `{"state":...,"brightness":...,"transition":...}` payload (commands and retained state
/// share the format).
#[derive(Clone, Copy, Debug, PartialEq)]
struct Command {
    on: bool,
    /// Already clamped to 1..=255.
    brightness: Option<u8>,
    /// Seconds, never negative.
    transition: Option<f32>,
}

impl Command {
    fn into_set(self) -> LightCommand {
        LightCommand::Set { on: self.on, brightness: self.brightness, transition: self.transition }
    }

    /// The light state a retained payload describes. A missing brightness keeps `fallback`'s.
    fn restore(self, fallback: LightState) -> LightState {
        LightState { on: self.on, brightness: self.brightness.unwrap_or(fallback.brightness).max(1) }
    }
}

/// Why a payload was ignored (worded for the warn line).
#[derive(Clone, Debug, PartialEq, Eq)]
enum CommandError {
    /// Delivered with the broker's retain flag: a leftover somebody published with `-r`, not a
    /// live command (Home Assistant never retains commands). It would be re-applied after every
    /// reconnect.
    Retained,
    NotJson(String),
    NotObject,
    BadState,
    BadBrightness,
    BadTransition,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retained => f.write_str("delivered with the retain flag (a stale retained command)"),
            Self::NotJson(e) => write!(f, "not JSON ({e})"),
            Self::NotObject => f.write_str("not a JSON object"),
            Self::BadState => f.write_str("\"state\" missing or not \"ON\"/\"OFF\""),
            Self::BadBrightness => f.write_str("\"brightness\" is not a non-negative integer"),
            Self::BadTransition => f.write_str("\"transition\" is not a number"),
        }
    }
}

/// 0 counts as 1, anything above 255 is 255.
fn clamp_brightness(raw: u64) -> u8 {
    // The clamp guarantees the value fits in a u8.
    raw.clamp(1, 255) as u8
}

/// Parses a command / state payload. Pure. Never panics on any input.
fn parse_command(payload: &[u8]) -> Result<Command, CommandError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|e| CommandError::NotJson(e.to_string()))?;
    let obj = value.as_object().ok_or(CommandError::NotObject)?;

    let on = match obj.get("state").and_then(Value::as_str) {
        Some(s) if s.eq_ignore_ascii_case("ON") => true,
        Some(s) if s.eq_ignore_ascii_case("OFF") => false,
        _ => return Err(CommandError::BadState),
    };

    let brightness = match obj.get("brightness") {
        None | Some(Value::Null) => None,
        Some(v) => Some(clamp_brightness(v.as_u64().ok_or(CommandError::BadBrightness)?)),
    };

    let transition = match obj.get("transition") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let secs = v.as_f64().ok_or(CommandError::BadTransition)?;
            // serde_json never yields NaN/inf. A negative transition is meaningless -> 0.
            Some(secs.max(0.0) as f32)
        }
    };

    Ok(Command { on, brightness, transition })
}

/// Decodes a delivery on the command topic: a retained one is refused before it's even
/// parsed (the retain flag, not the content, is what makes it stale).
fn decode_command(retain: bool, payload: &[u8]) -> Result<Command, CommandError> {
    if retain {
        return Err(CommandError::Retained);
    }
    parse_command(payload)
}

/// The retained discovery document for `<discovery_prefix>/light/<unique_id>/config`.
///
/// `name` is `null` on purpose: the light is the device's only entity, so it takes the device
/// name ("Bar display"). Repeating the device name as the entity name would make Home Assistant
/// log a "device name is equal to entity name" warning on every discovery.
fn discovery_payload(cfg: &Mqtt, topics: &Topics) -> Value {
    json!({
        "name": Value::Null,
        "unique_id": cfg.unique_id,
        "schema": "json",
        "state_topic": topics.state,
        "command_topic": topics.command,
        "availability_topic": topics.availability,
        "brightness": true,
        "supported_color_modes": ["brightness"],
        "device": {
            "identifiers": [cfg.unique_id],
            "name": cfg.device_name,
            "manufacturer": "barclock",
            "model": "Raspberry Pi Zero 2W + 1920x480 bar",
            "sw_version": SW_VERSION,
            "suggested_area": cfg.suggested_area,
        },
        "origin": {
            "name": "barclock",
            "sw_version": SW_VERSION,
        },
    })
}

/// `{"state":"ON"|"OFF","brightness":1..255}`
fn state_payload(state: LightState) -> String {
    json!({
        "state": if state.on { "ON" } else { "OFF" },
        "brightness": state.brightness.max(1),
    })
    .to_string()
}

/// Lossy, truncated payload text for log lines (payloads come from the network).
fn payload_preview(payload: &[u8]) -> String {
    let text = String::from_utf8_lossy(payload);
    let mut chars = text.chars();
    let mut out: String = chars.by_ref().take(LOG_PAYLOAD_CHARS).collect();
    if chars.next().is_some() {
        out.push('…');
    }
    out
}

/// Why `try_publish` / `try_subscribe` refused a request (rumqttc's own message is generic).
fn refusal_reason(topic: &str) -> &'static str {
    if rumqttc::valid_topic(topic) {
        "request queue full or event loop gone"
    } else {
        "invalid topic"
    }
}

/// Extra guidance for a refused CONNECT.
fn connect_hint(e: &ConnectionError) -> &'static str {
    match e {
        ConnectionError::ConnectionRefused(
            ConnectReturnCode::BadUserNamePassword | ConnectReturnCode::NotAuthorized,
        ) => " (check MQTT_USER / MQTT_PASSWORD in /etc/barclock.env)",
        ConnectionError::ConnectionRefused(ConnectReturnCode::BadClientId) => {
            " (the broker rejects mqtt.client_id)"
        }
        _ => "",
    }
}

fn publish(client: &AsyncClient, topic: &str, payload: impl Into<Vec<u8>>, retain: bool) {
    match client.try_publish(topic, QoS::AtLeastOnce, retain, payload) {
        Ok(()) => trace!(topic, retain, "mqtt: publish queued"),
        Err(_) => warn!(topic, "mqtt: publish dropped: {}", refusal_reason(topic)),
    }
}

fn subscribe(client: &AsyncClient, topic: &str) {
    match client.try_subscribe(topic, QoS::AtLeastOnce) {
        Ok(()) => trace!(topic, "mqtt: subscribe queued"),
        Err(_) => warn!(topic, "mqtt: subscribe dropped: {}", refusal_reason(topic)),
    }
}

fn publish_state(client: &AsyncClient, topics: &Topics, state: LightState) {
    let payload = state_payload(state);
    debug!(topic = %topics.state, %payload, "mqtt: publishing state");
    publish(client, &topics.state, payload, true);
}

/// Sleeps until `deadline`. Never completes when there's none (the branch is also gated).
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

pub async fn run(
    cfg: Mqtt,
    creds: Option<(String, String)>,
    commands: UnboundedSender<LightCommand>,
    mut state: watch::Receiver<LightState>,
) {
    let topics = Topics::from_cfg(&cfg);
    let keepalive_s = clamp_keepalive(cfg.keepalive_s);
    if keepalive_s != cfg.keepalive_s {
        warn!(
            "mqtt.keepalive_s = {} is outside {MIN_KEEPALIVE_S}..={MAX_KEEPALIVE_S}, using {keepalive_s}",
            cfg.keepalive_s
        );
    }

    let mut options = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
    options.set_keep_alive(Duration::from_secs(keepalive_s));
    options.set_last_will(LastWill::new(
        topics.availability.clone(),
        "offline",
        QoS::AtLeastOnce,
        true,
    ));
    let user = creds
        .map(|(user, password)| {
            options.set_credentials(user.clone(), password);
            user
        })
        .unwrap_or_else(|| "<anonymous>".to_string());
    info!(
        host = %cfg.host,
        port = cfg.port,
        client_id = %cfg.client_id,
        user = %user,
        keepalive_s,
        "mqtt: starting"
    );
    debug!(?topics, "mqtt: topics");

    let (client, mut eventloop) = AsyncClient::new(options, REQUEST_CAPACITY);
    let discovery = discovery_payload(&cfg, &topics).to_string();

    // Connection bookkeeping: `connected` gates every publish, `outage` paces the retries and the
    // outage warns, `retry_at` holds `poll()` back until the backoff has elapsed, `sessions`
    // distinguishes connect from reconnect in the log.
    let mut connected = false;
    let mut outage = Outage::new();
    let mut retry_at: Option<Instant> = None;
    let mut sessions: u64 = 0;
    // Per-session: which subscribe each SubAck answers, and one warn for a retained command.
    let mut sub_pkids = SubscribePkids::default();
    let mut retained_command_warned = false;
    // Restore window: open while `restore_deadline` is Some, `restore_done` once it has closed
    // (message or timeout) so later sessions skip it. A drop during the window reopens it on
    // the next ConnAck, the retained value is still whatever a previous process life left.
    let mut restore_deadline: Option<Instant> = None;
    let mut restore_done = false;
    // The UI keeps the watch sender for the life of the process. If it ever goes away the
    // branch is disabled instead of spinning on the error.
    let mut state_closed = false;

    loop {
        tokio::select! {
            event = eventloop.poll(), if retry_at.is_none() => match event {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    sessions += 1;
                    connected = true;
                    outage.reset();
                    sub_pkids.reset();
                    retained_command_warned = false;
                    if sessions == 1 {
                        info!(session_present = ack.session_present, "mqtt: connected to {}:{}", cfg.host, cfg.port);
                    } else {
                        info!(session = sessions, "mqtt: reconnected to {}:{}", cfg.host, cfg.port);
                    }
                    // (1) discovery + availability, (2) subscriptions (in `Topics::subscriptions` order).
                    publish(&client, &topics.config, discovery.clone(), true);
                    publish(&client, &topics.availability, "online", true);
                    for topic in topics.subscriptions() {
                        subscribe(&client, topic);
                    }
                    if restore_done {
                        // (4), reconnects skip the restore window.
                        publish_state(&client, &topics, *state.borrow_and_update());
                    } else {
                        // (3) first session: give the broker's retained state a chance to arrive.
                        restore_deadline = Some(Instant::now() + RESTORE_WINDOW);
                        debug!("mqtt: restore window open ({:?})", RESTORE_WINDOW);
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(ack))) => {
                    let subscribed = topics.subscriptions();
                    let index = sub_pkids.index_of(ack.pkid);
                    let topic = index.and_then(|i| subscribed.get(i).copied()).unwrap_or("<unknown>");
                    if subscription_refused(&ack.return_codes) {
                        warn!(
                            pkid = ack.pkid,
                            codes = ?ack.return_codes,
                            "mqtt: the broker refused the subscription to {topic}: {}; check the broker ACL for user {user}",
                            refusal_consequence(index.unwrap_or(usize::MAX))
                        );
                    } else {
                        trace!(pkid = ack.pkid, codes = ?ack.return_codes, "mqtt: subscribed to {topic}");
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    debug!(topic = %p.topic, retain = p.retain, payload = %payload_preview(&p.payload), "mqtt: message");
                    if p.topic == topics.command {
                        match decode_command(p.retain, &p.payload) {
                            Ok(cmd) => {
                                info!(on = cmd.on, brightness = ?cmd.brightness, transition = ?cmd.transition, "mqtt: light command");
                                if commands.send(cmd.into_set()).is_err() {
                                    warn!("mqtt: command dropped, the UI side is gone");
                                }
                            }
                            Err(CommandError::Retained) => {
                                // Never cleared from here: a publish of ours to the command
                                // topic would come straight back as a command.
                                if retained_command_warned {
                                    debug!(payload = %payload_preview(&p.payload), "mqtt: another retained command ignored");
                                } else {
                                    retained_command_warned = true;
                                    warn!(
                                        topic = %p.topic,
                                        payload = %payload_preview(&p.payload),
                                        "mqtt: ignoring a retained command (stale, it would be re-applied after every reconnect); clear it with an empty retained publish: mosquitto_pub -r -n -t '{}'",
                                        p.topic
                                    );
                                }
                            }
                            Err(e) => warn!(
                                topic = %p.topic,
                                payload = %payload_preview(&p.payload),
                                "mqtt: ignoring command: {e}"
                            ),
                        }
                    } else if p.topic == topics.state {
                        if restore_deadline.is_none() {
                            // (5) after the window the topic only echoes our own publishes.
                            trace!("mqtt: state echo ignored");
                        } else if !p.retain {
                            debug!("mqtt: live message on the state topic during the restore window, ignored");
                        } else {
                            let current = *state.borrow_and_update();
                            // (4) publishes what the process actually runs with from now on:
                            // the restored state (the UI's watch echo is then an identical
                            // duplicate), or ours when the retained payload is unusable.
                            let published = match parse_command(&p.payload) {
                                Ok(cmd) => {
                                    let restored = cmd.restore(current);
                                    info!(?restored, "mqtt: restoring the retained light state");
                                    if commands.send(LightCommand::Restore(restored)).is_ok() {
                                        restored
                                    } else {
                                        warn!("mqtt: restore dropped, the UI side is gone");
                                        current
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        payload = %payload_preview(&p.payload),
                                        "mqtt: retained state is unreadable ({e}), keeping ours"
                                    );
                                    current
                                }
                            };
                            restore_deadline = None;
                            restore_done = true;
                            publish_state(&client, &topics, published);
                        }
                    } else if p.topic == HA_STATUS_TOPIC {
                        let status = String::from_utf8_lossy(&p.payload);
                        if status.trim() == "online" {
                            info!("mqtt: Home Assistant is online, republishing discovery");
                            publish(&client, &topics.config, discovery.clone(), true);
                            publish(&client, &topics.availability, "online", true);
                            if restore_deadline.is_none() {
                                publish_state(&client, &topics, *state.borrow_and_update());
                            }
                        } else {
                            debug!(status = %status.trim(), "mqtt: Home Assistant status");
                        }
                    } else {
                        debug!(topic = %p.topic, "mqtt: message on an unexpected topic ignored");
                    }
                }
                Ok(Event::Incoming(packet)) => trace!(?packet, "mqtt: incoming"),
                Ok(Event::Outgoing(Outgoing::Subscribe(pkid))) => {
                    if !sub_pkids.sent(pkid) {
                        debug!(pkid, "mqtt: more subscribes sent than issued this session");
                    }
                    trace!(pkid, "mqtt: outgoing subscribe");
                }
                Ok(Event::Outgoing(packet)) => trace!(?packet, "mqtt: outgoing"),
                Err(e) => {
                    // A window that was open dies with the connection, `restore_done` stays
                    // false so the next ConnAck opens a fresh one.
                    restore_deadline = None;
                    let now = Instant::now();
                    let (delay, log) = outage.failed(now);
                    let hint = connect_hint(&e);
                    match log {
                        OutageLog::First if connected => {
                            warn!("mqtt: connection lost: {e}{hint}; reconnecting in {delay:?}");
                        }
                        OutageLog::First => warn!(
                            "mqtt: cannot connect to {}:{}: {e}{hint}; retrying in {delay:?}",
                            cfg.host, cfg.port
                        ),
                        OutageLog::Reminder => warn!(
                            "mqtt: still cannot connect to {}:{} after {} attempts ({} s): {e}{hint}; retrying every {delay:?}",
                            cfg.host, cfg.port, outage.failures, outage.elapsed(now).as_secs()
                        ),
                        OutageLog::Quiet => debug!(
                            attempt = outage.failures,
                            "mqtt: reconnect failed: {e}; retrying in {delay:?}"
                        ),
                    }
                    connected = false;
                    // `poll()` stays parked until then. The loop keeps serving the other branches.
                    retry_at = Some(now + delay);
                }
            },
            _ = wait_until(retry_at), if retry_at.is_some() => {
                retry_at = None;
                debug!(attempt = outage.failures.saturating_add(1), "mqtt: reconnecting");
            },
            changed = state.changed(), if !state_closed => match changed {
                Ok(()) => {
                    let current = *state.borrow_and_update();
                    if restore_deadline.is_some() {
                        debug!(?current, "mqtt: state changed during the restore window, publish deferred");
                    } else if connected {
                        publish_state(&client, &topics, current);
                    } else {
                        debug!(?current, "mqtt: state changed while disconnected, published on reconnect");
                    }
                }
                Err(_) => {
                    state_closed = true;
                    warn!("mqtt: the light state channel is closed, state updates stop here");
                }
            },
            _ = wait_until(restore_deadline), if restore_deadline.is_some() => {
                info!("mqtt: no retained state on the broker within {:?}, publishing ours", RESTORE_WINDOW);
                restore_deadline = None;
                restore_done = true;
                // (4)
                publish_state(&client, &topics, *state.borrow_and_update());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Mqtt {
        Mqtt::default()
    }

    fn parse(s: &str) -> Result<Command, CommandError> {
        parse_command(s.as_bytes())
    }

    #[test]
    fn topics_follow_the_config() {
        let t = Topics::from_cfg(&cfg());
        assert_eq!(t.config, "homeassistant/light/barclock_display/config");
        assert_eq!(t.state, "barclock/display/state");
        assert_eq!(t.command, "barclock/display/set");
        assert_eq!(t.availability, "barclock/display/availability");

        let custom = Mqtt {
            discovery_prefix: "ha/".into(),
            base_topic: "bar/clock/".into(),
            unique_id: "bar_1".into(),
            ..Mqtt::default()
        };
        let t = Topics::from_cfg(&custom);
        assert_eq!(t.config, "ha/light/bar_1/config");
        assert_eq!(t.state, "bar/clock/state");
        assert_eq!(t.command, "bar/clock/set");
        assert_eq!(t.availability, "bar/clock/availability");
    }

    #[test]
    fn subscriptions_are_issued_in_a_fixed_order() {
        let t = Topics::from_cfg(&cfg());
        assert_eq!(
            t.subscriptions(),
            ["barclock/display/set", "barclock/display/state", "homeassistant/status"]
        );
        // Every slot has a consequence text for the refusal warn. Out of range never panics.
        for i in 0..SUBSCRIPTIONS {
            assert_ne!(refusal_consequence(i), "unknown subscription");
        }
        assert_eq!(refusal_consequence(SUBSCRIPTIONS), "unknown subscription");
        assert_eq!(refusal_consequence(usize::MAX), "unknown subscription");
    }

    #[test]
    fn suback_pkids_map_back_to_the_issue_order() {
        let mut subs = SubscribePkids::default();
        assert_eq!(subs.index_of(7), None);
        assert!(subs.sent(7));
        assert!(subs.sent(8));
        assert!(subs.sent(9));
        // A fourth subscribe was never issued: refused, nothing overwritten.
        assert!(!subs.sent(10));
        assert_eq!(subs.index_of(7), Some(0));
        assert_eq!(subs.index_of(8), Some(1));
        assert_eq!(subs.index_of(9), Some(2));
        assert_eq!(subs.index_of(10), None);
        assert_eq!(subs.index_of(0), None);
        // A new session starts from scratch (the broker may reuse packet ids).
        subs.reset();
        assert_eq!(subs.index_of(8), None);
        assert!(subs.sent(8));
        assert_eq!(subs.index_of(8), Some(0));
    }

    #[test]
    fn subscription_refusal_is_detected() {
        use SubscribeReasonCode::{Failure, Success};
        assert!(!subscription_refused(&[Success(QoS::AtLeastOnce)]));
        assert!(!subscription_refused(&[Success(QoS::AtMostOnce)]));
        assert!(subscription_refused(&[Failure]));
        assert!(subscription_refused(&[Success(QoS::AtLeastOnce), Failure]));
        assert!(subscription_refused(&[]));
    }

    #[test]
    fn keepalive_is_clamped_to_the_wire_range() {
        assert_eq!(clamp_keepalive(0), MIN_KEEPALIVE_S);
        assert_eq!(clamp_keepalive(4), MIN_KEEPALIVE_S);
        assert_eq!(clamp_keepalive(5), 5);
        assert_eq!(clamp_keepalive(30), 30);
        assert_eq!(clamp_keepalive(65535), 65535);
        // 65536 would go on the wire as 0 ("no keepalive"), 65537 as 1 s.
        assert_eq!(clamp_keepalive(65536), 65535);
        assert_eq!(clamp_keepalive(65537), 65535);
        assert_eq!(clamp_keepalive(u64::MAX), 65535);
    }

    #[test]
    fn retry_delay_doubles_up_to_the_cap() {
        let s = Duration::from_secs;
        assert_eq!(retry_delay(0), s(2));
        assert_eq!(retry_delay(1), s(2));
        assert_eq!(retry_delay(2), s(4));
        assert_eq!(retry_delay(3), s(8));
        assert_eq!(retry_delay(4), s(16));
        assert_eq!(retry_delay(5), s(32));
        assert_eq!(retry_delay(6), s(60));
        assert_eq!(retry_delay(7), s(60));
        assert_eq!(retry_delay(1_000), s(60));
        assert_eq!(retry_delay(u32::MAX), s(60));
    }

    #[test]
    fn outage_warns_first_then_reminds_every_five_minutes() {
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut outage = Outage::new();
        assert_eq!(outage.elapsed(t0), Duration::ZERO);

        assert_eq!(outage.failed(t0), (s(2), OutageLog::First));
        assert_eq!(outage.failed(t0 + s(2)), (s(4), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(6)), (s(8), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(14)), (s(16), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(30)), (s(32), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(62)), (s(60), OutageLog::Quiet));
        // Just short of the reminder period since the first warn: still quiet.
        assert_eq!(outage.failed(t0 + s(299)), (s(60), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(300)), (s(60), OutageLog::Reminder));
        assert_eq!(outage.failures, 8);
        assert_eq!(outage.elapsed(t0 + s(300)), s(300));
        // The reminder period restarts from the reminder, not from the first warn.
        assert_eq!(outage.failed(t0 + s(360)), (s(60), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(599)), (s(60), OutageLog::Quiet));
        assert_eq!(outage.failed(t0 + s(600)), (s(60), OutageLog::Reminder));
        // Time never going backwards isn't assumed either.
        assert_eq!(outage.failed(t0), (s(60), OutageLog::Quiet));

        // ConnAck: the next outage starts over with a warn and a 2 s retry.
        outage.reset();
        assert_eq!(outage, Outage::new());
        assert_eq!(outage.failed(t0 + s(900)), (s(2), OutageLog::First));
        assert_eq!(outage.elapsed(t0 + s(901)), s(1));
    }

    #[test]
    fn retained_commands_are_refused_before_parsing() {
        let live = decode_command(false, br#"{"state":"ON","brightness":5}"#).unwrap();
        assert_eq!(live, Command { on: true, brightness: Some(5), transition: None });
        assert_eq!(
            decode_command(true, br#"{"state":"ON","brightness":5}"#),
            Err(CommandError::Retained)
        );
        // The retain flag alone decides. The content isn't even looked at.
        assert_eq!(decode_command(true, b"garbage"), Err(CommandError::Retained));
        assert_eq!(decode_command(true, b""), Err(CommandError::Retained));
        assert!(matches!(decode_command(false, b"garbage"), Err(CommandError::NotJson(_))));
        assert!(!CommandError::Retained.to_string().is_empty());
    }

    #[test]
    fn command_on_with_brightness() {
        let cmd = parse(r#"{"state":"ON","brightness":128}"#).unwrap();
        assert_eq!(cmd, Command { on: true, brightness: Some(128), transition: None });
        assert_eq!(
            cmd.into_set(),
            LightCommand::Set { on: true, brightness: Some(128), transition: None }
        );
    }

    #[test]
    fn command_on_without_brightness() {
        let cmd = parse(r#"{"state":"ON"}"#).unwrap();
        assert_eq!(cmd, Command { on: true, brightness: None, transition: None });
        // A null brightness is the same as an absent one.
        assert_eq!(parse(r#"{"state":"ON","brightness":null}"#).unwrap(), cmd);
    }

    #[test]
    fn command_off() {
        let cmd = parse(r#"{"state":"OFF"}"#).unwrap();
        assert_eq!(cmd, Command { on: false, brightness: None, transition: None });
        assert_eq!(parse(r#"{"state":"OFF","brightness":10}"#).unwrap().brightness, Some(10));
    }

    #[test]
    fn command_state_is_case_insensitive_and_extra_keys_are_ignored() {
        assert!(parse(r#"{"state":"on","color_mode":"brightness","effect":"none"}"#).unwrap().on);
        assert!(!parse(r#"{"state":"Off"}"#).unwrap().on);
    }

    #[test]
    fn command_brightness_zero_counts_as_one() {
        assert_eq!(parse(r#"{"state":"ON","brightness":0}"#).unwrap().brightness, Some(1));
        assert_eq!(clamp_brightness(0), 1);
        assert_eq!(clamp_brightness(1), 1);
        assert_eq!(clamp_brightness(255), 255);
    }

    #[test]
    fn command_brightness_above_255_is_clamped() {
        assert_eq!(parse(r#"{"state":"ON","brightness":999}"#).unwrap().brightness, Some(255));
        assert_eq!(
            parse(r#"{"state":"ON","brightness":18446744073709551615}"#).unwrap().brightness,
            Some(255)
        );
        assert_eq!(clamp_brightness(u64::MAX), 255);
    }

    #[test]
    fn command_transition() {
        let cmd = parse(r#"{"state":"ON","brightness":200,"transition":2.5}"#).unwrap();
        assert_eq!(cmd.transition, Some(2.5));
        // Integers are numbers too, negatives are floored at zero.
        assert_eq!(parse(r#"{"state":"OFF","transition":3}"#).unwrap().transition, Some(3.0));
        assert_eq!(parse(r#"{"state":"OFF","transition":-1}"#).unwrap().transition, Some(0.0));
        assert_eq!(parse(r#"{"state":"OFF","transition":null}"#).unwrap().transition, None);
    }

    #[test]
    fn command_garbage_is_rejected() {
        assert!(matches!(parse(""), Err(CommandError::NotJson(_))));
        assert!(matches!(parse("hello"), Err(CommandError::NotJson(_))));
        assert!(matches!(parse(r#"{"state":"ON""#), Err(CommandError::NotJson(_))));
        assert!(matches!(parse_command(&[0xff, 0xfe, 0x00]), Err(CommandError::NotJson(_))));
        assert_eq!(parse("[1,2,3]"), Err(CommandError::NotObject));
        assert_eq!(parse("42"), Err(CommandError::NotObject));
        assert_eq!(parse("{}"), Err(CommandError::BadState));
        assert_eq!(parse(r#"{"brightness":10}"#), Err(CommandError::BadState));
        assert_eq!(parse(r#"{"state":"DIM"}"#), Err(CommandError::BadState));
    }

    #[test]
    fn command_wrong_types_are_rejected() {
        assert_eq!(parse(r#"{"state":true}"#), Err(CommandError::BadState));
        assert_eq!(parse(r#"{"state":1}"#), Err(CommandError::BadState));
        assert_eq!(parse(r#"{"state":"ON","brightness":"128"}"#), Err(CommandError::BadBrightness));
        assert_eq!(parse(r#"{"state":"ON","brightness":-1}"#), Err(CommandError::BadBrightness));
        assert_eq!(parse(r#"{"state":"ON","brightness":12.5}"#), Err(CommandError::BadBrightness));
        assert_eq!(parse(r#"{"state":"ON","brightness":[1]}"#), Err(CommandError::BadBrightness));
        assert_eq!(parse(r#"{"state":"ON","transition":"fast"}"#), Err(CommandError::BadTransition));
        assert_eq!(parse(r#"{"state":"ON","transition":{}}"#), Err(CommandError::BadTransition));
        // Every error has a human message for the warn line.
        for e in [
            CommandError::Retained,
            CommandError::NotJson("x".into()),
            CommandError::NotObject,
            CommandError::BadState,
            CommandError::BadBrightness,
            CommandError::BadTransition,
        ] {
            assert!(!e.to_string().is_empty());
        }
    }

    #[test]
    fn discovery_payload_has_every_key() {
        let cfg = cfg();
        let topics = Topics::from_cfg(&cfg);
        let doc = discovery_payload(&cfg, &topics);
        let text = doc.to_string();
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed, doc);

        let obj = parsed.as_object().unwrap();
        assert!(obj.contains_key("name"));
        assert_eq!(obj["name"], Value::Null);
        assert_eq!(obj["unique_id"], "barclock_display");
        assert_eq!(obj["schema"], "json");
        assert_eq!(obj["state_topic"], "barclock/display/state");
        assert_eq!(obj["command_topic"], "barclock/display/set");
        assert_eq!(obj["availability_topic"], "barclock/display/availability");
        assert_eq!(obj["brightness"], true);
        assert_eq!(obj["supported_color_modes"], json!(["brightness"]));
        // No `retain`: Home Assistant must never retain its commands (they would be ignored).
        assert!(!obj.contains_key("retain"));

        let device = obj["device"].as_object().unwrap();
        assert_eq!(device["identifiers"], json!(["barclock_display"]));
        assert_eq!(device["name"], "Bar display");
        assert_eq!(device["manufacturer"], "barclock");
        assert_eq!(device["model"], "Raspberry Pi Zero 2W + 1920x480 bar");
        assert_eq!(device["sw_version"], SW_VERSION);
        assert_eq!(device["suggested_area"], "Bureau Lulu");
        assert_eq!(device.len(), 6);

        let origin = obj["origin"].as_object().unwrap();
        assert_eq!(origin["name"], "barclock");
        assert_eq!(origin["sw_version"], SW_VERSION);
        assert_eq!(origin.len(), 2);

        assert_eq!(obj.len(), 10);
        assert!(!SW_VERSION.is_empty());
    }

    #[test]
    fn state_payload_on_and_off() {
        let on: Value =
            serde_json::from_str(&state_payload(LightState { on: true, brightness: 200 })).unwrap();
        assert_eq!(on, json!({"state": "ON", "brightness": 200}));

        let off: Value =
            serde_json::from_str(&state_payload(LightState { on: false, brightness: 37 })).unwrap();
        assert_eq!(off, json!({"state": "OFF", "brightness": 37}));

        // A brightness of 0 never goes on the wire.
        let zero: Value =
            serde_json::from_str(&state_payload(LightState { on: true, brightness: 0 })).unwrap();
        assert_eq!(zero["brightness"], 1);

        // Our own state payload parses back as a restore.
        let restored = parse(&state_payload(LightState { on: false, brightness: 37 }))
            .unwrap()
            .restore(LightState::default());
        assert_eq!(restored, LightState { on: false, brightness: 37 });
    }

    #[test]
    fn restore_keeps_the_fallback_brightness_when_missing() {
        let fallback = LightState { on: true, brightness: 90 };
        let restored = parse(r#"{"state":"OFF"}"#).unwrap().restore(fallback);
        assert_eq!(restored, LightState { on: false, brightness: 90 });
        let restored = parse(r#"{"state":"ON","brightness":0}"#).unwrap().restore(fallback);
        assert_eq!(restored, LightState { on: true, brightness: 1 });
    }

    #[test]
    fn restored_state_is_what_goes_on_the_wire() {
        // Retained OFF/37 on the broker, local default ON/255: the state published at step (4)
        // must be the restored one, never the pre-restore local value.
        let current = LightState::default();
        let retained = state_payload(LightState { on: false, brightness: 37 });
        let restored = parse(&retained).unwrap().restore(current);
        assert_ne!(restored, current);
        let wire: Value = serde_json::from_str(&state_payload(restored)).unwrap();
        assert_eq!(wire, json!({"state": "OFF", "brightness": 37}));
    }

    #[test]
    fn payload_preview_is_bounded_and_lossy() {
        assert_eq!(payload_preview(b"abc"), "abc");
        assert_eq!(payload_preview(&[0xff, b'x']), "\u{fffd}x");
        let long = "y".repeat(LOG_PAYLOAD_CHARS * 3);
        let preview = payload_preview(long.as_bytes());
        assert_eq!(preview.chars().count(), LOG_PAYLOAD_CHARS + 1);
        assert!(preview.ends_with('…'));
    }
}
