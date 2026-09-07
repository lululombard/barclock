//! Home Assistant WebSocket client task.
//!
//! One connection at a time: TCP -> (rustls) -> tungstenite handshake -> `auth` ->
//! `subscribe_entities` -> a single `select!` loop over the socket, the UI's service calls, the
//! ping interval and the pong / handshake deadline. Every drop returns to the outer loop, which
//! reports it to the UI once and sleeps the backoff (still answering service calls with
//! "not connected" meanwhile). Frame building, result parsing and the entity merge are pure
//! functions over `serde_json::Value`, unit-tested below without a socket.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use futures_util::{SinkExt, StreamExt};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use serde_json::{json, Map, Value};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{client_async_with_config, WebSocketStream};
use tracing::{debug, error, info, trace, warn};

use crate::config::HomeAssistant;
use crate::types::{EntityState, HaEvent, ServiceCall};

/// Message id of the one `subscribe_entities` per connection. The per-connection counter starts
/// here and every call_service / ping takes the next value (HA answers `id_reuse` otherwise).
const SUBSCRIBE_ID: u64 = 1;
/// Budget for TCP connect + TLS handshake + WebSocket upgrade.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// HA answers `auth` and `subscribe_entities` immediately. Longer than this and the link is dead.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// Contract: no pong within 10 s -> drop and reconnect.
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// How long we wait for the close handshake before abandoning a dropped socket.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// The snapshot for a dozen entities is a few KiB. Anything near this is a bug on the other side
/// and must not be buffered on a 512 MB device (tungstenite would accept 64 MiB by default).
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// Safety net against a UI that fires calls faster than HA answers them.
const MAX_PENDING_CALLS: usize = 256;
/// Frame bodies are cut here in trace logs so the journal stays readable.
const LOG_BODY_CHARS: usize = 512;
/// A session that stayed `Live` this long counts as stable and resets the backoff. One that died
/// sooner keeps doubling, so a deterministic failure right after `Connected` (oversized
/// snapshot, HA in a crash loop) never turns into a 1 Hz TLS-handshake storm.
const STABLE_SESSION: Duration = Duration::from_secs(60);
/// Wait after `auth_invalid`. The token can't change without a restart, so retrying at
/// `backoff_max` only feeds Home Assistant's failed-login counter (`http.ip_ban_enabled`) until
/// the Pi's IP lands in `ip_bans.yaml`. Long, and never doubled.
const AUTH_RETRY: Duration = Duration::from_secs(15 * 60);
/// `Config::validate` already enforces these ranges, `run()` clamps again so that a caller
/// bypassing it can never build an `Instant + Duration` that overflows.
const MAX_CONFIG_SECS: u64 = 3600;

/// Never returns. Reconnects forever with exponential backoff. Never panics.
pub async fn run(
    cfg: HomeAssistant,
    token: String,
    entity_ids: Vec<String>,
    events: UnboundedSender<HaEvent>,
    calls: UnboundedReceiver<ServiceCall>,
) {
    let mut backoff = Backoff::new(
        Duration::from_secs(cfg.backoff_min_s.clamp(1, MAX_CONFIG_SECS)),
        Duration::from_secs(cfg.backoff_max_s.clamp(1, MAX_CONFIG_SECS)),
    );
    let ping_every = Duration::from_secs(cfg.ping_interval_s.clamp(5, MAX_CONFIG_SECS));
    // An empty `entity_ids` makes HA stream every entity it has, refuse rather than flood a Pi.
    let entity_ids: Vec<String> = entity_ids.into_iter().filter(|id| !id.trim().is_empty()).collect();
    if entity_ids.is_empty() {
        error!("no Home Assistant entities configured ([entities] in barclock.toml); not connecting");
        std::future::pending::<()>().await;
    }
    let subscribed: HashSet<String> = entity_ids.iter().cloned().collect();
    let tls = match tls_config() {
        Ok(c) => Some(c),
        Err(e) => {
            error!("TLS setup failed ({e:#}); wss:// connections cannot work");
            None
        }
    };
    let mut calls = Some(calls);
    let mut last_failure: Option<String> = None;

    loop {
        debug!("connecting to {}", cfg.url);
        let mut session = Session::new(&token, &entity_ids, &subscribed, &events);
        let reason = connect_and_serve(&cfg.url, tls.as_ref(), &mut session, &mut calls, ping_every).await;
        session.fail_pending("disconnected");
        let end = session.end();
        let delay = backoff.delay_after(end);
        match end {
            SessionEnd::Live(_) => {
                warn!("Home Assistant connection lost: {reason}; reconnecting in {}s", delay.as_secs());
                let _ = events.send(HaEvent::Disconnected { reason });
                last_failure = None;
            }
            SessionEnd::AuthRejected => {
                // The error line naming HA_TOKEN was logged when the frame arrived.
                warn!(
                    "Home Assistant rejected HA_TOKEN: next attempt in {} min (every failed login \
                     counts toward Home Assistant's IP ban)",
                    delay.as_secs() / 60
                );
                last_failure = Some(reason);
            }
            SessionEnd::NeverLive => {
                // With HA down for hours this fires every `backoff_max`: warn once per distinct reason.
                let line = format!("Home Assistant unreachable: {reason}; retrying in {}s", delay.as_secs());
                if last_failure.as_deref() == Some(reason.as_str()) {
                    debug!("{line}");
                } else {
                    warn!("{line}");
                }
                last_failure = Some(reason);
            }
        }
        sleep_rejecting(delay, &mut calls, &events).await;
    }
}

// ---------------------------------------------------------------------------------------------
// Reconnect policy (pure, unit-tested below)
// ---------------------------------------------------------------------------------------------

/// How a session ended, as far as the reconnect delay is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionEnd {
    /// Never reached `Live`: connect, handshake or subscribe failed.
    NeverLive,
    /// Was `Live` for this long before the drop.
    Live(Duration),
    /// HA answered `auth_invalid`: the token is wrong until the process restarts.
    AuthRejected,
}

/// Doubling delay from `min` to `max`. It resets to `min` only after a session stayed `Live` for
/// `STABLE_SESSION`. A session that dies sooner keeps the climb going. `auth_invalid` waits
/// `AUTH_RETRY` and leaves the climb where it was.
struct Backoff {
    min: Duration,
    max: Duration,
    /// Delay of the next non-stable failure.
    next: Duration,
}

impl Backoff {
    fn new(min: Duration, max: Duration) -> Self {
        let max = max.max(min);
        Self { min, max, next: min }
    }

    /// Delay to sleep before the next attempt, given how the last session ended.
    fn delay_after(&mut self, end: SessionEnd) -> Duration {
        match end {
            SessionEnd::AuthRejected => AUTH_RETRY,
            SessionEnd::Live(lived) if lived >= STABLE_SESSION => {
                self.next = self.min;
                self.step()
            }
            SessionEnd::Live(_) | SessionEnd::NeverLive => self.step(),
        }
    }

    fn step(&mut self) -> Duration {
        let delay = self.next;
        self.next = delay.saturating_mul(2).min(self.max);
        delay
    }
}

// ---------------------------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------------------------

/// The two transports share `serve()` through its generic `S`.
enum Ws {
    Plain(WebSocketStream<TcpStream>),
    Tls(WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// One connection attempt from TCP to the final drop. Returns the human reason for the drop.
async fn connect_and_serve(
    url: &str,
    tls: Option<&Arc<ClientConfig>>,
    session: &mut Session<'_>,
    calls: &mut Option<UnboundedReceiver<ServiceCall>>,
    ping_every: Duration,
) -> String {
    let mut attempt = std::pin::pin!(tokio::time::timeout(CONNECT_TIMEOUT, connect(url, tls)));
    let outcome = loop {
        tokio::select! {
            res = &mut attempt => break res,
            call = next_call(calls) => reject(session.events, &call, "not connected"),
        }
    };
    let ws = match outcome {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => return format!("{e:#}"),
        Err(_) => return format!("connect timed out after {}s", CONNECT_TIMEOUT.as_secs()),
    };
    match ws {
        Ws::Plain(ws) => serve(ws, session, calls, ping_every).await,
        Ws::Tls(ws) => serve(ws, session, calls, ping_every).await,
    }
}

fn tls_config() -> anyhow::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .context("TLS protocol versions")?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

/// TCP -> optional TLS -> WebSocket upgrade. `ws://` skips the TLS layer entirely.
async fn connect(url: &str, tls: Option<&Arc<ClientConfig>>) -> anyhow::Result<Ws> {
    let request = url.into_client_request().context("invalid homeassistant.url")?;
    let uri = request.uri();
    let secure = match uri.scheme_str().map(str::to_ascii_lowercase).as_deref() {
        Some("wss") => true,
        Some("ws") => false,
        other => bail!("homeassistant.url scheme {other:?} is not ws:// or wss://"),
    };
    let host = uri
        .host()
        .map(|h| h.trim_start_matches('[').trim_end_matches(']').to_string())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| anyhow!("homeassistant.url has no host"))?;
    let port = uri.port_u16().unwrap_or(if secure { 443 } else { 80 });

    let tcp = TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("TCP connect to {host}:{port}"))?;
    // Every frame we send is tiny. Never let Nagle hold a call_service back.
    let _ = tcp.set_nodelay(true);
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(MAX_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_MESSAGE_BYTES));

    if secure {
        let tls = tls.ok_or_else(|| anyhow!("TLS is unavailable"))?;
        let name = ServerName::try_from(host.clone())
            .with_context(|| format!("{host:?} is not a valid TLS server name"))?;
        let stream = TlsConnector::from(Arc::clone(tls))
            .connect(name, tcp)
            .await
            .with_context(|| format!("TLS handshake with {host}"))?;
        let (ws, response) = client_async_with_config(request, stream, Some(ws_config))
            .await
            .map_err(|e| anyhow!(handshake_failure(&e)))?;
        debug!("WebSocket upgraded over TLS (HTTP {})", response.status());
        Ok(Ws::Tls(ws))
    } else {
        let (ws, response) = client_async_with_config(request, tcp, Some(ws_config))
            .await
            .map_err(|e| anyhow!(handshake_failure(&e)))?;
        debug!("WebSocket upgraded without TLS (HTTP {})", response.status());
        Ok(Ws::Plain(ws))
    }
}

/// Human reason for a failed WebSocket upgrade. An HTTP 403 gets the IP-ban hint: that's what
/// Home Assistant answers to every request once the address is listed in `ip_bans.yaml` (after
/// `login_attempts_threshold` failed logins), and a corrected token alone won't clear it.
fn handshake_failure(e: &WsError) -> String {
    match e {
        WsError::Http(response) => {
            let status = response.status();
            let mut reason = format!("WebSocket handshake rejected with HTTP {status}");
            if status.as_u16() == 403 {
                reason.push_str(
                    ": this IP may be banned by Home Assistant after repeated failed logins \
                     (remove it from ip_bans.yaml next to configuration.yaml and restart Home Assistant)",
                );
            }
            reason
        }
        other => format!("WebSocket handshake: {other}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    AwaitAuthRequired,
    AwaitAuthOk,
    AwaitSubscribed,
    Live,
}

/// Everything that lives exactly as long as one WebSocket connection.
struct Session<'a> {
    token: &'a str,
    entity_ids: &'a [String],
    subscribed: &'a HashSet<String>,
    events: &'a UnboundedSender<HaEvent>,
    phase: Phase,
    /// Next message id: strictly increasing per connection, shared by subscribe, calls and pings.
    next_id: u64,
    /// Merged state of every subscribed entity we have heard about on this connection.
    entities: HashMap<String, EntityState>,
    /// call_service message id -> `ServiceCall::request`.
    pending: HashMap<u64, u64>,
    /// Id of the ping whose pong we are waiting for.
    ping_outstanding: Option<u64>,
    /// Handshake deadline before `Live`, pong deadline after a ping, `None` = nothing to wait for.
    deadline: Option<Instant>,
    /// When `HaEvent::Connected` was emitted on this connection, `None` until then.
    live_since: Option<Instant>,
    /// HA answered `auth_invalid` on this connection.
    auth_rejected: bool,
    /// The first `a` event of the connection is still to come. HA sends it right after the
    /// subscribe result, complete for every subscribed entity that exists, so a subscribed id
    /// absent from it vanished (renamed, deleted) while we were away, and no `r` will ever say so.
    snapshot_pending: bool,
}

impl<'a> Session<'a> {
    fn new(
        token: &'a str,
        entity_ids: &'a [String],
        subscribed: &'a HashSet<String>,
        events: &'a UnboundedSender<HaEvent>,
    ) -> Self {
        Self {
            token,
            entity_ids,
            subscribed,
            events,
            phase: Phase::AwaitAuthRequired,
            next_id: SUBSCRIBE_ID,
            entities: HashMap::new(),
            pending: HashMap::new(),
            ping_outstanding: None,
            deadline: None,
            live_since: None,
            auth_rejected: false,
            snapshot_pending: true,
        }
    }

    /// How this session ended, for the reconnect policy.
    fn end(&self) -> SessionEnd {
        if self.auth_rejected {
            SessionEnd::AuthRejected
        } else {
            match self.live_since {
                Some(since) => SessionEnd::Live(since.elapsed()),
                None => SessionEnd::NeverLive,
            }
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn emit(&self, ev: HaEvent) {
        // The receiver only disappears while the process is shutting down.
        let _ = self.events.send(ev);
    }

    /// Fails every in-flight call (in the order they were sent) with `why`.
    fn fail_pending(&mut self, why: &str) {
        let mut pending: Vec<(u64, u64)> = self.pending.drain().collect();
        pending.sort_unstable();
        for (id, request) in pending {
            debug!("call id {id} (request {request}) failed: {why}");
            self.emit(HaEvent::CallResult { request, success: false, error: Some(why.to_string()) });
        }
    }

    async fn on_message<S>(&mut self, ws: &mut WebSocketStream<S>, msg: Message) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match msg {
            Message::Text(text) => {
                let text = text.as_str();
                let frame: Value = match serde_json::from_str(text) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("ignoring non-JSON frame ({e}): {}", excerpt(text));
                        return Ok(());
                    }
                };
                debug!("← {}", describe(&frame));
                trace!("← {}", excerpt(text));
                self.on_frame(ws, &frame).await
            }
            Message::Binary(bytes) => {
                debug!("ignoring binary frame ({} bytes)", bytes.len());
                Ok(())
            }
            Message::Ping(_) => {
                // tungstenite queued the pong. It only leaves with the next write or flush.
                debug!("← ws ping, flushing pong");
                ws.flush().await.map_err(|e| format!("pong flush failed: {e}"))
            }
            Message::Pong(_) => {
                debug!("← ws pong");
                Ok(())
            }
            Message::Close(frame) => Err(match frame {
                Some(f) => format!("closed by server ({:?} {:?})", f.code, f.reason.as_str()),
                None => "closed by server".to_string(),
            }),
            // Never produced when reading.
            Message::Frame(_) => Ok(()),
        }
    }

    async fn on_frame<S>(&mut self, ws: &mut WebSocketStream<S>, frame: &Value) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
        let id = frame.get("id").and_then(Value::as_u64);
        match kind {
            "auth_required" => {
                if self.phase != Phase::AwaitAuthRequired {
                    debug!("unexpected auth_required in phase {:?}", self.phase);
                    return Ok(());
                }
                debug!("→ auth (token redacted)");
                send_text(ws, auth_frame(self.token)).await?;
                self.phase = Phase::AwaitAuthOk;
                self.deadline = Some(Instant::now() + HANDSHAKE_TIMEOUT);
                Ok(())
            }
            "auth_ok" => {
                if self.phase != Phase::AwaitAuthOk {
                    debug!("unexpected auth_ok in phase {:?}", self.phase);
                    return Ok(());
                }
                let version = frame.get("ha_version").and_then(Value::as_str).unwrap_or("?");
                info!("authenticated with Home Assistant {version}");
                let id = self.next_id();
                send_json(ws, &subscribe_frame(id, self.entity_ids)).await?;
                self.phase = Phase::AwaitSubscribed;
                self.deadline = Some(Instant::now() + HANDSHAKE_TIMEOUT);
                Ok(())
            }
            "auth_invalid" => {
                let message = frame.get("message").and_then(Value::as_str).unwrap_or("no message");
                error!(
                    "Home Assistant rejected the access token: {message}. \
                     Fix HA_TOKEN in /etc/barclock.env and restart barclock"
                );
                self.auth_rejected = true;
                Err("authentication failed (HA_TOKEN rejected)".to_string())
            }
            "result" => match id {
                Some(SUBSCRIBE_ID) => self.on_subscribe_result(frame),
                Some(id) => {
                    match self.pending.remove(&id) {
                        Some(request) => {
                            let (success, error) = parse_result(frame);
                            debug!("call id {id} (request {request}) success={success} error={error:?}");
                            self.emit(HaEvent::CallResult { request, success, error });
                        }
                        None => debug!("result for unknown id {id}"),
                    }
                    Ok(())
                }
                None => {
                    debug!("result frame without id");
                    Ok(())
                }
            },
            "event" => {
                if id != Some(SUBSCRIBE_ID) {
                    debug!("event for unknown subscription {id:?}");
                    return Ok(());
                }
                match frame.get("event") {
                    Some(event) => self.on_event(event),
                    None => debug!("event frame without event body"),
                }
                Ok(())
            }
            "pong" => {
                if id.is_some() && id == self.ping_outstanding {
                    self.ping_outstanding = None;
                    self.deadline = None;
                } else {
                    debug!("pong for unexpected id {id:?} (waiting for {:?})", self.ping_outstanding);
                }
                Ok(())
            }
            other => {
                debug!("ignoring frame of type {other:?}");
                Ok(())
            }
        }
    }

    fn on_subscribe_result(&mut self, frame: &Value) -> Result<(), String> {
        let (success, error) = parse_result(frame);
        if !success {
            return Err(format!(
                "subscribe_entities failed: {}",
                error.unwrap_or_else(|| "unknown error".to_string())
            ));
        }
        if self.phase == Phase::Live {
            debug!("duplicate subscribe result");
            return Ok(());
        }
        self.phase = Phase::Live;
        self.deadline = None;
        self.live_since = Some(Instant::now());
        info!("Home Assistant connected, subscribed to {} entities", self.entity_ids.len());
        self.emit(HaEvent::Connected);
        Ok(())
    }

    /// One `subscribe_entities` event body: merges it and reports every change to the UI. The
    /// first `a` of the connection is the snapshot. Subscribed ids missing from it are reported
    /// as removed (after the adds, in subscription order) so the UI shows them unavailable.
    fn on_event(&mut self, event: &Value) {
        let mut changes = apply_event(&mut self.entities, self.subscribed, event);
        if self.snapshot_pending {
            if let Some(snapshot) = event.get("a").and_then(Value::as_object) {
                self.snapshot_pending = false;
                let missing = missing_from_snapshot(self.entity_ids, snapshot);
                if !missing.is_empty() {
                    let ids: Vec<&str> = missing
                        .iter()
                        .filter_map(|ev| match ev {
                            HaEvent::StateRemoved { entity_id } => Some(entity_id.as_str()),
                            _ => None,
                        })
                        .collect();
                    warn!(
                        "{} subscribed entities are missing from Home Assistant, shown as unavailable: {}",
                        ids.len(),
                        ids.join(", ")
                    );
                }
                changes.extend(missing);
            }
        }
        if changes.is_empty() {
            debug!("event carried no change for a subscribed entity");
        }
        for change in changes {
            self.emit(change);
        }
    }

    async fn on_call<S>(&mut self, ws: &mut WebSocketStream<S>, call: ServiceCall) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if self.phase != Phase::Live {
            reject(self.events, &call, "not connected");
            return Ok(());
        }
        if self.pending.len() >= MAX_PENDING_CALLS {
            warn!("{} service calls unanswered by Home Assistant", self.pending.len());
            reject(self.events, &call, "too many pending calls");
            return Ok(());
        }
        let id = self.next_id();
        debug!(
            "→ call_service {}.{} {:?} as id {id} (request {})",
            call.domain, call.service, call.entity_ids, call.request
        );
        self.pending.insert(id, call.request);
        // A failed send ends the session. The outer loop then fails this call as "disconnected".
        send_json(ws, &call_service_frame(id, &call)).await
    }

    async fn on_ping_tick<S>(&mut self, ws: &mut WebSocketStream<S>) -> Result<(), String>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        if self.phase != Phase::Live {
            return Ok(());
        }
        if self.ping_outstanding.is_some() {
            debug!("previous ping still unanswered, not sending another");
            return Ok(());
        }
        let id = self.next_id();
        self.ping_outstanding = Some(id);
        self.deadline = Some(Instant::now() + PONG_TIMEOUT);
        send_json(ws, &ping_frame(id)).await
    }
}

/// The one loop of a connection: socket frames, UI calls, ping ticks and the deadline.
/// Returns the reason the connection ended.
async fn serve<S>(
    mut ws: WebSocketStream<S>,
    s: &mut Session<'_>,
    calls: &mut Option<UnboundedReceiver<ServiceCall>>,
    ping_every: Duration,
) -> String
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut ping = tokio::time::interval_at(Instant::now() + ping_every, ping_every);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    s.deadline = Some(Instant::now() + HANDSHAKE_TIMEOUT);

    let reason = loop {
        let deadline = s.deadline;
        tokio::select! {
            frame = ws.next() => {
                let msg = match frame {
                    Some(Ok(msg)) => msg,
                    Some(Err(WsError::ConnectionClosed)) | Some(Err(WsError::AlreadyClosed)) | None => {
                        break "connection closed".to_string();
                    }
                    Some(Err(e)) => break format!("socket error: {e}"),
                };
                if let Err(reason) = s.on_message(&mut ws, msg).await {
                    break reason;
                }
            }
            call = next_call(calls) => {
                if let Err(reason) = s.on_call(&mut ws, call).await {
                    break reason;
                }
            }
            _ = ping.tick() => {
                if let Err(reason) = s.on_ping_tick(&mut ws).await {
                    break reason;
                }
            }
            _ = wait_until(deadline) => {
                break match s.phase {
                    Phase::Live => format!("no pong within {}s", PONG_TIMEOUT.as_secs()),
                    phase => format!("handshake timed out in phase {phase:?}"),
                };
            }
        }
    };
    // Best effort: let HA drop the session cleanly, but never wait on a dead peer.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, ws.close(None)).await;
    reason
}

/// Next service call from the UI. Pending forever once the UI has dropped its sender, so the
/// `select!` loops never spin on a closed channel.
async fn next_call(calls: &mut Option<UnboundedReceiver<ServiceCall>>) -> ServiceCall {
    if let Some(rx) = calls.as_mut() {
        if let Some(call) = rx.recv().await {
            return call;
        }
        debug!("service call channel closed");
        *calls = None;
    }
    std::future::pending().await
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Backoff sleep that keeps answering service calls with "not connected".
async fn sleep_rejecting(
    delay: Duration,
    calls: &mut Option<UnboundedReceiver<ServiceCall>>,
    events: &UnboundedSender<HaEvent>,
) {
    let until = Instant::now() + delay;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(until) => return,
            call = next_call(calls) => reject(events, &call, "not connected"),
        }
    }
}

fn reject(events: &UnboundedSender<HaEvent>, call: &ServiceCall, why: &str) {
    debug!("rejecting call_service {}.{} (request {}): {why}", call.domain, call.service, call.request);
    let _ = events.send(HaEvent::CallResult { request: call.request, success: false, error: Some(why.to_string()) });
}

async fn send_text<S>(ws: &mut WebSocketStream<S>, text: String) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    ws.send(Message::Text(text.into())).await.map_err(|e| format!("send failed: {e}"))
}

async fn send_json<S>(ws: &mut WebSocketStream<S>, frame: &Value) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    debug!("→ {}", describe(frame));
    let text = frame.to_string();
    trace!("→ {}", excerpt(&text));
    send_text(ws, text).await
}

// ---------------------------------------------------------------------------------------------
// Pure JSON helpers (unit-tested below)
// ---------------------------------------------------------------------------------------------

/// `{"type":"auth","access_token":...}`, never logged.
fn auth_frame(token: &str) -> String {
    json!({"type": "auth", "access_token": token}).to_string()
}

fn subscribe_frame(id: u64, entity_ids: &[String]) -> Value {
    json!({"id": id, "type": "subscribe_entities", "entity_ids": entity_ids})
}

fn ping_frame(id: u64) -> Value {
    json!({"id": id, "type": "ping"})
}

/// `call_service` frame, `service_data` is omitted when it's `Null` or an empty object.
fn call_service_frame(id: u64, call: &ServiceCall) -> Value {
    let mut frame = Map::new();
    frame.insert("id".into(), json!(id));
    frame.insert("type".into(), json!("call_service"));
    frame.insert("domain".into(), json!(call.domain));
    frame.insert("service".into(), json!(call.service));
    let has_data = match &call.service_data {
        Value::Null => false,
        Value::Object(m) => !m.is_empty(),
        _ => true,
    };
    if has_data {
        frame.insert("service_data".into(), call.service_data.clone());
    }
    frame.insert("target".into(), json!({"entity_id": call.entity_ids}));
    Value::Object(frame)
}

/// `(success, error)` of a `result` frame: `error.message` when present, else `error.code`.
fn parse_result(frame: &Value) -> (bool, Option<String>) {
    let success = frame.get("success").and_then(Value::as_bool).unwrap_or(false);
    if success {
        return (true, None);
    }
    let error = frame.get("error").and_then(|e| {
        e.get("message")
            .and_then(Value::as_str)
            .or_else(|| e.get("code").and_then(Value::as_str))
            .map(str::to_string)
    });
    (false, error)
}

/// Applies one compressed `subscribe_entities` event (`a` add/replace, `c` change, `r` remove)
/// to `entities`, keeping only subscribed ids, and returns the UI events in order.
fn apply_event(
    entities: &mut HashMap<String, EntityState>,
    subscribed: &HashSet<String>,
    event: &Value,
) -> Vec<HaEvent> {
    let mut out = Vec::new();
    if let Some(added) = event.get("a").and_then(Value::as_object) {
        for (entity_id, full) in added {
            if !subscribed.contains(entity_id) {
                debug!("ignoring unsubscribed entity {entity_id}");
                continue;
            }
            let state = full_state(full);
            entities.insert(entity_id.clone(), state.clone());
            out.push(HaEvent::StateChanged { entity_id: entity_id.clone(), state });
        }
    }
    if let Some(changed) = event.get("c").and_then(Value::as_object) {
        for (entity_id, diff) in changed {
            if !subscribed.contains(entity_id) {
                debug!("ignoring unsubscribed entity {entity_id}");
                continue;
            }
            let entity = entities.entry(entity_id.clone()).or_default();
            apply_diff(entity, diff);
            out.push(HaEvent::StateChanged { entity_id: entity_id.clone(), state: entity.clone() });
        }
    }
    if let Some(removed) = event.get("r").and_then(Value::as_array) {
        for entity_id in removed.iter().filter_map(Value::as_str) {
            if !subscribed.contains(entity_id) {
                continue;
            }
            entities.remove(entity_id);
            out.push(HaEvent::StateRemoved { entity_id: entity_id.to_string() });
        }
    }
    out
}

/// `StateRemoved` for every subscribed id (subscription order, duplicates once) that a snapshot
/// (`a` of the first event of a connection) doesn't carry.
fn missing_from_snapshot(entity_ids: &[String], snapshot: &Map<String, Value>) -> Vec<HaEvent> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(entity_ids.len());
    entity_ids
        .iter()
        .filter(|id| !snapshot.contains_key(id.as_str()) && seen.insert(id.as_str()))
        .map(|id| HaEvent::StateRemoved { entity_id: id.clone() })
        .collect()
}

/// `{"s": state, "a": {attrs}, ...}` -> a whole entity.
fn full_state(v: &Value) -> EntityState {
    EntityState {
        state: v.get("s").and_then(scalar_string).unwrap_or_default(),
        attributes: v
            .get("a")
            .and_then(Value::as_object)
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
    }
}

/// `{"+": {"s"?, "a"?: {...}, ...}, "-": {"a": [names]}}` merged into `entity`, either key may be
/// absent and anything malformed is skipped.
fn apply_diff(entity: &mut EntityState, diff: &Value) {
    if let Some(plus) = diff.get("+") {
        if let Some(state) = plus.get("s").and_then(scalar_string) {
            entity.state = state;
        }
        if let Some(attrs) = plus.get("a").and_then(Value::as_object) {
            for (k, v) in attrs {
                entity.attributes.insert(k.clone(), v.clone());
            }
        }
    }
    if let Some(names) = diff.get("-").and_then(|m| m.get("a")).and_then(Value::as_array) {
        for name in names.iter().filter_map(Value::as_str) {
            entity.attributes.remove(name);
        }
    }
}

/// HA states are strings. A bare number or bool is tolerated, anything else is ignored.
fn scalar_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(_) | Value::Bool(_) => Some(v.to_string()),
        _ => None,
    }
}

/// One-line summary of a frame for the debug log (type, id, and what an event/result carries).
fn describe(frame: &Value) -> String {
    let kind = frame.get("type").and_then(Value::as_str).unwrap_or("?");
    let mut s = kind.to_string();
    if let Some(id) = frame.get("id").and_then(Value::as_u64) {
        let _ = write!(s, " id={id}");
    }
    match kind {
        "event" => {
            if let Some(event) = frame.get("event") {
                for key in ["a", "c", "r"] {
                    if let Some(v) = event.get(key) {
                        let n = v.as_object().map(Map::len).or_else(|| v.as_array().map(Vec::len)).unwrap_or(0);
                        let _ = write!(s, " {key}={n}");
                    }
                }
            }
        }
        "result" => {
            let _ = write!(s, " success={}", frame.get("success").and_then(Value::as_bool).unwrap_or(false));
        }
        _ => {}
    }
    s
}

/// The first `LOG_BODY_CHARS` characters of a frame body (cut on a char boundary).
fn excerpt(text: &str) -> String {
    match text.char_indices().nth(LOG_BODY_CHARS) {
        Some((cut, _)) => format!("{}… ({} bytes)", &text[..cut], text.len()),
        None => text.to_string(),
    }
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn subs(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn attrs(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    fn light(state: &str, brightness: u64) -> EntityState {
        EntityState {
            state: state.into(),
            attributes: attrs(&[("brightness", json!(brightness)), ("friendly_name", json!("Plafonnier"))]),
        }
    }

    fn known_light() -> (HashMap<String, EntityState>, HashSet<String>) {
        let mut map = HashMap::new();
        map.insert("light.a".to_string(), light("on", 50));
        (map, subs(&["light.a", "sensor.a", "sensor.b"]))
    }

    #[test]
    fn add_replaces_the_whole_entity() {
        let (mut map, subs) = known_light();
        let ev = json!({"a": {"light.a": {"s": "off", "a": {"color_mode": "brightness"}, "lc": 1.0, "lu": 1.0, "c": "ctx"}}});
        let out = apply_event(&mut map, &subs, &ev);
        let expected = EntityState { state: "off".into(), attributes: attrs(&[("color_mode", json!("brightness"))]) };
        assert_eq!(out, vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: expected.clone() }]);
        assert_eq!(map.get("light.a"), Some(&expected));
    }

    #[test]
    fn change_with_plus_only_merges_state_and_attributes() {
        let (mut map, subs) = known_light();
        let ev = json!({"c": {"light.a": {"+": {"s": "on", "a": {"brightness": 200, "effect": "none"}, "lu": 2.0,
            "c": {"id": "x", "parent_id": null, "user_id": null}}}}});
        let out = apply_event(&mut map, &subs, &ev);
        let expected = EntityState {
            state: "on".into(),
            attributes: attrs(&[("brightness", json!(200)), ("friendly_name", json!("Plafonnier")), ("effect", json!("none"))]),
        };
        assert_eq!(out, vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: expected.clone() }]);
        assert_eq!(map["light.a"], expected);
    }

    #[test]
    fn change_with_minus_only_removes_attributes() {
        let (mut map, subs) = known_light();
        let ev = json!({"c": {"light.a": {"-": {"a": ["brightness", "not_there"]}}}});
        let out = apply_event(&mut map, &subs, &ev);
        let expected = EntityState { state: "on".into(), attributes: attrs(&[("friendly_name", json!("Plafonnier"))]) };
        assert_eq!(out, vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: expected.clone() }]);
        assert_eq!(map["light.a"], expected);
    }

    #[test]
    fn state_only_change_keeps_attributes() {
        let (mut map, subs) = known_light();
        let ev = json!({"c": {"light.a": {"+": {"s": "off", "lu": 3.0, "c": "ctx"}}}});
        let out = apply_event(&mut map, &subs, &ev);
        let expected = light("off", 50);
        assert_eq!(out, vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: expected.clone() }]);
        assert_eq!(map["light.a"], expected);
    }

    #[test]
    fn multiple_entities_in_one_event_emit_in_order() {
        let (mut map, subs) = known_light();
        let ev = json!({"a": {
            "sensor.a": {"s": "1.5", "a": {"unit_of_measurement": "W"}},
            "sensor.b": {"s": "unavailable", "a": {}},
        }});
        let out = apply_event(&mut map, &subs, &ev);
        assert_eq!(
            out,
            vec![
                HaEvent::StateChanged {
                    entity_id: "sensor.a".into(),
                    state: EntityState { state: "1.5".into(), attributes: attrs(&[("unit_of_measurement", json!("W"))]) },
                },
                HaEvent::StateChanged { entity_id: "sensor.b".into(), state: EntityState { state: "unavailable".into(), attributes: HashMap::new() } },
            ]
        );
        assert_eq!(map.len(), 3);
        assert!(map["sensor.a"].state_f64() == Some(1.5));
    }

    #[test]
    fn remove_drops_entity_and_ignores_unknown_ids() {
        let (mut map, subs) = known_light();
        let ev = json!({"r": ["light.a", "light.unsubscribed", 7, null]});
        let out = apply_event(&mut map, &subs, &ev);
        assert_eq!(out, vec![HaEvent::StateRemoved { entity_id: "light.a".into() }]);
        assert!(map.is_empty());
    }

    #[test]
    fn change_for_unseen_entity_creates_it() {
        let (mut map, subs) = known_light();
        let ev = json!({"c": {"sensor.a": {"+": {"s": "42"}}}});
        let out = apply_event(&mut map, &subs, &ev);
        assert_eq!(
            out,
            vec![HaEvent::StateChanged { entity_id: "sensor.a".into(), state: EntityState { state: "42".into(), attributes: HashMap::new() } }]
        );
    }

    #[test]
    fn unsubscribed_and_malformed_input_never_panics() {
        let (mut map, subs) = known_light();
        assert!(apply_event(&mut map, &subs, &json!({"a": {"light.other": {"s": "on", "a": {}}}})).is_empty());
        assert!(apply_event(&mut map, &subs, &json!("string")).is_empty());
        assert!(apply_event(&mut map, &subs, &json!({"a": [1, 2], "c": 5, "r": {"x": 1}})).is_empty());
        // Garbage inside a diff leaves the entity untouched but still reports it.
        let out = apply_event(&mut map, &subs, &json!({"c": {"light.a": {"+": "garbage", "-": {"a": "notalist"}}}}));
        assert_eq!(out, vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: light("on", 50) }]);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn call_service_frame_with_data() {
        let call = ServiceCall {
            request: 9,
            domain: "light".into(),
            service: "turn_on".into(),
            service_data: json!({"brightness_pct": 50}),
            entity_ids: vec!["light.a".into(), "light.b".into()],
        };
        let frame = call_service_frame(7, &call);
        assert_eq!(
            frame,
            json!({
                "id": 7, "type": "call_service", "domain": "light", "service": "turn_on",
                "service_data": {"brightness_pct": 50},
                "target": {"entity_id": ["light.a", "light.b"]}
            })
        );
    }

    #[test]
    fn call_service_frame_without_data() {
        let mut call = ServiceCall {
            request: 1,
            domain: "switch".into(),
            service: "toggle".into(),
            service_data: Value::Null,
            entity_ids: vec!["switch.a".into()],
        };
        let expected = json!({"id": 3, "type": "call_service", "domain": "switch", "service": "toggle",
            "target": {"entity_id": ["switch.a"]}});
        assert_eq!(call_service_frame(3, &call), expected);
        call.service_data = json!({});
        assert_eq!(call_service_frame(3, &call), expected);
        assert!(call_service_frame(3, &call).get("service_data").is_none());
    }

    #[test]
    fn result_parsing() {
        assert_eq!(parse_result(&json!({"id": 2, "type": "result", "success": true, "result": null})), (true, None));
        assert_eq!(
            parse_result(&json!({"id": 2, "type": "result", "success": false,
                "error": {"code": "not_found", "message": "Service light.nope not found."}})),
            (false, Some("Service light.nope not found.".into()))
        );
        assert_eq!(
            parse_result(&json!({"id": 2, "type": "result", "success": false, "error": {"code": "unknown_error"}})),
            (false, Some("unknown_error".into()))
        );
        assert_eq!(parse_result(&json!({"id": 2, "type": "result", "success": false})), (false, None));
        assert_eq!(parse_result(&json!({"id": 2, "type": "result"})), (false, None));
    }

    #[test]
    fn handshake_frames() {
        let auth: Value = serde_json::from_str(&auth_frame("s3cret")).unwrap();
        assert_eq!(auth, json!({"type": "auth", "access_token": "s3cret"}));
        let ids = vec!["light.a".to_string(), "sensor.b".to_string()];
        assert_eq!(
            subscribe_frame(SUBSCRIBE_ID, &ids),
            json!({"id": 1, "type": "subscribe_entities", "entity_ids": ["light.a", "sensor.b"]})
        );
        assert_eq!(ping_frame(12), json!({"id": 12, "type": "ping"}));
    }

    #[test]
    fn log_helpers() {
        let ev = json!({"id": 1, "type": "event", "event": {"c": {"light.a": {}}, "r": ["x", "y"]}});
        assert_eq!(describe(&ev), "event id=1 c=1 r=2");
        assert_eq!(describe(&json!({"id": 4, "type": "result", "success": true})), "result id=4 success=true");
        assert_eq!(describe(&json!(null)), "?");
        let long = "é".repeat(LOG_BODY_CHARS + 3);
        let cut = excerpt(&long);
        assert!(cut.starts_with(&"é".repeat(LOG_BODY_CHARS)));
        assert!(cut.ends_with(&format!("… ({} bytes)", long.len())));
        assert_eq!(excerpt("short"), "short");
    }

    // ---- reconnect policy ----

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn backoff_doubles_to_max_while_never_live() {
        let mut b = Backoff::new(secs(1), secs(30));
        let delays: Vec<u64> = (0..8).map(|_| b.delay_after(SessionEnd::NeverLive).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    }

    #[test]
    fn backoff_keeps_climbing_when_the_session_died_young() {
        // The reviewed storm: Connected, then a deterministic drop within seconds, forever.
        let mut b = Backoff::new(secs(1), secs(30));
        let delays: Vec<u64> = (0..7).map(|_| b.delay_after(SessionEnd::Live(secs(3))).as_secs()).collect();
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30]);
        // Just under the stability bar still counts as unstable.
        assert_eq!(b.delay_after(SessionEnd::Live(STABLE_SESSION - Duration::from_millis(1))), secs(30));
    }

    #[test]
    fn backoff_resets_after_a_stable_session() {
        let mut b = Backoff::new(secs(1), secs(30));
        for _ in 0..5 {
            b.delay_after(SessionEnd::NeverLive);
        }
        assert_eq!(b.delay_after(SessionEnd::NeverLive), secs(30));
        assert_eq!(b.delay_after(SessionEnd::Live(STABLE_SESSION)), secs(1));
        assert_eq!(b.delay_after(SessionEnd::Live(secs(5))), secs(2));
        assert_eq!(b.delay_after(SessionEnd::Live(secs(3600))), secs(1));
    }

    #[test]
    fn auth_rejected_waits_long_without_doubling() {
        let mut b = Backoff::new(secs(1), secs(30));
        assert_eq!(b.delay_after(SessionEnd::NeverLive), secs(1));
        assert_eq!(b.delay_after(SessionEnd::AuthRejected), AUTH_RETRY);
        assert_eq!(b.delay_after(SessionEnd::AuthRejected), AUTH_RETRY);
        assert!(AUTH_RETRY >= secs(15 * 60));
        // The climb resumes where it was, it's neither reset nor advanced by a rejected token.
        assert_eq!(b.delay_after(SessionEnd::NeverLive), secs(2));
    }

    #[test]
    fn backoff_tolerates_inverted_bounds() {
        let mut b = Backoff::new(secs(10), secs(2));
        assert_eq!(b.delay_after(SessionEnd::NeverLive), secs(10));
        assert_eq!(b.delay_after(SessionEnd::NeverLive), secs(10));
    }

    #[test]
    fn session_end_reflects_auth_and_liveness() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let ids = vec!["light.a".to_string()];
        let subs = subs(&["light.a"]);
        let mut s = Session::new("t", &ids, &subs, &tx);
        assert_eq!(s.end(), SessionEnd::NeverLive);
        s.live_since = Some(Instant::now());
        assert!(matches!(s.end(), SessionEnd::Live(d) if d < secs(5)));
        // auth_invalid wins even if the flags disagree.
        s.auth_rejected = true;
        assert_eq!(s.end(), SessionEnd::AuthRejected);
    }

    // ---- handshake failures ----

    #[test]
    fn http_403_upgrade_gets_the_ip_ban_hint() {
        use tokio_tungstenite::tungstenite::http::Response;
        let forbidden = Response::builder().status(403).body(None).unwrap();
        let reason = handshake_failure(&WsError::Http(Box::new(forbidden)));
        assert!(reason.starts_with("WebSocket handshake rejected with HTTP 403"), "{reason}");
        assert!(reason.contains("ip_bans.yaml"), "{reason}");
        assert!(reason.contains("banned"), "{reason}");

        let not_found = Response::builder().status(404).body(None).unwrap();
        let reason = handshake_failure(&WsError::Http(Box::new(not_found)));
        assert_eq!(reason, "WebSocket handshake rejected with HTTP 404 Not Found");

        let reason = handshake_failure(&WsError::ConnectionClosed);
        assert!(reason.starts_with("WebSocket handshake: "), "{reason}");
        assert!(!reason.contains("banned"));
    }

    // ---- snapshot pruning ----

    #[test]
    fn missing_from_snapshot_is_in_subscription_order_without_duplicates() {
        let ids: Vec<String> = ["light.a", "sensor.a", "sensor.b", "light.a", "switch.c"].iter().map(|s| s.to_string()).collect();
        let snapshot = json!({"sensor.a": {"s": "1"}, "light.unsubscribed": {"s": "on"}});
        let out = missing_from_snapshot(&ids, snapshot.as_object().unwrap());
        assert_eq!(
            out,
            vec![
                HaEvent::StateRemoved { entity_id: "light.a".into() },
                HaEvent::StateRemoved { entity_id: "sensor.b".into() },
                HaEvent::StateRemoved { entity_id: "switch.c".into() },
            ]
        );
        assert!(missing_from_snapshot(&ids, json!({"light.a": {}, "sensor.a": {}, "sensor.b": {}, "switch.c": {}}).as_object().unwrap()).is_empty());
        assert!(missing_from_snapshot(&[], snapshot.as_object().unwrap()).is_empty());
    }

    /// Drains everything the session emitted so far.
    fn drain(rx: &mut tokio::sync::mpsc::UnboundedReceiver<HaEvent>) -> Vec<HaEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    #[test]
    fn first_snapshot_of_a_session_reports_vanished_entities_once() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let ids: Vec<String> = ["light.a", "sensor.a", "sensor.b"].iter().map(|s| s.to_string()).collect();
        let subs = subs(&["light.a", "sensor.a", "sensor.b"]);
        let mut s = Session::new("t", &ids, &subs, &tx);

        // A `c` before the snapshot (can't happen on the wire, but must not consume the snapshot).
        s.on_event(&json!({"c": {"sensor.a": {"+": {"s": "1"}}}}));
        assert_eq!(drain(&mut rx), vec![HaEvent::StateChanged { entity_id: "sensor.a".into(), state: EntityState { state: "1".into(), attributes: HashMap::new() } }]);
        assert!(s.snapshot_pending);

        // The snapshot: light.a was renamed while we were away. Adds first, then the removals.
        s.on_event(&json!({"a": {"sensor.a": {"s": "2", "a": {}}, "sensor.b": {"s": "3", "a": {}}}}));
        assert_eq!(
            drain(&mut rx),
            vec![
                HaEvent::StateChanged { entity_id: "sensor.a".into(), state: EntityState { state: "2".into(), attributes: HashMap::new() } },
                HaEvent::StateChanged { entity_id: "sensor.b".into(), state: EntityState { state: "3".into(), attributes: HashMap::new() } },
                HaEvent::StateRemoved { entity_id: "light.a".into() },
            ]
        );
        assert!(!s.snapshot_pending);
        assert!(!s.entities.contains_key("light.a"));

        // A later `a` (entity created again) is a plain add: nothing else is pruned.
        s.on_event(&json!({"a": {"light.a": {"s": "on", "a": {}}}}));
        assert_eq!(drain(&mut rx), vec![HaEvent::StateChanged { entity_id: "light.a".into(), state: EntityState { state: "on".into(), attributes: HashMap::new() } }]);

        // An empty snapshot on a fresh session prunes everything, in subscription order.
        let mut s = Session::new("t", &ids, &subs, &tx);
        s.on_event(&json!({"a": {}}));
        assert_eq!(
            drain(&mut rx),
            vec![
                HaEvent::StateRemoved { entity_id: "light.a".into() },
                HaEvent::StateRemoved { entity_id: "sensor.a".into() },
                HaEvent::StateRemoved { entity_id: "sensor.b".into() },
            ]
        );
        // A malformed `a` isn't a snapshot and leaves the pruning for the real one.
        let mut s = Session::new("t", &ids, &subs, &tx);
        s.on_event(&json!({"a": [1, 2]}));
        assert!(drain(&mut rx).is_empty());
        assert!(s.snapshot_pending);
    }
}
