//! Direct polling of an IoTaWatt energy monitor on the LAN.
//!
//! Home Assistant's IoTaWatt integration refreshes on a fixed ~30 s cycle, so the house total and
//! the servers' draw shown on the panel can be half a minute old. The device itself recomputes
//! every second and answers a ~1 KB JSON status request in about 100 ms, which is what this task
//! reads. `main.rs` prefers these values and falls back to the Home Assistant sensors whenever the
//! device stops answering, so an unreachable monitor degrades to the previous behaviour instead of
//! blanking the module.
//!
//! No HTTP crate is pulled in for this: the endpoint is fixed and plain HTTP, and the device
//! answers with `Content-Length`, so a request is one write followed by a read of exactly that
//! many body bytes. Reading to end of stream instead would work but be uselessly slow: the device
//! announces `Connection: close` and then lingers about a second before actually closing, which
//! is longer than a sensible poll timeout.
//!
//! The house total is a *named output* (`HydroQuebec` = `@13+@14`, a float). The servers' draw is a
//! *named input*, which the status document identifies by channel number only, so the name is
//! resolved to its channel through `/config.txt` once per healthy stretch. Input watts come back as
//! a right-aligned string (`" 5"`, `"962"`), outputs as a number. Both shapes are accepted.

use std::fmt;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{interval, timeout, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::config::IotaWatt;

/// What the poller tells the UI thread.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PowerEvent {
    /// Fresh watts straight from the device. `None` = not configured, or absent from the response.
    Live { house: Option<f64>, servers: Option<f64> },
    /// The device stopped answering: go back to the Home Assistant sensors.
    Down,
}

/// Refuse to buffer more than this from the device (real bodies are ~1 KB and ~4 KB).
const MAX_BODY: usize = 256 * 1024;

/// Never poll faster than this, whatever the config says.
const MIN_POLL_MS: u64 = 200;
const MAX_POLL_MS: u64 = 300_000;
const MIN_TIMEOUT_MS: u64 = 200;
const MAX_TIMEOUT_MS: u64 = 30_000;

/// Poll the monitor until the process ends. Never returns while it's configured.
pub async fn run(cfg: IotaWatt, tx: UnboundedSender<PowerEvent>) {
    if cfg.url.trim().is_empty() {
        debug!("iotawatt: no url configured, house and servers stay on the Home Assistant sensors");
        return;
    }
    let ep = match Endpoint::parse(&cfg.url) {
        Ok(ep) => ep,
        Err(e) => {
            error!("iotawatt: {e:#}; house and servers stay on the Home Assistant sensors");
            return;
        }
    };

    let poll = Duration::from_millis(cfg.poll_interval_ms.clamp(MIN_POLL_MS, MAX_POLL_MS));
    let io_timeout = Duration::from_millis(cfg.timeout_ms.clamp(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS));
    let give_up_after = cfg.failures_before_fallback.max(1);
    info!(
        "iotawatt: polling http://{ep}/ every {} ms (house output {:?}, servers input {:?})",
        poll.as_millis(),
        cfg.house_output,
        cfg.servers_input
    );

    let mut ticker = interval(poll);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Channel of the named servers input, resolved from /config.txt. Cleared whenever the device
    // goes away so a reconfigured monitor is picked up on the way back.
    let mut servers_channel: Option<i64> = None;
    let mut name_warned = false;
    let mut failures: u32 = 0;
    let mut fell_back = false;
    let mut last_error = String::new();

    loop {
        ticker.tick().await;

        let status = match fetch_status(&ep, io_timeout).await {
            Ok(s) => s,
            Err(e) => {
                failures += 1;
                let msg = format!("{e:#}");
                // One warn per distinct outage reason. The repeats are debug so a monitor that's
                // off for a week doesn't fill the journal at 1 Hz.
                if msg != last_error {
                    warn!("iotawatt: {msg} (attempt {failures})");
                    last_error = msg;
                } else {
                    debug!("iotawatt: still failing, attempt {failures}");
                }
                if failures >= give_up_after && !fell_back {
                    warn!(
                        "iotawatt: unreachable after {failures} polls, house and servers fall back to the Home Assistant sensors"
                    );
                    fell_back = true;
                    servers_channel = None;
                    name_warned = false;
                    if tx.send(PowerEvent::Down).is_err() {
                        return;
                    }
                }
                continue;
            }
        };

        if failures > 0 {
            info!("iotawatt: answering again after {failures} failed poll(s)");
            failures = 0;
            fell_back = false;
            last_error.clear();
        }

        // Resolve the servers input's channel lazily: one extra request per healthy stretch.
        if servers_channel.is_none() && !cfg.servers_input.trim().is_empty() {
            match resolve_input_channel(&ep, &cfg.servers_input, io_timeout).await {
                Ok(Some(ch)) => {
                    debug!("iotawatt: input {:?} is channel {ch}", cfg.servers_input);
                    servers_channel = Some(ch);
                    name_warned = false;
                }
                Ok(None) => {
                    if !name_warned {
                        warn!(
                            "iotawatt: no input named {:?} on the device, the servers value stays on Home Assistant",
                            cfg.servers_input
                        );
                        name_warned = true;
                    }
                }
                Err(e) => debug!("iotawatt: reading config.txt failed: {e:#}"),
            }
        }

        let house = status.output_watts(&cfg.house_output);
        if house.is_none() && !cfg.house_output.trim().is_empty() && !name_warned {
            warn!(
                "iotawatt: no output named {:?} on the device (it has {:?})",
                cfg.house_output,
                status.output_names()
            );
            name_warned = true;
        }
        let servers = servers_channel.and_then(|ch| status.input_watts(ch));

        if tx.send(PowerEvent::Live { house, servers }).is_err() {
            return;
        }
    }
}

async fn fetch_status(ep: &Endpoint, t: Duration) -> Result<Status> {
    let body = http_get(ep, "/status?inputs=yes&outputs=yes", t).await?;
    serde_json::from_slice(&body).context("parsing the status JSON")
}

async fn resolve_input_channel(ep: &Endpoint, name: &str, t: Duration) -> Result<Option<i64>> {
    let body = http_get(ep, "/config.txt", t).await?;
    let cfg: DeviceConfig = serde_json::from_slice(&body).context("parsing config.txt")?;
    Ok(cfg.channel_of(name))
}

/// One plain-HTTP GET, bounded by `t` end to end.
async fn http_get(ep: &Endpoint, path: &str, t: Duration) -> Result<Vec<u8>> {
    let exchange = async {
        let mut sock = TcpStream::connect((ep.host.as_str(), ep.port))
            .await
            .with_context(|| format!("connecting to {ep}"))?;
        let _ = sock.set_nodelay(true);
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: {ep}\r\nUser-Agent: barclock\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
        );
        sock.write_all(req.as_bytes()).await.context("sending the request")?;
        let mut buf = Vec::with_capacity(2048);
        let mut chunk = [0u8; 2048];
        let mut framing: Option<(usize, Option<usize>)> = None; // (end of headers, Content-Length)
        loop {
            // Stop as soon as the announced body is complete instead of waiting for the close.
            if let Some((head_end, Some(len))) = framing {
                if buf.len() >= head_end + 4 + len {
                    break;
                }
            }
            let n = sock.read(&mut chunk).await.context("reading the response")?;
            if n == 0 {
                break; // no Content-Length: the close is the end of the body
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.len() > MAX_BODY {
                bail!("response larger than {MAX_BODY} bytes");
            }
            if framing.is_none() {
                if let Some(end) = head_end(&buf) {
                    framing = Some((end, content_length(&buf[..end])));
                }
            }
        }
        Ok::<Vec<u8>, anyhow::Error>(buf)
    };
    let raw = timeout(t, exchange)
        .await
        .map_err(|_| anyhow!("no answer from {ep} within {} ms", t.as_millis()))??;
    split_body(&raw)
}

/// End of the header block, if the buffer already holds it.
fn head_end(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|w| w == b"\r\n\r\n")
}

/// `Content-Length` from a header block. The device always sends one. Anything else falls back to
/// reading until the connection closes.
fn content_length(head: &[u8]) -> Option<usize> {
    let head = std::str::from_utf8(head).ok()?;
    head.lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.trim().parse::<usize>().ok())
}

/// Check the status line and return the body, trimmed to `Content-Length` when it's present.
/// The device never uses chunked encoding, so everything after the blank line is the body.
fn split_body(raw: &[u8]) -> Result<Vec<u8>> {
    let end = head_end(raw).ok_or_else(|| anyhow!("truncated response: no end of headers"))?;
    let head = &raw[..end];
    let head_str = std::str::from_utf8(head).context("non-UTF-8 response headers")?;
    let status_line = head_str.lines().next().unwrap_or_default().trim();
    if status_line.split_whitespace().nth(1) != Some("200") {
        bail!("unexpected response status: {status_line:?}");
    }
    let body = &raw[end + 4..];
    Ok(match content_length(head) {
        Some(n) if n <= body.len() => body[..n].to_vec(),
        _ => body.to_vec(),
    })
}

/// `/status?inputs=yes&outputs=yes`
#[derive(Debug, Deserialize)]
struct Status {
    #[serde(default)]
    inputs: Vec<StatusInput>,
    #[serde(default)]
    outputs: Vec<StatusOutput>,
}

#[derive(Debug, Deserialize)]
struct StatusInput {
    channel: i64,
    #[serde(rename = "Watts", default)]
    watts: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct StatusOutput {
    #[serde(default)]
    name: String,
    #[serde(default)]
    value: Option<Value>,
}

impl Status {
    fn output_watts(&self, name: &str) -> Option<f64> {
        if name.trim().is_empty() {
            return None;
        }
        self.outputs
            .iter()
            .find(|o| o.name.eq_ignore_ascii_case(name.trim()))
            .and_then(|o| o.value.as_ref())
            .and_then(as_watts)
    }

    fn input_watts(&self, channel: i64) -> Option<f64> {
        self.inputs
            .iter()
            .find(|i| i.channel == channel)
            .and_then(|i| i.watts.as_ref())
            .and_then(as_watts)
    }

    fn output_names(&self) -> Vec<&str> {
        self.outputs.iter().map(|o| o.name.as_str()).collect()
    }
}

/// `/config.txt`. Unused input slots come back as `null`.
#[derive(Debug, Deserialize)]
struct DeviceConfig {
    #[serde(default)]
    inputs: Vec<Option<ConfigInput>>,
}

#[derive(Debug, Deserialize)]
struct ConfigInput {
    channel: i64,
    #[serde(default)]
    name: String,
}

impl DeviceConfig {
    fn channel_of(&self, name: &str) -> Option<i64> {
        let want = name.trim();
        self.inputs
            .iter()
            .flatten()
            .find(|i| i.name.trim().eq_ignore_ascii_case(want))
            .map(|i| i.channel)
    }
}

/// Inputs report watts as a right-aligned string, outputs as a number.
fn as_watts(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
    .filter(|w| w.is_finite())
}

#[derive(Debug, PartialEq)]
struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self> {
        let s = url.trim();
        if s.starts_with("https://") {
            bail!("the IoTaWatt is polled over plain HTTP, https is not supported: {s:?}");
        }
        let rest = s.strip_prefix("http://").unwrap_or(s);
        let authority = rest.split('/').next().unwrap_or_default().trim();
        if authority.is_empty() {
            bail!("no host in the iotawatt url {s:?}");
        }
        // Only split on the last colon so a bare host keeps working.
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) => (
                h,
                p.parse::<u16>().with_context(|| format!("bad port in the iotawatt url {s:?}"))?,
            ),
            None => (authority, 80),
        };
        if host.is_empty() {
            bail!("no host in the iotawatt url {s:?}");
        }
        Ok(Self { host: host.to_string(), port })
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.port == 80 {
            write!(f, "{}", self.host)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured from the real device on 2026-09-06 (trimmed to the channels that matter).
    const STATUS: &str = r#"{"inputs":[{"channel":0,"Vrms":123.5632,"Hz":60.03416,"phase":0.6},
        {"channel":3,"Watts":" 5","Pf":0.477556},{"channel":4,"Watts":"962","Pf":0.953421},
        {"channel":13,"Watts":"3399"},{"channel":14,"Watts":"3657"}],
        "outputs":[{"name":"HydroQuebec","units":"Watts","value":7027.6}]}"#;

    const CONFIG: &str = r#"{"inputs":[{"channel":0,"name":"Tension","model":"Jameco/112336"},null,
        {"channel":4,"name":"Serveurs","model":"AcuCT-H040-50"},
        {"channel":10,"name":"Poele","model":"AcuCT-H040-50"}],
        "outputs":[{"name":"HydroQuebec","script":"@13+@14","units":"Watts"}]}"#;

    #[test]
    fn reads_the_house_output_and_the_servers_input() {
        let s: Status = serde_json::from_str(STATUS).unwrap();
        assert_eq!(s.output_watts("HydroQuebec"), Some(7027.6));
        assert_eq!(s.output_watts("hydroquebec"), Some(7027.6), "names match case-insensitively");
        assert_eq!(s.output_watts("Nope"), None);
        assert_eq!(s.output_watts(""), None);
        // padded strings, and the voltage channel that carries no Watts at all
        assert_eq!(s.input_watts(4), Some(962.0));
        assert_eq!(s.input_watts(3), Some(5.0));
        assert_eq!(s.input_watts(0), None);
        assert_eq!(s.input_watts(99), None);
    }

    #[test]
    fn resolves_an_input_name_to_its_channel_skipping_empty_slots() {
        let c: DeviceConfig = serde_json::from_str(CONFIG).unwrap();
        assert_eq!(c.channel_of("Serveurs"), Some(4));
        assert_eq!(c.channel_of(" serveurs "), Some(4));
        assert_eq!(c.channel_of("Poele"), Some(10));
        assert_eq!(c.channel_of("Absent"), None);
    }

    #[test]
    fn watts_accept_both_shapes_and_reject_junk() {
        assert_eq!(as_watts(&serde_json::json!(7027.6)), Some(7027.6));
        assert_eq!(as_watts(&serde_json::json!(" 962 ")), Some(962.0));
        assert_eq!(as_watts(&serde_json::json!("-12")), Some(-12.0));
        assert_eq!(as_watts(&serde_json::json!("")), None);
        assert_eq!(as_watts(&serde_json::json!("n/a")), None);
        assert_eq!(as_watts(&serde_json::json!(null)), None);
        assert_eq!(as_watts(&serde_json::json!({"a": 1})), None);
    }

    #[test]
    fn reads_content_length_from_headers() {
        assert_eq!(content_length(b"HTTP/1.1 200 OK\r\nContent-Length: 950"), Some(950));
        assert_eq!(content_length(b"HTTP/1.1 200 OK\r\ncontent-length:  12  "), Some(12));
        assert_eq!(content_length(b"HTTP/1.1 200 OK\r\nContent-Type: application/json"), None);
        assert_eq!(content_length(b"HTTP/1.1 200 OK\r\nContent-Length: nope"), None);
    }

    #[test]
    fn splits_the_body_and_refuses_non_200() {
        let ok = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
        assert_eq!(split_body(ok).unwrap(), b"{}");
        let missing = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        assert!(split_body(missing).unwrap_err().to_string().contains("404"));
        assert!(split_body(b"HTTP/1.1 200 OK\r\nno end of headers").is_err());
        // a body containing the separator's bytes must not confuse the split
        let tricky = b"HTTP/1.1 200 OK\r\n\r\n{\"a\":\"\r\n\r\n\"}";
        assert_eq!(split_body(tricky).unwrap(), b"{\"a\":\"\r\n\r\n\"}");
        // Content-Length wins over whatever else the device leaves on the socket
        let extra = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}trailing junk";
        assert_eq!(split_body(extra).unwrap(), b"{}");
        // a too-large Content-Length keeps what actually arrived instead of panicking
        let short = b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n{}";
        assert_eq!(split_body(short).unwrap(), b"{}");
    }

    #[test]
    fn parses_urls() {
        assert_eq!(
            Endpoint::parse("http://192.168.1.50/").unwrap(),
            Endpoint { host: "192.168.1.50".into(), port: 80 }
        );
        assert_eq!(
            Endpoint::parse("192.168.1.50").unwrap(),
            Endpoint { host: "192.168.1.50".into(), port: 80 }
        );
        assert_eq!(
            Endpoint::parse("http://iotawatt.local:8080/status").unwrap(),
            Endpoint { host: "iotawatt.local".into(), port: 8080 }
        );
        assert_eq!(Endpoint::parse("http://192.168.1.50/").unwrap().to_string(), "192.168.1.50");
        assert_eq!(
            Endpoint::parse("http://h:8080/").unwrap().to_string(),
            "h:8080",
            "the Host header keeps a non-default port"
        );
        assert!(Endpoint::parse("https://192.168.1.50/").is_err());
        assert!(Endpoint::parse("http://host:notaport/").is_err());
        assert!(Endpoint::parse("   ").is_err());
    }
}
