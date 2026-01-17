use {
    crate::{node::Kind, SharedError},
    cid::Cid,
    reqwest::{Client, Proxy},
    std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
        vec::Vec,
    },
    tokio::io::{AsyncRead, AsyncSeek, BufReader},
};

use std::path::Path;

use rseek::Seekable;

use crate::node_reader::Len;

/// Default base URL used to fetch compact epoch CAR archives hosted by Old Faithful.
pub const BASE_URL: &str = "https://files.old-faithful.net";

/// A small pool of [`reqwest::Client`] instances, optionally configured with proxies.
///
/// `reqwest::Client` stores proxy configuration at build time, so to support
/// proxy rotation we keep multiple clients (one per proxy URL) and select one
/// per request using round-robin.
///
/// This is designed to work well with [`rseek::Seekable`], which will call a
/// request factory closure multiple times (re-open, range fetch, retries).
#[derive(Clone)]
pub struct HttpPool {
    clients: Arc<Vec<Client>>,
    proxies: Arc<Vec<Option<String>>>, // None => direct
    rr: Arc<AtomicUsize>,
}

impl HttpPool {
    /// Builds a new [`HttpPool`].
    ///
    /// `proxies` may contain URLs like:
    /// - `http://user:pass@host:port`
    /// - `socks5h://host:port` (DNS via proxy)
    ///
    /// If `proxies` is empty, the pool contains a single direct client (no proxy).
    pub fn new(proxies: Vec<String>) -> Self {
        if proxies.is_empty() {
            return Self {
                clients: Arc::new(vec![Self::build_client(None)]),
                proxies: Arc::new(vec![None]),
                rr: Arc::new(AtomicUsize::new(0)),
            };
        }

        let mut clients: Vec<Client> = Vec::with_capacity(proxies.len());
        let mut meta: Vec<Option<String>> = Vec::with_capacity(proxies.len());

        for p in proxies {
            clients.push(Self::build_client(Some(p.as_str())));
            meta.push(Some(p));
        }

        Self {
            clients: Arc::new(clients),
            proxies: Arc::new(meta),
            rr: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Picks a client from the pool using round-robin selection.
    ///
    /// Returns `(idx, proxy_url, client)` where `proxy_url` is `None` for direct clients.
    #[inline]
    pub fn pick_with_meta(&self) -> (usize, Option<&str>, Client) {
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        let proxy = self.proxies[idx].as_deref();
        (idx, proxy, self.clients[idx].clone())
    }

    fn build_client(proxy: Option<&str>) -> Client {
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            // 10s can be aggressive for large/ranged reads; adjust if needed.
            .timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(30));

        if let Some(p) = proxy {
            builder = builder.proxy(Proxy::all(p).expect("bad proxy url"));
        }

        builder.build().expect("failed to build reqwest client")
    }
}

/* ────────────────────────────────────────────────────────────────────────── */
/*  Epoch node                                                                 */
/* ────────────────────────────────────────────────────────────────────────── */

/// Representation of a `Kind::Epoch` node pointing to subset indexes.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Epoch {
    /// Kind discriminator copied from the CBOR payload.
    pub kind: u64,
    /// Epoch number encoded in the payload.
    pub epoch: u64,
    /// Subset CIDs that compose the epoch.
    pub subsets: Vec<Cid>,
}

impl Epoch {
    /// Decodes an [`Epoch`] from raw CBOR bytes.
    pub fn from_bytes(data: Vec<u8>) -> Result<Epoch, SharedError> {
        let decoded_data: serde_cbor::Value = serde_cbor::from_slice(&data).unwrap();
        Epoch::from_cbor(decoded_data)
    }

    /// Decodes an [`Epoch`] from a CBOR [`serde_cbor::Value`].
    pub fn from_cbor(val: serde_cbor::Value) -> Result<Epoch, SharedError> {
        let mut epoch = Epoch {
            kind: 0,
            epoch: 0,
            subsets: vec![],
        };

        if let serde_cbor::Value::Array(array) = val {
            if let Some(serde_cbor::Value::Integer(kind)) = array.first() {
                epoch.kind = *kind as u64;

                if *kind as u64 != Kind::Epoch as u64 {
                    return Err(Box::new(std::io::Error::other(std::format!(
                        "Wrong kind for Epoch. Expected {:?}, got {:?}",
                        Kind::Epoch,
                        kind
                    ))) as SharedError);
                }
            }

            if let Some(serde_cbor::Value::Integer(num)) = array.get(1) {
                epoch.epoch = *num as u64;
            }

            if let Some(serde_cbor::Value::Array(subsets)) = &array.get(2) {
                for subset in subsets {
                    if let serde_cbor::Value::Bytes(subset) = subset {
                        epoch
                            .subsets
                            .push(Cid::try_from(subset[1..].to_vec()).unwrap());
                    }
                }
            }
        }

        Ok(epoch)
    }

    /// Renders the epoch as a JSON object for debugging.
    pub fn to_json(&self) -> serde_json::Value {
        let mut subsets = vec![];
        for subset in &self.subsets {
            subsets.push(serde_json::json!({ "/": subset.to_string() }));
        }

        let mut map = serde_json::Map::new();
        map.insert("kind".to_string(), serde_json::Value::from(self.kind));
        map.insert("epoch".to_string(), serde_json::Value::from(self.epoch));
        map.insert("subsets".to_string(), serde_json::Value::from(subsets));

        serde_json::Value::from(map)
    }
}

/// Fetches an epoch’s CAR file from Old Faithful as a buffered, seekable async stream.
///
/// The returned reader implements [`Len`] and can be consumed sequentially or
/// randomly via [`AsyncSeek`].
///
/// Logging:
/// - Logs which pool index is used and the proxy URL (if any).
/// - Note: we cannot log HTTP status here because `rseek` performs the `.send().await` internally.
pub async fn fetch_epoch_stream(
    epoch: u64,
    pool: Arc<HttpPool>,
) -> impl AsyncRead + AsyncSeek + Len {
    let url = format!("{}/{}/epoch-{}.car", BASE_URL, epoch, epoch);

    let seekable = Seekable::new(move || {
        let (idx, proxy, client) = pool.pick_with_meta();

        if let Some(p) = proxy {
            eprintln!("[old-faithful] epoch={epoch} open idx={idx} proxy={p} url={url}");
        } else {
            eprintln!("[old-faithful] epoch={epoch} open idx={idx} direct url={url}");
        }

        client.get(url.clone())
    })
    .await;

    BufReader::with_capacity(8 * 1024 * 1024, seekable)
}

/// Reads a proxy list file and converts it into a list of proxy URLs.
///
/// # File format
/// The file must contain one proxy per line in the format:
/// `ip:port:user:pass`
///
/// - Empty lines are ignored.
/// - Lines starting with `#` are treated as comments and ignored.
/// - Whitespace around each line is trimmed.
///
/// Returns proxies as `http://user:pass@ip:port` strings (ready for `reqwest::Proxy::all`).
pub fn read_proxies_file(path: impl AsRef<Path>) -> std::io::Result<Vec<String>> {
    let content = std::fs::read_to_string(path)?;
    let mut out = Vec::new();

    for (i, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split(':').collect();
        if parts.len() != 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "bad proxy line {}: {:?} (expected ip:port:user:pass)",
                    i + 1,
                    line
                ),
            ));
        }

        let ip = parts[0];
        let port = parts[1];
        let user = parts[2];
        let pass = parts[3];

        out.push(format!("http://{}:{}@{}:{}", user, pass, ip, port));
    }

    Ok(out)
}
