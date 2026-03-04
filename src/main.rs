mod store;

use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::{ConnectInfo, Query, State, OriginalUri},
    http::StatusCode,
    response::{Html, Response},
    routing::get,
    Router,
};
use clap::Parser;
use futures_util::{StreamExt, SinkExt};
use once_cell::sync::Lazy;
use percent_encoding::percent_decode_str;
use rand::Rng;
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc, oneshot, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::hash::{Hasher};
use std::collections::hash_map::DefaultHasher;
use time::{OffsetDateTime, UtcOffset, format_description::FormatItem};
use time::macros::format_description;
use tracing::{info, debug};
use store::PeerStore;

/// Tracker Server - BitTorrent tracker load balancer
#[derive(Parser, Debug)]
#[command(name = "tracker-server")]
#[command(about = "BitTorrent tracker load balancer", long_about = None)]
struct Args {
    /// Server bind address
    #[arg(long, default_value = "0.0.0.0:1337")]
    bind: String,

    /// Default announce interval in seconds
    #[arg(long, default_value = "900")]
    interval: i64,

    /// Minimum interval in seconds
    #[arg(long, default_value = "450")]
    min_interval: i64,

    /// Peer TTL in seconds (for cleanup)
    #[arg(long, default_value = "3600")]
    peer_ttl: i64,
}

// 环境开关
static LOAD_BALANCE_ALL: Lazy<bool> = Lazy::new(|| std::env::var("LOAD_BALANCE_ALL").map(|v| v=="1").unwrap_or(false));
static SUPPRESS_WARN: Lazy<bool> = Lazy::new(|| std::env::var("SUPPRESS_WARN").map(|v| v=="1").unwrap_or(false));
macro_rules! wlog { ($($arg:tt)*) => { if !*SUPPRESS_WARN { debug!($($arg)*); } } }

#[derive(Clone, Copy)]
enum UpstreamStrategy { All, RoundRobin, Random }

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FastMode { Wait, Timeout, Async, Off }

// 进程级上游 announce 使用的 20 字节 peer_id（BT 客户端标识），模仿 qBittorrent 风格
static PEER_ID_ENC: Lazy<String> = Lazy::new(|| {
    let mut id = [0u8; 20];
    let prefix = b"-qB4630-";
    id[..prefix.len()].copy_from_slice(prefix);
    const ALPHANUM: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::thread_rng();
    for i in prefix.len()..20 { id[i] = ALPHANUM[rng.gen_range(0..ALPHANUM.len())]; }
    percent_encoding::percent_encode(&id, percent_encoding::NON_ALPHANUMERIC).to_string()
});

#[derive(Clone)]
struct AppState {
    store: Arc<PeerStore>,
    interval: i64,
    min_interval: i64,
    // 上游 trackers 列表（热更新）
    trackers_http: Arc<RwLock<Vec<String>>>,
    trackers_udp: Arc<RwLock<Vec<String>>>,
    trackers_ws: Arc<RwLock<Vec<String>>>,
    // 聚合/并发控制
    poll_interval: Duration,
    upstream_strategy: UpstreamStrategy,
    batch_size: usize,
    rr_index: Arc<std::sync::atomic::AtomicUsize>,
    timeout: Duration,
    fast_mode: FastMode,
    fast_max_wait: Duration,
    semaphore: Arc<tokio::sync::Semaphore>,
    /// upstream fetch shards senders
    upstream_senders: Arc<Vec<mpsc::Sender<UpstreamJob>>>,
    upstream_shards: usize,
    /// per-shard map of in-flight info_hash -> waiters
    upstream_inflight: Arc<Vec<Arc<Mutex<HashMap<String, Vec<oneshot::Sender<()>>>>>>>,
    app_started_at: std::time::Instant,
    accumulated_uptime: Arc<AtomicU64>, // seconds from previous runs
    announce_queries: Arc<AtomicU64>,
    dht_enable: bool,
    dht_bootstrap: Arc<Vec<String>>,
    dht_timeout: Duration,
    dht_max_nodes: usize,
    dht_random_queries: usize,
    dht_listen_port: Option<u16>,
    pex_enable: bool,
    pex_timeout: Duration,
    pex_max_peers_per_hash: usize,
    // counters for debugging removed; discovery now contributes to peers/torrents totals
}

#[derive(Clone)]
struct TorrentView {
    filename: String,
    info_hash: String,
    seeders: usize,
    leechers: usize,
    completed: u64,
    total_peers: usize,
    last_seen: i64,
}

#[derive(Clone)]
struct DashboardSnapshot {
    torrents: usize,
    peers: usize,
    queries: u64,
    uptime: String,
    top: Vec<TorrentView>,
}

#[derive(Debug, Deserialize)]
struct DashboardQuery {
    search: Option<String>,
}

static TS_FMT: &[FormatItem<'static>] = format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");

#[derive(Clone)]
struct UpstreamJob {
    ih_hex: String,
    ih_raw: Vec<u8>,
    params: UpstreamAnnounceParams,
}

#[derive(Debug, Deserialize)]
struct AnnounceQuery {
    info_hash: Option<String>,
    ih: Option<String>,
    peer_id: Option<String>,
    port: Option<u16>,
    _uploaded: Option<u64>,
    _downloaded: Option<u64>,
    left: Option<u64>,
    event: Option<String>,
    numwant: Option<usize>,
    compact: Option<u8>,
    ip: Option<String>,
}

static TRACKER_ID: Lazy<String> = Lazy::new(|| {
    let r: u64 = rand::thread_rng().gen();
    format!("{:016x}", r)
});

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let addr: SocketAddr = args
        .bind
        .parse()
        .context("failed to parse bind address")?;

    if addr.ip().is_unspecified() {
        eprintln!(
            "[tracker-server] 启动中，访问地址将是: http://127.0.0.1:{}/",
            addr.port()
        );
    } else {
        eprintln!("[tracker-server] 启动中，访问地址将是: http://{}/", addr);
    }

    let peer_store = Arc::new(PeerStore::new());

    if let Err(e) = peer_store.init().await {
        tracing::error!(?e, "failed to initialize peer store");
        return Err(anyhow::anyhow!("peer store initialization failed: {}", e));
    }
    // load accumulated uptime from previous run

    debug!(
        bind = args.bind,
        "tracker-server started"
    );

    // 启动时加载 trackers
    let trackers_urls_env = std::env::var("TRACKERS_URLS").ok();
    let trackers_url_single = std::env::var("TRACKERS_URL").unwrap_or_else(|_| "https://raw.githubusercontent.com/adysec/tracker/main/trackers_best.txt".to_string());
    let trackers_urls: Vec<String> = if let Some(s) = trackers_urls_env { s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect() } else { vec![trackers_url_single.clone()] };
    let attempts = std::env::var("TRACKERS_DOWNLOAD_ATTEMPTS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(3);
    let trackers_file = std::env::var("TRACKERS_FILE").unwrap_or_else(|_| "trackers.txt".to_string());
    eprintln!("[tracker-server] 正在加载 trackers 列表...");
    let _ = download_and_write_trackers_multi(&trackers_urls, &trackers_file, attempts).await;
    let (trackers_http_init, trackers_udp_init, trackers_ws_init) = load_trackers_multi(&trackers_file).await.unwrap_or_default();
    eprintln!("[tracker-server] trackers 加载完成，HTTP:{} UDP:{} WS:{}", trackers_http_init.len(), trackers_udp_init.len(), trackers_ws_init.len());

    let poll_secs = std::env::var("POLL_INTERVAL_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(60);
    let strategy = std::env::var("UPSTREAM_STRATEGY").unwrap_or_else(|_| "round_robin".to_string());
    let upstream_strategy = match strategy.to_lowercase().as_str() { "all" => UpstreamStrategy::All, "random" => UpstreamStrategy::Random, _ => UpstreamStrategy::RoundRobin };
    let batch_size = std::env::var("UPSTREAM_BATCH_SIZE").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let fast_mode = match std::env::var("FAST_ANNOUNCE_MODE").unwrap_or_else(|_| "timeout".to_string()).to_lowercase().as_str() { "wait" => FastMode::Wait, "async" => FastMode::Async, "off" => FastMode::Off, _ => FastMode::Timeout };
    let timeout = std::env::var("UPSTREAM_TIMEOUT_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(10);
    let fast_max_wait = std::env::var("FAST_ANNOUNCE_MAX_WAIT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(2500);
    let agg_max_concurrency = std::env::var("AGG_MAX_CONCURRENCY").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(trackers_http_init.len().max(50));
    // upstream shards for smoothing upstream requests (per-infohash sharding)
    let upstream_shards = std::env::var("UPSTREAM_SHARDS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(16).max(1);
    // DHT configuration is hard-coded; environment variables are ignored
    let dht_enable = true;
    let dht_bootstrap: Vec<String> = vec![
        "router.bittorrent.com:6881".to_string(),
        "dht.transmissionbt.com:6881".to_string(),
        "router.utorrent.com:6881".to_string(),
    ];
    let dht_timeout = Duration::from_secs(3);
    let dht_max_nodes = 64;
    let dht_random_queries = 1;
    let dht_listen_port = Some(6881);
    let pex_enable = std::env::var("PEX_ENABLE").map(|v| v == "1").unwrap_or(false);
    let pex_timeout = Duration::from_secs(
        std::env::var("PEX_TIMEOUT_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(3).max(1),
    );
    let pex_max_peers_per_hash = std::env::var("PEX_MAX_PEERS_PER_HASH")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8)
        .max(2);
    let mut upstream_senders_vec: Vec<mpsc::Sender<UpstreamJob>> = Vec::with_capacity(upstream_shards);
    let mut upstream_receivers: Vec<mpsc::Receiver<UpstreamJob>> = Vec::with_capacity(upstream_shards);
    let mut upstream_inflight_vec: Vec<Arc<Mutex<HashMap<String, Vec<oneshot::Sender<()>>>>>> = Vec::with_capacity(upstream_shards);
    for _ in 0..upstream_shards {
        let (tx, rx) = mpsc::channel::<UpstreamJob>(1024);
        upstream_senders_vec.push(tx);
        upstream_receivers.push(rx);
        upstream_inflight_vec.push(Arc::new(Mutex::new(HashMap::new())));
    }

    let state = AppState {
        store: peer_store,
        interval: args.interval,
        min_interval: args.min_interval,
        trackers_http: Arc::new(RwLock::new(trackers_http_init)),
        trackers_udp: Arc::new(RwLock::new(trackers_udp_init)),
        trackers_ws: Arc::new(RwLock::new(trackers_ws_init)),
        poll_interval: Duration::from_secs(poll_secs),
        upstream_strategy,
        batch_size,
        rr_index: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        timeout: Duration::from_secs(timeout),
        fast_mode,
        fast_max_wait: Duration::from_millis(fast_max_wait),
        semaphore: Arc::new(tokio::sync::Semaphore::new(agg_max_concurrency.max(1))),
        upstream_senders: Arc::new(upstream_senders_vec),
        upstream_shards,
        upstream_inflight: Arc::new(upstream_inflight_vec),
        app_started_at: std::time::Instant::now(),
        announce_queries: Arc::new(AtomicU64::new(0)),
        accumulated_uptime: Arc::new(AtomicU64::new(0)),
        dht_enable,
        dht_bootstrap: Arc::new(dht_bootstrap),
        dht_timeout,
        dht_max_nodes,
        dht_random_queries,
        dht_listen_port,
        pex_enable,
        pex_timeout,
        pex_max_peers_per_hash,
    };

    // load accumulated uptime from previous run
    if let Ok(prev) = state.store.get_accumulated_uptime().await {
        state.accumulated_uptime.store(prev, Ordering::Relaxed);
    }

    // poll loop 按需刷新
    {
        let state_clone = state.clone();
        tokio::spawn(async move { poll_loop(state_clone).await; });
    }

    // optional DHT listener for announce_peer crawl
    if state.dht_enable {
        if let Some(port) = state.dht_listen_port {
            let st2 = state.clone();
            tokio::spawn(async move {
                if let Err(e) = run_dht_listener(st2, port).await {
                    tracing::error!(?e, "dht listener error");
                }
            });
        } else {
            let st2 = state.clone();
            tokio::spawn(async move {
                // choose default 6881 if not specified
                if let Err(e) = run_dht_listener(st2, 6881).await {
                    tracing::error!(?e, "dht listener error");
                }
            });
        }
    }

    // 每日刷新 trackers 列表
    {
        let state_reload = state.clone();
        let trackers_urls_clone = trackers_urls.clone();
        tokio::spawn(async move {
            let ok_wait = std::env::var("TRACKERS_REFRESH_OK_SECS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(24*3600);
            let fail_wait = std::env::var("TRACKERS_REFRESH_FAIL_SECS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(3600);
            let attempts = std::env::var("TRACKERS_DOWNLOAD_ATTEMPTS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(3);
            loop {
                let result = download_and_write_trackers_multi(&trackers_urls_clone, &trackers_file, attempts).await;
                let mut success = false;
                match result {
                    Ok(()) => {
                        match load_trackers_multi(&trackers_file).await {
                            Ok((http, udp, ws)) => {
                                { let mut w = state_reload.trackers_http.write().await; *w = http; }
                                { let mut w = state_reload.trackers_udp.write().await; *w = udp; }
                                { let mut w = state_reload.trackers_ws.write().await; *w = ws; }
                                debug!("trackers list updated from remote");
                                success = true;
                            }
                            Err(e) => wlog!(?e, "reload trackers from file failed"),
                        }
                    }
                    Err(e) => { wlog!(?e, urls=?trackers_urls_clone, "daily trackers download failed"); }
                }
                let sleep_secs = if success { ok_wait } else { fail_wait };
                tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            }
        });
    }

    let app = Router::new()
        .route("/", get(index_dashboard))
        .route("/announce", get(announce))
        .with_state(state);

    debug!(%addr, "starting HTTP server");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind listener")?;

    let bound_addr = listener.local_addr().unwrap_or(addr);
    if bound_addr.ip().is_unspecified() {
        let dashboard = format!("http://127.0.0.1:{}/", bound_addr.port());
        let announce_url = format!("http://127.0.0.1:{}/announce", bound_addr.port());
        info!(%dashboard, announce=%announce_url, bind=%bound_addr, "tracker-server is ready");
    } else {
        let dashboard = format!("http://{}/", bound_addr);
        let announce_url = format!("http://{}/announce", bound_addr);
        info!(%dashboard, announce=%announce_url, bind=%bound_addr, "tracker-server is ready");
    }
    
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .context("server error")?;

    Ok(())
}

fn failure(reason: &str) -> Response {
    let mut body = Vec::with_capacity(32 + reason.len());
    body.extend_from_slice(b"d14:failure reason");
    body.extend_from_slice(reason.len().to_string().as_bytes());
    body.push(b':');
    body.extend_from_slice(reason.as_bytes());
    body.extend_from_slice(b"e");
    Response::builder()
        .header("Content-Type", "text/plain")
        .body(Bytes::from(body).into())
        .unwrap()
}

async fn announce(
    State(state): State<AppState>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
    OriginalUri(orig): OriginalUri,
    Query(q): Query<AnnounceQuery>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    state.announce_queries.fetch_add(1, Ordering::Relaxed);

    // Try to extract raw percent-encoded values from the original URI first
    let raw_info = orig
        .path_and_query()
        .and_then(|pq| pq.query())
        .and_then(|qs| qs.split('&').find_map(|pair| pair.split_once('=').and_then(|(k, v)| if k == "info_hash" { Some(v.to_string()) } else { None })));

    let raw_ih = orig
        .path_and_query()
        .and_then(|pq| pq.query())
        .and_then(|qs| qs.split('&').find_map(|pair| pair.split_once('=').and_then(|(k, v)| if k == "ih" { Some(v.to_string()) } else { None })));

    let info_hash_raw = match parse_info_hash(&raw_info.or_else(|| q.info_hash.clone()), &raw_ih.or_else(|| q.ih.clone())) {
        Ok(v) => v,
        Err(e) => {
            debug!(?e, "info_hash parse failed");
            return Ok(failure("bad info_hash format"));
        }
    };
    
    if info_hash_raw.len() != 20 {
        return Ok(failure("info_hash must be 20 bytes"));
    }
    
    let info_hash_hex = hex::encode(&info_hash_raw);

    let peer_id_raw = match &q.peer_id {
        Some(s) => percent_decode_str(s).collect::<Vec<u8>>(),
        None => return Ok(failure("missing peer_id")),
    };
    
    if peer_id_raw.is_empty() {
        return Ok(failure("bad peer_id length"));
    }

    let port = match q.port {
        Some(p) if p > 0 => p,
        _ => return Ok(failure("missing or invalid port")),
    };

    // 获取客户端真实 IP（支持 CDN/代理场景）
    // 优先级：
    // 1. 查询参数 ?ip=
    // 2. CF-Connecting-IP (Cloudflare) - 支持多种大小写变体
    // 3. X-Forwarded-For (标准代理头，取第一个)
    // 4. X-Real-IP (Nginx/备选)
    // 5. TCP 连接 IP (remote.ip())
    
    // Helper function: 大小写不敏感获取头部值
    let get_header = |name: &str| -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    };
    
    let client_ip_string = q
        .ip
        .clone()
        .or_else(|| {
            // Cloudflare: 尝试多种可能的大小写形式
            get_header("CF-Connecting-IP")
                .or_else(|| get_header("cf-connecting-ip"))
                .or_else(|| get_header("Cf-Connecting-Ip"))
        })
        .or_else(|| {
            // X-Forwarded-For: 取第一个 IP（最原始的客户端）
            get_header("X-Forwarded-For")
                .or_else(|| get_header("x-forwarded-for"))
                .and_then(|s| s.split(',').next().map(|ip| ip.trim().to_string()))
        })
        .or_else(|| {
            // X-Real-IP
            get_header("X-Real-IP")
                .or_else(|| get_header("x-real-ip"))
        })
        .unwrap_or_else(|| remote.ip().to_string());
    let client_ip = client_ip_string.as_str();

    let mut left = q.left;
    // normalize event to lowercase for matching
    let ev_lower = q.event.as_deref().map(|s| s.to_ascii_lowercase());
    if ev_lower.as_deref() == Some("completed") {
        left = Some(0);
        info!(ih=%info_hash_hex, "completed event received");
        if let Err(e) = state.store.increment_completed(&info_hash_hex).await {
            debug!(?e, ih=%info_hash_hex, "failed to increment completed counter");
        }
    }

    if ev_lower.as_deref() == Some("stopped") {
        if let Err(e) = state.store.remove_peer(&info_hash_hex, client_ip, port).await {
            debug!(?e, ih=%info_hash_hex, "failed to remove peer on stopped");
        }
        info!(ih=%info_hash_hex, ip=client_ip, port, "peer stopped");
    } else {
        // 过滤 localhost IP
        if client_ip != "127.0.0.1" {
            if let Err(e) = state
                .store
                .upsert_peer(
                    &info_hash_hex,
                    client_ip,
                    port,
                    left,
                )
                .await
            {
                debug!(?e, ih=%info_hash_hex, "failed to upsert peer");
            }
        }

        // 无需单独注册 info_hash
    }

    let numwant = q.numwant.unwrap_or(50).min(200);
    let initial_peers = state.store.list_peers(&info_hash_hex, 3).await.unwrap_or_default();
    let effective_fast_mode = if initial_peers.is_empty() && matches!(state.fast_mode, FastMode::Timeout) { FastMode::Wait } else { state.fast_mode };

    // 上游聚合（Fast 模式）
    let upstream_numwant_env = std::env::var("UPSTREAM_NUMWANT").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(200);
    let upstream_left_env = std::env::var("UPSTREAM_LEFT").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(16384);
    let peer_id_encoded_incoming = encode_lowercase(&peer_id_raw);
    let announce_params = UpstreamAnnounceParams {
        peer_id_encoded: if peer_id_raw.len()==20 { peer_id_encoded_incoming.clone() } else { (*PEER_ID_ENC).clone() },
        port,
        left: left.unwrap_or(upstream_left_env),
        event: q.event.clone().unwrap_or_else(|| "started".to_string()),
        numwant: upstream_numwant_env.min(numwant as u32),
    };

    match effective_fast_mode {
        FastMode::Off => {}
        FastMode::Async => {
            let st = state.clone();
            let ih_hex = info_hash_hex.clone();
            let ih_raw = info_hash_raw.clone();
            let params = announce_params.clone();
            // enqueue job into upstream shard to smooth requests; fallback to spawn if queue full
            let shard = shard_for(&ih_raw, st.upstream_shards);
            if let Some(sender) = st.upstream_senders.get(shard) {
                        // dedupe: set inflight marker before sending
                        if let Some(map) = st.upstream_inflight.get(shard) {
                            let mut m = map.lock().await;
                            if m.contains_key(&ih_hex) {
                                // already inflight, skip enqueue
                            } else {
                                m.insert(ih_hex.clone(), Vec::new());
                                match sender.try_send(UpstreamJob { ih_hex: ih_hex.clone(), ih_raw: ih_raw.clone(), params: params.clone() }) {
                                    Ok(_) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(job)) => {
                                        // queue full: remove inflight marker and fallback to spawn directly
                                        let _ = m.remove(&job.ih_hex);
                                        let st2 = st.clone(); let ih2 = job.ih_hex.clone(); let raw2 = job.ih_raw.clone(); let p2 = job.params.clone();
                                        tokio::spawn(async move {
                                            let http = st2.trackers_http.read().await.clone();
                                            let subset = select_trackers(&http, st2.upstream_strategy, st2.batch_size.max(5), &st2.rr_index);
                                            if subset.is_empty() { return; }
                                            if let Err(e) = fetch_from_upstreams(&st2, &subset, &ih2, &raw2, &p2).await { wlog!(?e, ih=%ih2, "fast async upstream failed (fallback)"); }
                                        });
                                    }
                                    Err(_) => { /* ignore other send errors */ }
                                }
                            }
                        }
            }
        }
        FastMode::Wait => {
            // dedupe: if another fetch for same infohash is inflight, wait for it; otherwise run and notify waiters
            let shard = shard_for(&info_hash_raw, state.upstream_shards);
            let mut should_run = false;
            let rx_opt = if let Some(map) = state.upstream_inflight.get(shard) {
                let mut m = map.lock().await;
                if m.contains_key(&info_hash_hex) {
                    // already inflight: register waiter
                    let (tx, rx) = oneshot::channel();
                    if let Some(vec) = m.get_mut(&info_hash_hex) { vec.push(tx); }
                    Some(rx)
                } else {
                    // mark inflight and run
                    m.insert(info_hash_hex.clone(), Vec::new());
                    should_run = true;
                    None
                }
            } else { None };

            if let Some(rx) = rx_opt {
                // wait until the inflight fetch completes (no extra timeout here)
                let _ = rx.await;
            } else if should_run {
                let http = state.trackers_http.read().await.clone();
                let subset = select_trackers(&http, state.upstream_strategy, state.batch_size.max(5), &state.rr_index);
                if !subset.is_empty() {
                    let started = std::time::Instant::now();
                    match fetch_from_upstreams(&state, &subset, &info_hash_hex, &info_hash_raw, &announce_params).await {
                        Ok(sum) => debug!(ih=%info_hash_hex, attempted=sum.attempted, trackers_ok=sum.ok, peers_added=sum.peers_added, elapsed_ms=started.elapsed().as_millis(), "fast wait fetched"),
                        Err(e) => wlog!(?e, ih=%info_hash_hex, "fast wait upstream error"),
                    }
                }
                // notify waiters and clear inflight
                if let Some(map) = state.upstream_inflight.get(shard) {
                    let mut m = map.lock().await;
                    if let Some(waiters) = m.remove(&info_hash_hex) {
                        for tx in waiters { let _ = tx.send(()); }
                    }
                }
            }
        }
        FastMode::Timeout => {
            // dedupe with timeout: if inflight exists, wait with timeout; otherwise run with timeout
            let shard = shard_for(&info_hash_raw, state.upstream_shards);
            let mut should_run = false;
            let rx_opt = if let Some(map) = state.upstream_inflight.get(shard) {
                let mut m = map.lock().await;
                if m.contains_key(&info_hash_hex) {
                    let (tx, rx) = oneshot::channel();
                    if let Some(vec) = m.get_mut(&info_hash_hex) { vec.push(tx); }
                    Some(rx)
                } else {
                    m.insert(info_hash_hex.clone(), Vec::new());
                    should_run = true;
                    None
                }
            } else { None };

            if let Some(rx) = rx_opt {
                // wait with timeout
                match tokio::time::timeout(state.fast_max_wait, rx).await {
                    Ok(_) => { /* done by other fetch */ }
                    Err(_) => { wlog!(ih=%info_hash_hex, waited_ms=state.fast_max_wait.as_millis(), "fast timeout reached (waiting for inflight)"); }
                }
            } else if should_run {
                let http = state.trackers_http.read().await.clone();
                let subset = select_trackers(&http, state.upstream_strategy, state.batch_size.max(5), &state.rr_index);
                if !subset.is_empty() {
                    let started = std::time::Instant::now();
                    let fut = fetch_from_upstreams(&state, &subset, &info_hash_hex, &info_hash_raw, &announce_params);
                    match tokio::time::timeout(state.fast_max_wait, fut).await {
                        Ok(Ok(sum)) => debug!(ih=%info_hash_hex, attempted=sum.attempted, trackers_ok=sum.ok, peers_added=sum.peers_added, elapsed_ms=started.elapsed().as_millis(), "fast timeout fetched"),
                        Ok(Err(e)) => wlog!(?e, ih=%info_hash_hex, "fast timeout upstream error"),
                        Err(_) => wlog!(ih=%info_hash_hex, waited_ms=state.fast_max_wait.as_millis(), "fast timeout reached"),
                    }
                }
                // notify waiters and clear inflight
                if let Some(map) = state.upstream_inflight.get(shard) {
                    let mut m = map.lock().await;
                    if let Some(waiters) = m.remove(&info_hash_hex) {
                        for tx in waiters { let _ = tx.send(()); }
                    }
                }
            }
        }
    }

    // maintain existing announce behavior; using store list as before
    let mut peers = state.store.list_peers(&info_hash_hex, numwant).await.unwrap_or_default();
    if peers.len() < numwant/2 {
        let http = state.trackers_http.read().await.clone();
        let udp = state.trackers_udp.read().await.clone();
        let ws = state.trackers_ws.read().await.clone();
        let params_full = UpstreamAnnounceParams { peer_id_encoded: peer_id_encoded_incoming, port, left: left.unwrap_or(0), event: q.event.clone().unwrap_or_else(|| "started".to_string()), numwant: numwant as u32 };
        let st = state.clone();
        let ih_hex = info_hash_hex.clone();
        let ih_raw = info_hash_raw.clone();
        let tmo = state.timeout;
        let max_wait = std::env::var("ON_DEMAND_MAX_WAIT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(4000);
        let fut = async move { let _ = aggregated_fetch(&st, &ih_hex, &ih_raw, &params_full, &http, &udp, &ws, tmo).await; };
        let _ = tokio::time::timeout(Duration::from_millis(max_wait), fut).await;
        peers = state.store.list_peers(&info_hash_hex, numwant).await.unwrap_or_default();
    }

    let (complete, incomplete) = state.store.peer_stats(&info_hash_hex).await.unwrap_or((0, peers.len()));

    let compact_flag = q.compact.unwrap_or(1) == 1;
    // no_peer_id 参数在当前实现中无效（不返回 peer id）

    info!(
        ih=%info_hash_hex,
        peers_out=peers.len(),
        compact=?compact_flag,
        "announce served"
    );

    let mut body = Vec::new();
    
    body.extend_from_slice(b"d8:intervali");
    body.extend_from_slice(state.interval.to_string().as_bytes());
    body.extend_from_slice(b"e");
    
    body.extend_from_slice(b"12:min intervali");
    body.extend_from_slice(state.min_interval.to_string().as_bytes());
    body.extend_from_slice(b"e");
    
    body.extend_from_slice(b"10:tracker id");
    body.extend_from_slice(TRACKER_ID.len().to_string().as_bytes());
    body.push(b':');
    body.extend_from_slice(TRACKER_ID.as_bytes());
    
    body.extend_from_slice(b"8:completei");
    body.extend_from_slice(complete.to_string().as_bytes());
    body.extend_from_slice(b"e");
    
    body.extend_from_slice(b"10:incompletei");
    body.extend_from_slice(incomplete.to_string().as_bytes());
    body.extend_from_slice(b"e");
    
    body.extend_from_slice(b"5:peers");
    
    if compact_flag {
        let mut blob = Vec::with_capacity(peers.len() * 6);
        for peer in &peers {
            if let Ok(addr) = peer.ip.parse::<std::net::IpAddr>() {
                if let std::net::IpAddr::V4(v4) = addr {
                    blob.extend_from_slice(&v4.octets());
                    blob.extend_from_slice(&peer.port.to_be_bytes());
                }
            }
        }
        body.extend_from_slice(blob.len().to_string().as_bytes());
        body.push(b':');
        body.extend_from_slice(&blob);
    } else {
        body.push(b'l');
        for peer in &peers {
            body.push(b'd');
            
            body.extend_from_slice(b"2:ip");
            body.extend_from_slice(peer.ip.len().to_string().as_bytes());
            body.push(b':');
            body.extend_from_slice(peer.ip.as_bytes());
            
            body.extend_from_slice(b"4:porti");
            body.extend_from_slice(peer.port.to_string().as_bytes());
            body.push(b'e');
            
            // peer_id 不再存储；dictionary 模式不返回 peer id
            
            body.push(b'e');
        }
        body.push(b'e');
    }
    
    body.push(b'e');

    Ok(Response::builder()
        .header("Content-Type", "text/plain")
        .body(Bytes::from(body).into())
        .unwrap())
}

async fn index_dashboard(
    State(state): State<AppState>,
    Query(q): Query<DashboardQuery>,
) -> Html<String> {
    let snapshot = build_dashboard_snapshot(&state, q.search.as_deref()).await;
    Html(render_dashboard_html(&snapshot, q.search.as_deref()))
}

async fn build_dashboard_snapshot(state: &AppState, search: Option<&str>) -> DashboardSnapshot {
    let max_scan = std::env::var("DASHBOARD_SCAN_INFOHASHES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5000)
        .max(100);
    let top_limit = std::env::var("DASHBOARD_TOP_LIMIT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100)
        .max(10);

    let mut all = state.store.list_torrent_stats(max_scan).await.unwrap_or_default();

    let mut peers_total = 0usize;
    let mut views = Vec::with_capacity(all.len());
    for item in all.drain(..) {
        peers_total += item.total_peers;
        let filename = format!("Torrent_{}.torrent", &item.info_hash[..item.info_hash.len().min(16)]);
        views.push(TorrentView {
            filename,
            info_hash: item.info_hash,
            seeders: item.seeders,
            leechers: item.leechers,
            completed: item.completed,
            total_peers: item.total_peers,
            last_seen: item.last_seen,
        });
    }

    if let Some(s) = search.map(|x| x.trim().to_ascii_lowercase()).filter(|x| !x.is_empty()) {
        views.retain(|v| {
            v.info_hash.to_ascii_lowercase().contains(&s)
                || v.filename.to_ascii_lowercase().contains(&s)
        });
    }

    views.sort_by(|a, b| b.total_peers.cmp(&a.total_peers));
    if views.len() > top_limit {
        views.truncate(top_limit);
    }

    let base = state.accumulated_uptime.load(Ordering::Relaxed);
    let up = base + state.app_started_at.elapsed().as_secs();
    DashboardSnapshot {
        torrents: state.store.count_infohashes().await.unwrap_or(0),
        peers: peers_total,
        queries: state.store.count_announces().await.unwrap_or(0),
        uptime: format_uptime(Duration::from_secs(up)),
        top: views,
    }
}

fn render_dashboard_html(snapshot: &DashboardSnapshot, search: Option<&str>) -> String {
    let mut rows_html = String::new();
    for t in &snapshot.top {
        rows_html.push_str(&format!(
            "<article class=\"torrent-item\"><h3>📦 {}</h3><div class=\"chips\"><span class=\"chip chip-seed\">🌱 {} Seeds</span><span class=\"chip chip-leech\">📥 {} Leechers</span><span class=\"chip chip-comp\">✅ {} Completed</span></div><div class=\"hash\">Hash: {}</div><div class=\"meta\">Total Peers: {} · Last Seen: {}</div></article>",
            escape_html(&t.filename),
            t.seeders,
            t.leechers,
            t.completed,
            escape_html(&t.info_hash),
            t.total_peers,
            escape_html(&fmt_ts(t.last_seen))
        ));
    }

    let search_val = search.unwrap_or("");
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><title>Tracker Observatory</title><meta http-equiv=\"refresh\" content=\"30\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><style>
        :root{{--bg:#0a0f1a;--panel:#111a2b;--panel2:#17233a;--line:#253759;--txt:#dce8ff;--muted:#8ea5cf;--a:#6ea8ff;--b:#8c7bff;--ok:#2fd27a;--warn:#ffcf5c;--info:#45caff}}
        *{{box-sizing:border-box}}body{{margin:0;font-family:Inter,Segoe UI,Roboto,Arial,sans-serif;background:radial-gradient(1200px 500px at 80% -10%,rgba(110,168,255,.12),transparent),radial-gradient(900px 500px at -20% 10%,rgba(140,123,255,.15),transparent),var(--bg);color:var(--txt)}}
        .wrap{{max-width:1180px;margin:28px auto;padding:0 16px}} .hero{{padding:20px 22px;border:1px solid var(--line);border-radius:18px;background:linear-gradient(160deg,rgba(23,35,58,.95),rgba(17,26,43,.95));box-shadow:0 18px 42px rgba(0,0,0,.35)}}
        .title{{margin:0;font-size:30px;letter-spacing:.4px}} .subtitle{{margin-top:8px;color:var(--muted);font-size:14px}}
        .stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-top:18px}} .stat{{border:1px solid var(--line);border-radius:14px;padding:14px;background:linear-gradient(180deg,var(--panel2),var(--panel))}}
        .k{{font-size:34px;font-weight:800;line-height:1;background:linear-gradient(90deg,var(--a),var(--info));-webkit-background-clip:text;background-clip:text;color:transparent}} .label{{margin-top:8px;color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.1em}}
        .section{{margin-top:22px;border:1px solid var(--line);border-radius:16px;background:rgba(17,26,43,.85);padding:16px}} .section h2{{margin:0 0 12px 0;font-size:18px}}
        .search{{display:flex;gap:10px;flex-wrap:wrap}} .search input{{flex:1 1 320px;background:#0e1626;color:var(--txt);border:1px solid var(--line);border-radius:12px;padding:12px 14px;font-size:15px;outline:none}} .search input:focus{{border-color:var(--a);box-shadow:0 0 0 3px rgba(110,168,255,.15)}}
        .search button{{border:0;border-radius:12px;padding:12px 18px;background:linear-gradient(90deg,var(--a),var(--b));color:#081120;font-weight:700;cursor:pointer}}
        .torrent-item{{border:1px solid var(--line);border-radius:14px;padding:14px;margin:0 0 10px 0;background:linear-gradient(180deg,#121d30,#0f1728)}} .torrent-item h3{{margin:0 0 10px 0;color:#b9d5ff;font-size:17px}}
        .chips{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}} .chip{{padding:4px 10px;border-radius:999px;font-size:12px;font-weight:700}} .chip-seed{{background:rgba(47,210,122,.2);color:#7ff0ae;border:1px solid rgba(47,210,122,.4)}} .chip-leech{{background:rgba(255,207,92,.2);color:#ffe39a;border:1px solid rgba(255,207,92,.4)}} .chip-comp{{background:rgba(69,202,255,.18);color:#a9ebff;border:1px solid rgba(69,202,255,.4)}}
        .hash{{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:var(--muted);word-break:break-all}} .meta{{margin-top:8px;color:var(--muted);font-size:13px}}
        .footer{{margin-top:8px;text-align:center;color:var(--muted);font-size:13px}}
        </style></head><body><div class=\"wrap\"><header class=\"hero\"><h1 class=\"title\">Tracker Observatory</h1><div class=\"subtitle\">A custom dark dashboard for live swarm visibility</div>
        <div class=\"stats\">    
            <div class=\"stat\"><div class=\"k\">{}</div><div class=\"label\">Torrents</div></div>
            <div class=\"stat\"><div class=\"k\">{}</div><div class=\"label\">Peers</div></div>
            <div class=\"stat\"><div class=\"k\">{}</div><div class=\"label\">Queries</div></div>
            <div class=\"stat\"><div class=\"k\">{}</div><div class=\"label\">Total Uptime</div></div>
            </div></header>
        <section class=\"section\"><h2>🔎 Search Torrents</h2><form class=\"search\" method=\"get\"><input type=\"text\" name=\"search\" placeholder=\"Search by filename or hash...\" value=\"{}\"><button type=\"submit\">Search</button></form></section>
        <section class=\"section\"><h2>🌍 Active Torrents (Top 100)</h2>{}</section>
        <div class=\"footer\">Auto-refresh: 30s</div></div></body></html>",
        snapshot.torrents,
        snapshot.peers,
        snapshot.queries,
        escape_html(&snapshot.uptime),
        escape_html(search_val),
        rows_html
    )
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn fmt_ts(ts: i64) -> String {
    let Ok(dt) = OffsetDateTime::from_unix_timestamp(ts) else {
        return ts.to_string();
    };
    let offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    dt.to_offset(offset)
        .format(TS_FMT)
        .unwrap_or_else(|_| ts.to_string())
}

fn format_uptime(d: Duration) -> String {
    let s = d.as_secs();
    let h = s / 3600;
    let m = (s % 3600) / 60;
    if h > 0 {
        format!("{}h {}m", h, m)
    } else {
        format!("{}m", m)
    }
}

// 移除 /peers 非标准端点

fn parse_info_hash(
    info_hash: &Option<String>,
    ih: &Option<String>,
) -> Result<Vec<u8>, String> {
    use data_encoding::BASE32;

    // Accept multiple aliases / formats:
    // 1. Raw 20-byte URL percent-encoded (spec)
    // 2. 40-char hex (lower/upper)
    // 3. base32 (32 chars) as seen in some magnet URNs
    // 4. urn:btih:<hex|base32>
    // 5. Fallback: try percent-decoding then hex/base32 again
    let raw_original = info_hash
        .as_ref()
        .or(ih.as_ref())
        .ok_or("missing info_hash")?
        .trim();

    // Strip common prefix
    let raw = raw_original.strip_prefix("urn:btih:").unwrap_or(raw_original);

    // Try hex first (lower or upper). Length must be 40.
    if raw.len() == 40 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
        match hex::decode(raw) {
            Ok(bytes) if bytes.len() == 20 => return Ok(bytes),
            Ok(_) => return Err("hex decoded length != 20".to_string()),
            Err(e) => return Err(format!("hex decode error: {e}")),
        }
    }

    // Try base32 (magnet sometimes uses upper-case; BASE32 is case-insensitive after upper())
    if raw.len() == 32 && raw.chars().all(|c| c.is_ascii_alphanumeric()) {
        let upper = raw.to_ascii_uppercase();
        match BASE32.decode(upper.as_bytes()) {
            Ok(bytes) if bytes.len() == 20 => return Ok(bytes),
            Ok(_) => return Err("base32 decoded length != 20".to_string()),
            Err(_e) => { /* ignore and continue */ }
        }
    }

    // Percent-decode (spec form)
    let decoded = percent_decode_str(raw).collect::<Vec<u8>>();
    if decoded.len() == 20 {
        return Ok(decoded);
    }

    // Sometimes clients accidentally double-encode; try one more decode pass if '%' present.
    if raw.contains('%') {
        let once = percent_decode_str(raw).decode_utf8_lossy();
        let second = percent_decode_str(&once).collect::<Vec<u8>>();
        if second.len() == 20 { return Ok(second); }
    }

    Err(format!("invalid info_hash format: raw='{}' len={} decoded_len={}", raw_original, raw_original.len(), decoded.len()))
}

// =============== 上游抓取与工具函数 =================

#[derive(Clone)]
struct UpstreamAnnounceParams {
    peer_id_encoded: String,
    port: u16,
    left: u64,
    event: String,
    numwant: u32,
}

fn encode_lowercase(bytes: &[u8]) -> String { let mut out = String::with_capacity(bytes.len()*3); for b in bytes { out.push('%'); out.push(hex_char(b >> 4)); out.push(hex_char(b & 0x0f)); } out }
fn hex_char(n: u8) -> char { match n { 0..=9 => (b'0'+n) as char, 10..=15 => (b'a'+(n-10)) as char, _ => '?' } }

struct FetchSummary { attempted: usize, ok: usize, peers_added: usize }

// TLS 回退客户端
static NATIVE_TLS_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .user_agent("AdySec-Tracker-NativeTLS")
        .timeout(Duration::from_secs(15))
        .build()
        .expect("build native-tls client")
});

fn select_trackers(list: &[String], strat: UpstreamStrategy, batch: usize, rr_index: &Arc<std::sync::atomic::AtomicUsize>) -> Vec<String> {
    if list.is_empty() { return Vec::new(); }
    if *LOAD_BALANCE_ALL {
        let start = rr_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % list.len();
        let mut out = Vec::with_capacity(list.len());
        for i in 0..list.len() { out.push(list[(start + i) % list.len()].clone()); }
        return out;
    }
    match strat {
        UpstreamStrategy::All => list.to_vec(),
        UpstreamStrategy::Random => { use rand::seq::SliceRandom; let mut rng = rand::thread_rng(); let mut v = list.to_vec(); v.shuffle(&mut rng); v.truncate(batch.min(v.len())); v },
        UpstreamStrategy::RoundRobin => { let start = rr_index.fetch_add(batch, std::sync::atomic::Ordering::Relaxed) % list.len().max(1); let mut out = Vec::new(); for i in 0..batch.min(list.len()) { out.push(list[(start + i) % list.len()].clone()); } out }
    }
}

fn shard_for(data: &[u8], shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    hasher.write(data);
    (hasher.finish() as usize) % shards
}

async fn fetch_from_upstreams(state: &AppState, trackers: &[String], ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams) -> Result<FetchSummary> {
    let client = reqwest::Client::builder().timeout(state.timeout).user_agent("AdySec-Tracker").build()?;
    let ih_encoded = encode_lowercase(ih_raw);
    let mut handles = Vec::new();
    let mut attempted = 0usize;
    for t in trackers.iter() { attempted += 1;
        let _permit = state.semaphore.clone().acquire_owned().await.ok();
        let Ok(mut url) = reqwest::Url::parse(t) else { continue };
        if !url.path().ends_with("announce") { let mut path = url.path().to_string(); if !path.ends_with('/') { path.push('/'); } path.push_str("announce"); url.set_path(&path); }
        let key = format!("{:08X}", rand::thread_rng().gen::<u32>());
        url.set_query(Some(&format!(
            "info_hash={}&peer_id={}&port={}&uploaded=0&downloaded=0&left={}&event={}&numwant={}&compact=1&no_peer_id=1&supportcrypto=1&redundant=0&key={}",
            ih_encoded, params.peer_id_encoded, params.port, params.left, params.event, params.numwant, key
        )));
        let st = state.clone(); let ih = ih_hex.to_string(); let u = url.clone(); let client = client.clone();
        let handle = tokio::spawn(async move {
            let first = client.get(u.clone()).send().await;
            let result = match first {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.bytes().await {
                            Ok(bytes) => {
                                if bytes.windows(7).any(|w| w == b"7:failure") {
                                    let snippet = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
                                    wlog!(tracker=%u, body=%snippet, "tracker failure returned");
                                }
                                match parse_and_upsert(&st, &ih, &bytes).await {
                                    Ok(n) => { (true, n) }
                                    Err(e) => { wlog!(?e, tracker=%u, ih=%ih, "parse/upsert failed"); (false, 0) }
                                }
                            }
                            Err(e) => { wlog!(?e, tracker=%u, ih=%ih, "read body failed"); (false, 0) }
                        }
                    } else { wlog!(status=?resp.status(), tracker=%u, ih=%ih, "tracker http error"); (false, 0) }
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    let enable_native = std::env::var("ENABLE_NATIVE_TLS_FALLBACK").map(|v| v=="1").unwrap_or(true);
                    if enable_native && (err_msg.contains("tls") || err_msg.contains("handshake") || err_msg.contains("certificate")) {
                        wlog!(?e, tracker=%u, ih=%ih, "rustls failed, trying native-tls");
                        match NATIVE_TLS_CLIENT.get(u.clone()).send().await {
                            Ok(resp2) => {
                                if resp2.status().is_success() {
                                    match resp2.bytes().await {
                                        Ok(bytes) => match parse_and_upsert(&st, &ih, &bytes).await { Ok(n) => (true, n), Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "parse/upsert failed (native)" ); (false,0) } },
                                        Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "read body failed (native)" ); (false,0) }
                                    }
                                } else { wlog!(status=?resp2.status(), tracker=%u, ih=%ih, "tracker http error (native)" ); (false,0) }
                            }
                            Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "native-tls also failed" ); (false,0) }
                        }
                    } else { wlog!(?e, tracker=%u, ih=%ih, "tracker request failed"); (false, 0) }
                }
            };
            result
        });
        handles.push(handle);
    }
    let mut ok = 0usize; let mut peers_total = 0usize;
    for h in handles { if let Ok((success, added)) = h.await { if success { ok += 1; } peers_total += added; } }
    Ok(FetchSummary { attempted, ok, peers_added: peers_total })
}

async fn fetch_udp_tracker(state: &AppState, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, tracker: &str, timeout: Duration) -> Result<()> {
    use tokio::net::UdpSocket; use tokio::time::timeout as tok_timeout;
    let url = tracker.trim_start_matches("udp://");
    let (host_port, _rest) = url.split_once('/').unwrap_or((url, ""));
    let (host, port_str) = host_port.split_once(':').unwrap_or((host_port, "6969"));
    let port: u16 = port_str.parse().unwrap_or(6969);
    let addr = format!("{host}:{port}");
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    let mut buf = [0u8; 4096];
    let conn_id: i64 = 0x41727101980; let trans_id: i32 = rand::thread_rng().gen();
    let mut req = Vec::with_capacity(16); req.extend_from_slice(&conn_id.to_be_bytes()); req.extend_from_slice(&0i32.to_be_bytes()); req.extend_from_slice(&trans_id.to_be_bytes());
    sock.send_to(&req, &addr).await?;
    let (n, _) = tok_timeout(timeout, sock.recv_from(&mut buf)).await??; if n < 16 { return Ok(()); }
    let conn_new = i64::from_be_bytes(buf[8..16].try_into().unwrap());
    let peer_id_dec = percent_encoding::percent_decode_str(&params.peer_id_encoded).collect::<Vec<u8>>();
    let pid = if peer_id_dec.len()==20 { peer_id_dec } else { ih_raw.to_vec() };
    let mut announce = Vec::with_capacity(98);
    announce.extend_from_slice(&conn_new.to_be_bytes()); announce.extend_from_slice(&1i32.to_be_bytes()); announce.extend_from_slice(&trans_id.to_be_bytes()); announce.extend_from_slice(ih_raw); announce.extend_from_slice(&pid); announce.extend_from_slice(&0i64.to_be_bytes()); announce.extend_from_slice(&(params.left as i64).to_be_bytes()); announce.extend_from_slice(&0i64.to_be_bytes()); announce.extend_from_slice(&0i32.to_be_bytes()); announce.extend_from_slice(&0i32.to_be_bytes()); announce.extend_from_slice(&rand::thread_rng().gen::<i32>().to_be_bytes()); announce.extend_from_slice(&(-1i32).to_be_bytes()); announce.extend_from_slice(&(params.port as i16).to_be_bytes());
    sock.send_to(&announce, &addr).await?;
    let (n2, _) = tok_timeout(timeout, sock.recv_from(&mut buf)).await??; if n2 < 20 { return Ok(()); }
    for chunk in buf[20..n2].chunks(6) { if chunk.len()==6 { let ip = std::net::Ipv4Addr::new(chunk[0],chunk[1],chunk[2],chunk[3]); let port = u16::from_be_bytes([chunk[4],chunk[5]]); let ip_str = ip.to_string(); if ip_str != "127.0.0.1" { let _ = state.store.upsert_peer(ih_hex, &ip_str, port, None).await; } } }
    Ok(())
}

async fn fetch_ws_tracker(state: &AppState, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, tracker: &str, timeout: Duration) -> Result<()> {
    let (ws, _) = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(tracker)).await??; let (mut write, mut read) = ws.split();
    let msg = serde_json::json!({"action":"announce","info_hash":hex::encode(ih_raw),"peer_id":params.peer_id_encoded,"uploaded":0,"downloaded":0,"left":params.left,"port":params.port,"event":params.event,"numwant":params.numwant});
    write.send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string())).await?;
    if let Ok(Some(frame)) = tokio::time::timeout(timeout, read.next()).await { if let Ok(text) = frame?.to_text() { if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) { if let Some(arr) = v.get("peers").and_then(|x| x.as_array()) { for p in arr { if let (Some(ip), Some(port)) = (p.get("ip").and_then(|x| x.as_str()), p.get("port").and_then(|x| x.as_u64())) { if ip != "127.0.0.1" { let _ = state.store.upsert_peer(ih_hex, ip, port as u16, None).await; } } } } } } }
    Ok(())
}

async fn aggregated_fetch(state: &AppState, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, http_list: &[String], udp_list: &[String], ws_list: &[String], timeout: Duration) -> Result<()> {
    if !http_list.is_empty() {
        let _ = fetch_from_upstreams(state, http_list, ih_hex, ih_raw, params).await;
    }
    for u in udp_list.iter().take(50) { let _ = fetch_udp_tracker(state, ih_hex, ih_raw, params, u, timeout).await; }
    for w in ws_list.iter().take(10) { let _ = fetch_ws_tracker(state, ih_hex, ih_raw, params, w, timeout).await; }
    Ok(())
}

async fn parse_and_upsert(state: &AppState, ih_hex: &str, body: &Bytes) -> Result<usize> {
    let mut count = 0usize;
    // compact peers
    if let Some(idx) = body.windows(7).position(|w| w == b"5:peers") {
        let mut i = idx + 7; let mut len: usize = 0; while i < body.len() && body[i].is_ascii_digit() { len = len * 10 + (body[i]-b'0') as usize; i+=1; }
    if i < body.len() && body[i]==b':' { i+=1; if len>0 && i+len <= body.len() { let slice = &body[i..i+len]; if len % 6 == 0 { for chunk in slice.chunks(6) { if chunk.len()==6 { let ip = std::net::Ipv4Addr::new(chunk[0],chunk[1],chunk[2],chunk[3]); let port = u16::from_be_bytes([chunk[4],chunk[5]]); let ip_str = ip.to_string(); if ip_str != "127.0.0.1" && state.store.upsert_peer(ih_hex, &ip_str, port, None).await.is_ok() { count+=1; } } } } } }
    }
    if count == 0 { // non-compact list
        if let Some(start) = body.windows(8).position(|w| w == b"5:peersl") {
            let mut i = start + 8;
            while i < body.len() && body[i] != b'e' {
                if body[i] != b'd' { break; }
                i += 1;
                let mut ip_opt: Option<String> = None; let mut port_opt: Option<u16> = None;
                while i < body.len() && body[i] != b'e' {
                    let mut key_len = 0usize; while i < body.len() && body[i].is_ascii_digit() { key_len = key_len*10 + (body[i]-b'0') as usize; i+=1; }
                    if i >= body.len() || body[i] != b':' { break; }
                    i+=1; if i + key_len > body.len() { break; }
                    let key = &body[i..i+key_len]; i+=key_len; if i >= body.len() { break; }
                    match body[i] {
                        b'i' => { i+=1; let mut val: i64 = 0; let mut neg=false; if i<body.len() && body[i]==b'-' { neg=true; i+=1; } while i<body.len() && body[i].is_ascii_digit() { val = val*10 + (body[i]-b'0') as i64; i+=1; } if neg { val=-val; } if i<body.len() && body[i]==b'e' { i+=1; } if key==b"port" { if val>=0 && val<=u16::MAX as i64 { port_opt = Some(val as u16); } } }
                        b'l' => { i+=1; let mut depth=1; while i<body.len() && depth>0 { if body[i]==b'l' { depth+=1; } else if body[i]==b'e' { depth-=1; } i+=1; } }
                        b'd' => { i+=1; let mut depth=1; while i<body.len() && depth>0 { if body[i]==b'd' { depth+=1; } else if body[i]==b'e' { depth-=1; } i+=1; } }
                        _ if body[i].is_ascii_digit() => { let mut vlen=0usize; while i<body.len() && body[i].is_ascii_digit() { vlen = vlen*10 + (body[i]-b'0') as usize; i+=1; } if i<body.len() && body[i]==b':' { i+=1; } if i+vlen > body.len() { break; } let val_bytes=&body[i..i+vlen]; i+=vlen; if key==b"ip" { if let Ok(s)=std::str::from_utf8(val_bytes) { ip_opt = Some(s.to_string()); } } /* ignore other string keys */ }
                        _ => { break; }
                    }
                }
                if i < body.len() && body[i]==b'e' { i+=1; }
                if let (Some(ip), Some(port)) = (ip_opt, port_opt) { if ip != "127.0.0.1" { let _ = state.store.upsert_peer(ih_hex, &ip, port, None).await; count+=1; } }
            }
        }
    }
    Ok(count)
}

async fn dht_discover_peers(state: &AppState, ih_hex: &str, ih_raw: &[u8]) -> Result<usize> {
    if state.dht_bootstrap.is_empty() {
        info!(ih=%ih_hex, "dht bootstrap list empty, skipping");
        return Ok(0);
    }

    info!(ih=%ih_hex, bootstraps=state.dht_bootstrap.len(), "starting dht discovery");

    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    let mut queue = VecDeque::<SocketAddr>::new();
    let mut visited = HashSet::<SocketAddr>::new();

    for b in state.dht_bootstrap.iter() {
        if let Ok(addrs) = tokio::net::lookup_host(b).await {
            for addr in addrs {
                queue.push_back(addr);
            }
        }
    }

    let mut inserted = 0usize;
    while let Some(target) = queue.pop_front() {
        debug!(ih=%ih_hex, target=%target, visited=?visited.len(), "querying dht node");
        if visited.len() >= state.dht_max_nodes {
            break;
        }
        if !visited.insert(target) {
            continue;
        }
        match dht_get_peers_once(&socket, target, ih_raw, state.dht_timeout).await {
            Ok(resp) => {
                for (ip, port) in extract_compact_peers_from_values(&resp) {
                    if ip != "127.0.0.1" && state.store.upsert_peer(ih_hex, &ip, port, None).await.is_ok() {
                        inserted += 1;
                    }
                }

                // if we inserted peers via DHT, query upstream trackers to expand list for this infohash
                if inserted > 0 {
                    let http = state.trackers_http.read().await.clone();
                    let subset = select_trackers(&http, state.upstream_strategy, state.batch_size, &state.rr_index);
                    let params = UpstreamAnnounceParams { peer_id_encoded: (*PEER_ID_ENC).clone(), port: 6881, left: 16384, event: "started".to_string(), numwant: 100 };
                    let _ = fetch_from_upstreams(state, &subset, ih_hex, ih_raw, &params).await;
                }

                for node in extract_dht_nodes_ipv4(&resp) {
                    if !visited.contains(&node) {
                        queue.push_back(node);
                    }
                }
            }
            Err(_) => {}
        }
    }

    if inserted > 0 {
        info!(ih=%ih_hex, inserted, visited_nodes=visited.len(), "dht discovery inserted peers");
    } else {
        debug!(ih=%ih_hex, visited_nodes=visited.len(), "dht discovery inserted no peers");
    }
    Ok(inserted)
}

async fn dht_get_peers_once(
    socket: &tokio::net::UdpSocket,
    target: SocketAddr,
    ih_raw: &[u8],
    timeout: Duration,
) -> Result<Vec<u8>> {
    let txid = rand::thread_rng().gen::<u16>();
    let mut node_id = [0u8; 20];
    rand::thread_rng().fill(&mut node_id);

    let mut msg = Vec::with_capacity(128);
    msg.extend_from_slice(b"d1:ad2:id20:");
    msg.extend_from_slice(&node_id);
    msg.extend_from_slice(b"9:info_hash20:");
    msg.extend_from_slice(ih_raw);
    msg.extend_from_slice(b"e1:q9:get_peers1:t2:");
    msg.extend_from_slice(&txid.to_be_bytes());
    msg.extend_from_slice(b"1:y1:qe");

    socket.send_to(&msg, target).await?;
    let mut buf = [0u8; 4096];
    let (n, _) = tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await??;
    Ok(buf[..n].to_vec())
}

fn extract_compact_peers_from_values(body: &[u8]) -> Vec<(String, u16)> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 10 < body.len() {
        if &body[i..i + 9] == b"6:valuesl" {
            i += 9;
            while i < body.len() && body[i] != b'e' {
                let mut len = 0usize;
                while i < body.len() && body[i].is_ascii_digit() {
                    len = len * 10 + (body[i] - b'0') as usize;
                    i += 1;
                }
                if i >= body.len() || body[i] != b':' {
                    break;
                }
                i += 1;
                if i + len > body.len() {
                    break;
                }
                let bytes = &body[i..i + len];
                i += len;
                if len % 6 == 0 {
                    for chunk in bytes.chunks(6) {
                        let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]).to_string();
                        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                        out.push((ip, port));
                    }
                }
            }
        }
        i += 1;
    }
    out
}

fn extract_dht_nodes_ipv4(body: &[u8]) -> Vec<SocketAddr> {
    let mut out = Vec::new();
    if let Some(pos) = body.windows(7).position(|w| w == b"5:nodes") {
        let mut i = pos + 7;
        let mut len = 0usize;
        while i < body.len() && body[i].is_ascii_digit() {
            len = len * 10 + (body[i] - b'0') as usize;
            i += 1;
        }
        if i < body.len() && body[i] == b':' {
            i += 1;
            if i + len <= body.len() {
                let nodes = &body[i..i + len];
                for chunk in nodes.chunks(26) {
                    if chunk.len() != 26 {
                        continue;
                    }
                    let ip = std::net::Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
                    let port = u16::from_be_bytes([chunk[24], chunk[25]]);
                    out.push(SocketAddr::new(std::net::IpAddr::V4(ip), port));
                }
            }
        }
    }
    out
}

// ------------------------------------------------------------
// Minimal DHT listener used as a crawler; we answer queries and
// record announce_peer events into the peer store. We do *not*
// fetch or save torrent metadata, only info_hash and peer.

async fn run_dht_listener(state: AppState, port: u16) -> Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let socket = tokio::net::UdpSocket::bind(&addr).await?;
    info!(port, "dht listener bound");

    // our own random node id (20 bytes)
    let mut node_id = [0u8; 20];
    rand::thread_rng().fill(&mut node_id);

    let mut buf = [0u8; 4096];
    loop {
        let (n, src) = socket.recv_from(&mut buf).await?;
        let pkt = &buf[..n];

        // always try to respond with a basic DHT reply if we can extract the transaction id
        if let Some(tfield) = extract_t_field(pkt) {
            let mut resp = Vec::with_capacity(64 + tfield.len());
            resp.extend_from_slice(b"d1:rd2:id20:");
            resp.extend_from_slice(&node_id);
            resp.extend_from_slice(b"e1:t");
            resp.extend_from_slice(&tfield); // includes length, colon, and value
            resp.extend_from_slice(b"1:y1:r");
            let _ = socket.send_to(&resp, src).await;
        }

        // if this is an announce_peer query, extract info and store it
        if let Some(ih) = extract_info_hash_from_dht(pkt) {
            if let Some(port) = extract_port_from_dht(pkt) {
                let ip = extract_ip_from_dht(pkt).unwrap_or_else(|| src.ip().to_string());
                let ih_hex = hex::encode(ih);
                let _ = state.store.upsert_peer(&ih_hex, &ip, port, None).await;
                debug!(ih=%ih_hex, ip=%ip, port, "recorded announce_peer from DHT");
            }
        }
    }
}

// helpers -------------------------------------------------------

fn extract_t_field(buf: &[u8]) -> Option<Vec<u8>> {
    // find the 't' key and return the entire length:value slice after the colon
    if let Some(pos) = buf.windows(2).position(|w| w == b"1:t") {
        let mut i = pos + 2;
        // read length digits
        let mut len = 0usize;
        while i < buf.len() && buf[i].is_ascii_digit() {
            len = len * 10 + (buf[i] - b'0') as usize;
            i += 1;
        }
        if i < buf.len() && buf[i] == b':' {
            i += 1;
            if i + len <= buf.len() {
                return Some(buf[pos + 2..i + len].to_vec());
            }
        }
    }
    None
}

fn extract_info_hash_from_dht(buf: &[u8]) -> Option<&[u8]> {
    // look for the literal "9:info_hash20:" sequence
    if let Some(pos) = buf.windows(13).position(|w| w == b"9:info_hash20:") {
        let start = pos + 13;
        if start + 20 <= buf.len() {
            return Some(&buf[start..start + 20]);
        }
    }
    None
}

fn extract_port_from_dht(buf: &[u8]) -> Option<u16> {
    if let Some(pos) = buf.windows(6).position(|w| w == b"4:port") {
        let mut i = pos + 6;
        if i < buf.len() && buf[i] == b'i' {
            i += 1;
            let mut val = 0u16;
            while i < buf.len() && buf[i].is_ascii_digit() {
                val = val * 10 + (buf[i] - b'0') as u16;
                i += 1;
            }
            return Some(val);
        }
    }
    None
}

fn extract_ip_from_dht(buf: &[u8]) -> Option<String> {
    if let Some(pos) = buf.windows(3).position(|w| w == b"2:ip") {
        let mut i = pos + 3;
        if i < buf.len() && buf[i] == b':' {
            i += 1;
            let start = i;
            while i < buf.len() && (buf[i].is_ascii_digit() || buf[i] == b'.') {
                i += 1;
            }
            if start < i {
                if let Ok(s) = std::str::from_utf8(&buf[start..i]) {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

async fn pex_discover_peers(state: &AppState, ih_hex: &str, ih_raw: &[u8]) -> Result<usize> {
    let seeds = state
        .store
        .list_peers(ih_hex, state.pex_max_peers_per_hash)
        .await
        .unwrap_or_default();
    let mut inserted = 0usize;

    for peer in seeds {
        if peer.ip == "127.0.0.1" {
            continue;
        }
        if let Ok(found) = pex_from_peer(&peer.ip, peer.port, ih_raw, state.pex_timeout).await {
            for (ip, port) in found {
                if ip != "127.0.0.1" && state.store.upsert_peer(ih_hex, &ip, port, None).await.is_ok() {
                    inserted += 1;
                }
            }
        }
    }

    if inserted > 0 {
        debug!(ih=%ih_hex, inserted, "pex discovery inserted peers");
    }
    Ok(inserted)
}

async fn pex_from_peer(ip: &str, port: u16, ih_raw: &[u8], timeout: Duration) -> Result<Vec<(String, u16)>> {
    let addr = format!("{}:{}", ip, port);
    let mut stream = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await??;

    let mut hs = Vec::with_capacity(68);
    hs.push(19u8);
    hs.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10;
    hs.extend_from_slice(&reserved);
    hs.extend_from_slice(ih_raw);
    let mut pid = [0u8; 20];
    rand::thread_rng().fill(&mut pid);
    hs.extend_from_slice(&pid);
    tokio::time::timeout(timeout, stream.write_all(&hs)).await??;

    let mut hs_resp = [0u8; 68];
    tokio::time::timeout(timeout, stream.read_exact(&mut hs_resp)).await??;
    if hs_resp[0] != 19 || &hs_resp[1..20] != b"BitTorrent protocol" {
        return Ok(Vec::new());
    }

    let ext_handshake = b"d1:md6:ut_pexi1eee";
    let mut msg = Vec::with_capacity(4 + 2 + ext_handshake.len());
    msg.extend_from_slice(&((2 + ext_handshake.len()) as u32).to_be_bytes());
    msg.push(20u8);
    msg.push(0u8);
    msg.extend_from_slice(ext_handshake);
    tokio::time::timeout(timeout, stream.write_all(&msg)).await??;

    let mut ut_pex_id: Option<u8> = None;
    let mut found = Vec::new();
    for _ in 0..6 {
        let mut len_buf = [0u8; 4];
        if tokio::time::timeout(timeout, stream.read_exact(&mut len_buf)).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 1024 * 1024 {
            continue;
        }
        let mut payload = vec![0u8; len];
        tokio::time::timeout(timeout, stream.read_exact(&mut payload)).await??;
        if payload.len() < 2 || payload[0] != 20 {
            continue;
        }
        let ext_id = payload[1];
        let body = &payload[2..];
        if ext_id == 0 {
            if let Some(id) = parse_ut_pex_ext_id(body) {
                ut_pex_id = Some(id);
                let req = b"de";
                let mut ask = Vec::with_capacity(4 + 2 + req.len());
                ask.extend_from_slice(&((2 + req.len()) as u32).to_be_bytes());
                ask.push(20u8);
                ask.push(id);
                ask.extend_from_slice(req);
                let _ = tokio::time::timeout(timeout, stream.write_all(&ask)).await;
            }
            continue;
        }

        if Some(ext_id) == ut_pex_id {
            found.extend(extract_compact_from_added(body));
        }
    }
    Ok(found)
}

fn parse_ut_pex_ext_id(payload: &[u8]) -> Option<u8> {
    let pat = b"6:ut_pexi";
    let pos = payload.windows(pat.len()).position(|w| w == pat)?;
    let mut i = pos + pat.len();
    let mut val: u16 = 0;
    while i < payload.len() && payload[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((payload[i] - b'0') as u16);
        i += 1;
    }
    if i < payload.len() && payload[i] == b'e' && val <= u8::MAX as u16 {
        Some(val as u8)
    } else {
        None
    }
}

fn extract_compact_from_added(payload: &[u8]) -> Vec<(String, u16)> {
    let mut out = Vec::new();
    if let Some(pos) = payload.windows(7).position(|w| w == b"5:added") {
        let mut i = pos + 7;
        let mut len = 0usize;
        while i < payload.len() && payload[i].is_ascii_digit() {
            len = len * 10 + (payload[i] - b'0') as usize;
            i += 1;
        }
        if i < payload.len() && payload[i] == b':' {
            i += 1;
            if i + len <= payload.len() {
                let peers = &payload[i..i + len];
                if len % 6 == 0 {
                    for chunk in peers.chunks(6) {
                        let ip = std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]).to_string();
                        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                        out.push((ip, port));
                    }
                }
            }
        }
    }
    out
}

async fn poll_loop(state: AppState) {
    info!(interval=?state.poll_interval, dht_enable=state.dht_enable, pex_enable=state.pex_enable, "poll loop started");
    let st = Arc::new(state);
    loop {
        match st.store.list_all_infohashes(200).await { // heuristic limit
            Ok(list) => {
                for ih in list {
                    if let Ok(raw) = hex::decode(&ih) {
                        let st2 = st.clone();
                        tokio::spawn(async move {
                            let http = st2.trackers_http.read().await.clone();
                            let subset = select_trackers(&http, st2.upstream_strategy, st2.batch_size, &st2.rr_index);
                            let params = UpstreamAnnounceParams { peer_id_encoded: (*PEER_ID_ENC).clone(), port: 6881, left: 16384, event: "started".to_string(), numwant: 200 };
                            let _ = fetch_from_upstreams(&st2, &subset, &ih, &raw, &params).await;
                            if st2.dht_enable {
                                let _ = dht_discover_peers(&st2, &ih, &raw).await;
                            }
                            if st2.pex_enable {
                                let _ = pex_discover_peers(&st2, &ih, &raw).await;
                            }
                        });
                    }
                }
            }
            Err(e) => wlog!(?e, "list_all_infohashes failed"),
        }

        // additional DHT crawler: probe random infohashes
        if st.dht_enable {
            debug!(queries=st.dht_random_queries, "launching random dht probes");
            for _ in 0..st.dht_random_queries {
                let mut raw = [0u8; 20];
                rand::thread_rng().fill(&mut raw);
                let hex = hex::encode(&raw);
                let st2 = st.clone();
                tokio::spawn(async move {
                    let _ = dht_discover_peers(&st2, &hex, &raw).await;
                });
            }
        }

        // persist accumulated uptime each cycle
        let base = st.accumulated_uptime.load(Ordering::Relaxed);
        let elapsed = st.app_started_at.elapsed().as_secs();
        let total = base + elapsed;
        let _ = st.store.set_accumulated_uptime(total).await;

        tokio::time::sleep(st.poll_interval).await;
    }
}

// Multi-protocol tracker loader (http/https, udp, ws/wss)
async fn load_trackers_multi(path: &str) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let content = tokio::fs::read_to_string(path).await.unwrap_or_default();
    let mut http = Vec::new(); let mut udp = Vec::new(); let mut ws = Vec::new();
    for line in content.lines() {
        let s = line.trim(); if s.is_empty() || s.starts_with('#') { continue; }
        if s.starts_with("http://") || s.starts_with("https://") { http.push(s.to_string()); }
        else if s.starts_with("udp://") { udp.push(s.to_string()); }
        else if s.starts_with("ws://") || s.starts_with("wss://") { ws.push(s.to_string()); }
    }
    Ok((http, udp, ws))
}

// 更鲁棒的下载：支持多个 URL、重试与指数退避，并在 TLS 失败时尝试 native-tls 回退
async fn download_and_write_trackers_multi(urls: &[String], path: &str, attempts_per_url: usize) -> Result<()> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).user_agent("AdySec-Tracker").build()?;
    let enable_native = std::env::var("ENABLE_NATIVE_TLS_FALLBACK").map(|v| v=="1").unwrap_or(true);
    let mut last_err: Option<anyhow::Error> = None;
    for url in urls {
        for i in 0..attempts_per_url.max(1) {
            match client.get(url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() { let text = resp.text().await?; tokio::fs::write(path, &text).await?; return Ok(()); }
                    else { let e = anyhow::anyhow!("status {} for {}", resp.status(), url); wlog!(?e, attempt=i+1, url=%url, "trackers download non-success status"); last_err = Some(e); }
                }
                Err(e) => {
                    let err_msg = e.to_string(); wlog!(?e, attempt=i+1, url=%url, "trackers download error (rustls)"); last_err = Some(anyhow::anyhow!(e));
                    if enable_native && (err_msg.contains("tls") || err_msg.contains("handshake") || err_msg.contains("certificate")) {
                        match NATIVE_TLS_CLIENT.get(url).send().await {
                            Ok(resp2) => { if resp2.status().is_success() { let text = resp2.text().await?; tokio::fs::write(path, &text).await?; return Ok(()); } else { let e2 = anyhow::anyhow!("status {} for {} (native)", resp2.status(), url); wlog!(?e2, attempt=i+1, url=%url, "trackers download non-success (native)"); last_err = Some(e2); } }
                            Err(e2) => { wlog!(?e2, attempt=i+1, url=%url, "trackers download error (native)"); last_err = Some(anyhow::anyhow!(e2)); }
                        }
                    }
                }
            }
            let backoff_ms = 500u64.saturating_mul(1u64 << i.min(8)); tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all urls exhausted for trackers download")))
}
