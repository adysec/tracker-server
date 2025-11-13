use std::net::SocketAddr;
use anyhow::{Context, Result};
use axum::{body::Bytes, extract::{ConnectInfo, Query, State, OriginalUri}, http::StatusCode, response::{IntoResponse, Response}, routing::get, Router};
use percent_encoding::{percent_decode_str, percent_encode, NON_ALPHANUMERIC};
use serde::Deserialize;
use sqlx::{sqlite::{SqlitePoolOptions, SqliteConnectOptions}, Pool, Sqlite};
use sqlx::Row; // bring trait into scope for row access if needed later
use std::str::FromStr;
use tracing::{info, warn};
use std::time::Duration;
use once_cell::sync::Lazy;
use rand::Rng;
use tokio::process::Command;
use tokio::sync::RwLock;
use std::sync::Arc;

// 日志与负载均衡控制开关（需在使用前定义）
// (宏与静态控制已前置；无需重复导入 Lazy)
static LOAD_BALANCE_ALL: Lazy<bool> = Lazy::new(|| std::env::var("LOAD_BALANCE_ALL").map(|v| v=="1").unwrap_or(false));
static SUPPRESS_WARN: Lazy<bool> = Lazy::new(|| std::env::var("SUPPRESS_WARN").map(|v| v=="1").unwrap_or(false));
macro_rules! wlog { ($($arg:tt)*) => { if !*SUPPRESS_WARN { warn!($($arg)*); } } }

#[derive(Clone)]
struct AppState {
    db: Pool<Sqlite>,
    // 拆分不同协议 tracker 列表（支持热更新）
    trackers_http: Arc<RwLock<Vec<String>>>,
    trackers_udp: Arc<RwLock<Vec<String>>>,
    trackers_ws: Arc<RwLock<Vec<String>>>,
    poll_interval: Duration,
    upstream_strategy: UpstreamStrategy,
    batch_size: usize,
    interval_seconds: i64,
    rr_index: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    timeout: Duration,
    fast_mode: FastMode,
    fast_max_wait: Duration,
    agg_max_concurrency: usize,
    semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

#[derive(Clone, Copy)]
enum UpstreamStrategy { All, RoundRobin, Random }

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FastMode { Wait, Timeout, Async, Off }

// 进程级上游 announce 使用的 20 字节 peer_id（BT 客户端标识），模仿 qBittorrent 风格以提高兼容性：-qB4630-xxxxxxxxxxxx
static PEER_ID_ENC: Lazy<String> = Lazy::new(|| {
    let mut id = [0u8; 20];
    let prefix = b"-qB4630-"; // mimic qBittorrent 4.6.3 style
    id[..prefix.len()].copy_from_slice(prefix);
    // 用 [0-9a-zA-Z] 填充后 12 字节，避免奇怪字节导致部分 tracker 拒绝
    const ALPHANUM: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::thread_rng();
    for i in prefix.len()..20 {
        id[i] = ALPHANUM[rng.gen_range(0..ALPHANUM.len())];
    }
    percent_encode(&id, NON_ALPHANUMERIC).to_string()
});

// 默认上游 trackers 列表来源（可用环境变量 TRACKERS_URL 覆盖）
const DEFAULT_TRACKERS_URL: &str = "https://raw.githubusercontent.com/adysec/tracker/main/trackers_all.txt";
const DEFAULT_TRACKERS_FILE: &str = "trackers.txt";

#[tokio::main]
async fn main() -> Result<()> {
    // 默认使用 info 日志级别；如果设置了 RUST_LOG 则按环境变量生效
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:trackers.db".to_string());
    let connect_opts = SqliteConnectOptions::from_str(&db_url).context("parse sqlite url")?.create_if_missing(true);
    let pool = SqlitePoolOptions::new().max_connections(5).connect_with(connect_opts).await.context("connect sqlite")?;
    init_db(&pool).await?;

    // 启动时：下载并覆盖 trackers.txt（失败则回退到本地文件），支持多个 URL 和重试
    let trackers_urls_env = std::env::var("TRACKERS_URLS").ok();
    let trackers_url_single = std::env::var("TRACKERS_URL").unwrap_or_else(|_| DEFAULT_TRACKERS_URL.to_string());
    let trackers_urls: Vec<String> = if let Some(s) = trackers_urls_env {
        s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()
    } else {
        vec![trackers_url_single.clone()]
    };
    let attempts = std::env::var("TRACKERS_DOWNLOAD_ATTEMPTS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(3);
    if let Err(e) = download_and_write_trackers_multi(&trackers_urls, DEFAULT_TRACKERS_FILE, attempts).await {
        wlog!(?e, urls=?trackers_urls, "download trackers failed at startup; fallback to existing trackers.txt if present");
    }
    let (trackers_http_init, trackers_udp_init, trackers_ws_init) = load_trackers_multi(DEFAULT_TRACKERS_FILE).await.unwrap_or_default();
    info!(http_count = trackers_http_init.len(), udp_count=trackers_udp_init.len(), ws_count=trackers_ws_init.len(), "loaded trackers");

    let poll_secs = std::env::var("POLL_INTERVAL_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(60);
    let strategy = std::env::var("UPSTREAM_STRATEGY").unwrap_or_else(|_| "round_robin".to_string());
    let upstream_strategy = match strategy.to_lowercase().as_str() {
        "all" => UpstreamStrategy::All,
        "random" => UpstreamStrategy::Random,
        _ => UpstreamStrategy::RoundRobin,
    };
    let batch_size = std::env::var("UPSTREAM_BATCH_SIZE").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(10);
    let interval_seconds = std::env::var("INTERVAL_SECONDS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(900);
    let fast_mode = match std::env::var("FAST_ANNOUNCE_MODE").unwrap_or_else(|_| "timeout".to_string()).to_lowercase().as_str() {
        "wait" => FastMode::Wait,
        "async" => FastMode::Async,
        "off" => FastMode::Off,
        _ => FastMode::Timeout,
    };
    let timeout = std::env::var("UPSTREAM_TIMEOUT_SECONDS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(10);
    let fast_max_wait = std::env::var("FAST_ANNOUNCE_MAX_WAIT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(2500);
    // 并发：默认使用所有 HTTP tracker 数量(至少50)作为并发上限，保证可同时打满；可用 AGG_MAX_CONCURRENCY 覆盖
    let agg_max_concurrency = std::env::var("AGG_MAX_CONCURRENCY").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(trackers_http_init.len().max(50));
    let state = AppState {
        db: pool.clone(),
        trackers_http: Arc::new(RwLock::new(trackers_http_init)),
        trackers_udp: Arc::new(RwLock::new(trackers_udp_init)),
        trackers_ws: Arc::new(RwLock::new(trackers_ws_init)),
        poll_interval: Duration::from_secs(poll_secs),
        upstream_strategy,
        batch_size,
        interval_seconds,
        rr_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        timeout: Duration::from_secs(timeout),
        fast_max_wait: Duration::from_millis(fast_max_wait),
        fast_mode,
        agg_max_concurrency,
        semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(agg_max_concurrency.max(1))),
    };

    // 启动后台轮询任务：定期为所有已登记的 info_hash 查询上游
    let state_clone = state.clone();
    tokio::spawn(async move { poll_loop(state_clone).await; });

    // 启动每日更新 trackers 列表的任务（每24小时）
    let state_reload = state.clone();
    let trackers_urls_clone = trackers_urls.clone();
    tokio::spawn(async move {
        // 成功与失败的刷新间隔（秒）可调
        let ok_wait = std::env::var("TRACKERS_REFRESH_OK_SECS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(24*3600);
        let fail_wait = std::env::var("TRACKERS_REFRESH_FAIL_SECS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(3600);
        let attempts = std::env::var("TRACKERS_DOWNLOAD_ATTEMPTS").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(3);
        loop {
            let result = download_and_write_trackers_multi(&trackers_urls_clone, DEFAULT_TRACKERS_FILE, attempts).await;
            let mut success = false;
            match result {
                Ok(()) => {
                    match load_trackers_multi(DEFAULT_TRACKERS_FILE).await {
                        Ok((http, udp, ws)) => {
                            {
                                let mut w = state_reload.trackers_http.write().await; *w = http;
                            }
                            {
                                let mut w = state_reload.trackers_udp.write().await; *w = udp;
                            }
                            {
                                let mut w = state_reload.trackers_ws.write().await; *w = ws;
                            }
                            info!("trackers list updated from remote");
                            success = true;
                        }
                        Err(e) => wlog!(?e, "reload trackers from file failed"),
                    }
                }
                Err(e) => {
                    wlog!(?e, urls=?trackers_urls_clone, "daily trackers download failed");
                }
            }
            let sleep_secs = if success { ok_wait } else { fail_wait };
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
        }
    });

    let app = Router::new()
        .route("/announce", get(announce))
        .with_state(state);

    let addr: SocketAddr = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:1337".to_string()).parse().context("parse BIND addr")?;
    info!(%addr, "starting http server");
    let listener = tokio::net::TcpListener::bind(addr).await.context("bind listener")?;
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.context("server error")?;
    Ok(())
}

async fn init_db(db: &Pool<Sqlite>) -> Result<()> {
    // 全局/连接级 SQLite 参数：开启 WAL、降低同步等级以兼顾可靠与性能
    // 注意：journal_mode=WAL 对文件数据库持久生效；对内存库仅限当前连接
    let _ = sqlx::query("PRAGMA journal_mode=WAL;").execute(db).await; // 忽略错误以兼容某些平台
    let _ = sqlx::query("PRAGMA synchronous=NORMAL;").execute(db).await;

    // peers 表：新增 peer_id + last_seen 列以支持 TTL 和非紧缩响应的真实 peer_id
    sqlx::query(r#"CREATE TABLE IF NOT EXISTS peers (
        info_hash TEXT NOT NULL,
        ip TEXT NOT NULL,
        port INTEGER NOT NULL,
        left INTEGER,
        peer_id TEXT,
        last_seen INTEGER,
        PRIMARY KEY (info_hash, ip, port)
    );"#).execute(db).await?;
    sqlx::query(r#"CREATE TABLE IF NOT EXISTS info_hashes (ih TEXT PRIMARY KEY);"#).execute(db).await?;
    let cols = sqlx::query("PRAGMA table_info(peers)").fetch_all(db).await?;
    let mut has_left=false; let mut has_peer_id=false; let mut has_last_seen=false;
    for row in cols {
        let name: String = row.get(1);
        match name.as_str() {
            "left" => has_left = true,
            "peer_id" => has_peer_id = true,
            "last_seen" => has_last_seen = true,
            _ => {}
        }
    }
    if !has_left { sqlx::query("ALTER TABLE peers ADD COLUMN left INTEGER").execute(db).await?; }
    if !has_peer_id { sqlx::query("ALTER TABLE peers ADD COLUMN peer_id TEXT").execute(db).await?; }
    if !has_last_seen { sqlx::query("ALTER TABLE peers ADD COLUMN last_seen INTEGER").execute(db).await?; }

    // 针对常见查询/维护操作建立索引：
    // - prune_old_peers 使用 WHERE last_seen < ?，需要 last_seen 单列索引
    // - 按 info_hash 的读取由主键 (info_hash, ip, port) 的左前缀已覆盖，无需重复索引
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);").execute(db).await?;
    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct AnnounceQuery {
    info_hash: Option<String>,
    ih: Option<String>,
    peer_id: Option<String>,
    port: Option<u16>,
    uploaded: Option<u64>,
    downloaded: Option<u64>,
    left: Option<u64>,
    key: Option<String>,
    event: Option<String>,
    numwant: Option<usize>,
    compact: Option<u8>, // 1=compact blob, 0=list of dictionaries
    no_peer_id: Option<u8>, // 1=omit peer id when non-compact
    ip: Option<String>, // optional client supplied IP override
}

static TRACKER_ID: Lazy<String> = Lazy::new(|| {
    let r: u64 = rand::thread_rng().gen();
    format!("{:016x}", r)
});

fn failure(reason: &str) -> Response {
    let mut body = Vec::with_capacity(32 + reason.len());
    body.extend_from_slice(b"d14:failure reason");
    body.extend_from_slice(reason.len().to_string().as_bytes());
    body.push(b':');
    body.extend_from_slice(reason.as_bytes());
    body.push(b'e');
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, axum::http::HeaderValue::from_static("text/plain"))], body).into_response()
}

async fn announce(State(state): State<AppState>, ConnectInfo(remote): ConnectInfo<SocketAddr>, OriginalUri(uri): OriginalUri, Query(q): Query<AnnounceQuery>) -> Result<Response, StatusCode> {
    let raw_query = uri.query();
    let info_hash_raw = match parse_info_hash_full(q.info_hash.as_ref(), q.ih.as_ref(), raw_query) {
        Ok(v) => v,
        Err(e) => {
            wlog!(err=?e, original_info_hash=?q.info_hash, original_ih=?q.ih, "info_hash parse failed");
            return Ok(failure("bad info_hash format"));
        }
    };
    debug_assert_eq!(info_hash_raw.len(), 20);
    let peer_id_raw = if let Some(s) = &q.peer_id { percent_decode_str(s).collect::<Vec<u8>>() } else { return Ok(failure("missing peer_id")); };
    if peer_id_raw.is_empty() { return Ok(failure("bad peer_id length")); }
    if peer_id_raw.len()!=20 { wlog!(len=peer_id_raw.len(), "non-standard peer_id length accepted"); }
    let port = q.port.unwrap_or(0); if port==0 { return Ok(failure("missing port")); }
    let numwant = q.numwant.unwrap_or(50).min(200);
    let info_hash_hex = hex::encode(&info_hash_raw);

    // State update (register / upsert / remove)
    let mut left = q.left; if matches!(q.event.as_deref(), Some("completed")) { left = Some(0); }
    if matches!(q.event.as_deref(), Some("stopped")) {
    if let Err(e)=remove_peer(&state.db, &info_hash_hex, q.ip.as_ref().unwrap_or(&remote.ip().to_string()), port).await { wlog!(?e, "remove failed"); }
    } else {
    if let Err(e)=upsert_peer(&state.db, &info_hash_hex, q.ip.as_ref().unwrap_or(&remote.ip().to_string()), port, left, Some(&peer_id_raw)).await { wlog!(?e, "upsert failed"); }
    }
    if let Err(e)=register_info_hash(&state.db, &info_hash_hex).await { wlog!(?e, "register ih failed"); }

    // Upstream announce params (fallback to process peer id if non-standard length)
    let upstream_left_env = std::env::var("UPSTREAM_LEFT").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(16384);
    let upstream_numwant_env = std::env::var("UPSTREAM_NUMWANT").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(200);
    let peer_id_encoded_incoming = encode_lowercase(&peer_id_raw);
    let upstream_params = UpstreamAnnounceParams { peer_id_encoded: if peer_id_raw.len()==20 { peer_id_encoded_incoming.clone() } else { (*PEER_ID_ENC).clone() }, port, left: q.left.unwrap_or(upstream_left_env), event: q.event.clone().unwrap_or_else(|| "started".to_string()), numwant: upstream_numwant_env.min(numwant as u32) };

    // Python spider compatibility path: optionally invoke Python spider to aggregate peers exactly like the legacy script
    let use_py = std::env::var("USE_PY_SPIDER").map(|v| v=="1").unwrap_or(true);
    if use_py {
        let trackers_path = std::env::var("TRACKERS_FILE").unwrap_or_else(|_| "trackers.txt".to_string());
        let py_timeout = std::env::var("PY_SPIDER_TIMEOUT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5000);
        match tokio::time::timeout(Duration::from_millis(py_timeout), run_python_spider_and_upsert(&state, &info_hash_hex, &trackers_path)).await {
            Ok(Ok(inserted)) => info!(ih=%info_hash_hex, inserted, "python spider aggregated"),
            Ok(Err(e)) => wlog!(?e, "python spider failed; will fall back to Rust upstream fetching"),
            Err(_) => wlog!(timeout_ms=py_timeout, "python spider timeout; will fall back to Rust upstream fetching"),
        }
    }

    // Fast announce pre-fetch (Rust path)
    // 若首次出现该 info_hash 本地 peers 极少（<3），临时提升策略为 Wait，避免空响应（动态回退）
    let initial_peers = load_peers(&state.db, &info_hash_hex, 3).await.unwrap_or_default();
    let effective_fast_mode = if initial_peers.is_empty() && matches!(state.fast_mode, FastMode::Timeout) { FastMode::Wait } else { state.fast_mode };
    match effective_fast_mode {
        FastMode::Off => {}
        FastMode::Async => {
            let trackers_arc = state.trackers_http.clone();
            let db = state.db.clone();
            let ih_hex = info_hash_hex.clone();
            let ih_raw = info_hash_raw.clone();
            let strategy = state.upstream_strategy;
            let batch = state.batch_size;
            let rr = state.rr_index.clone();
            let params_clone = upstream_params.clone();
            let timeout = state.timeout;
            let fast_max_wait = state.fast_max_wait;
            tokio::spawn(async move {
                let http = trackers_arc.read().await.clone();
                let subset = select_trackers(&http, strategy, batch.max(5), &rr);
                if subset.is_empty() { return; }
                let temp_state = AppState { db, trackers_http: Arc::new(RwLock::new(Vec::new())), trackers_udp: Arc::new(RwLock::new(Vec::new())), trackers_ws: Arc::new(RwLock::new(Vec::new())), poll_interval: Duration::from_secs(0), upstream_strategy: strategy, batch_size: batch, interval_seconds: 0, rr_index: rr.clone(), timeout, fast_max_wait, fast_mode: FastMode::Off, agg_max_concurrency: 0, semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(subset.len().max(1))) };
                if let Err(e)=fetch_from_upstreams(&temp_state, &subset, &ih_hex, &ih_raw, &params_clone).await { wlog!(ih=%ih_hex, ?e, "fast async upstream failed"); }
            });
        }
        FastMode::Wait => {
            let subset = {
                let http = state.trackers_http.read().await.clone();
                select_trackers(&http, state.upstream_strategy, state.batch_size.max(5), &state.rr_index)
            };
            if !subset.is_empty() {
                let started = std::time::Instant::now();
                match fetch_from_upstreams(&state, &subset, &info_hash_hex, &info_hash_raw, &upstream_params).await {
                    Ok(sum) => info!(ih=%info_hash_hex, trackers_ok=sum.ok, peers_added=sum.peers_added, elapsed_ms=started.elapsed().as_millis(), "fast wait fetched"),
                    Err(e) => wlog!(?e, ih=%info_hash_hex, "fast wait upstream error"),
                }
            }
        }
        FastMode::Timeout => {
            let subset = {
                let http = state.trackers_http.read().await.clone();
                select_trackers(&http, state.upstream_strategy, state.batch_size.max(5), &state.rr_index)
            };
            if !subset.is_empty() {
                let started = std::time::Instant::now();
                let fut = fetch_from_upstreams(&state, &subset, &info_hash_hex, &info_hash_raw, &upstream_params);
                match tokio::time::timeout(state.fast_max_wait, fut).await {
                    Ok(Ok(sum)) => info!(ih=%info_hash_hex, trackers_ok=sum.ok, peers_added=sum.peers_added, elapsed_ms=started.elapsed().as_millis(), "fast timeout fetched"),
                    Ok(Err(e)) => wlog!(?e, ih=%info_hash_hex, "fast timeout upstream error"),
                    Err(_) => wlog!(ih=%info_hash_hex, waited_ms=state.fast_max_wait.as_millis(), "fast timeout reached"),
                }
            }
        }
    }

    // TTL prune and load cached peers
    let ttl_secs = std::env::var("PEER_TTL_SECONDS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(3600);
    if let Err(e)=prune_old_peers(&state.db, ttl_secs).await { wlog!(?e, "prune failed"); }
    let mut peers = load_peers(&state.db, &info_hash_hex, numwant).await.unwrap_or_default();

    // On-demand multi-protocol aggregation if insufficient (Rust path)
    if !use_py && peers.len() < numwant/2 {
    let enable_udp = std::env::var("ENABLE_UDP").map(|v| v=="1").unwrap_or(true);
    let enable_ws = std::env::var("ENABLE_WS").map(|v| v=="1").unwrap_or(true);
        let max_wait = std::env::var("ON_DEMAND_MAX_WAIT_MS").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(4000);
        let started = std::time::Instant::now();
        let params_full = UpstreamAnnounceParams { peer_id_encoded: peer_id_encoded_incoming.clone(), port, left: q.left.unwrap_or(0), event: q.event.clone().unwrap_or_else(|| "started".to_string()), numwant: numwant as u32 };
    let http_list = state.trackers_http.read().await.clone();
    let udp_list = if enable_udp { state.trackers_udp.read().await.clone() } else { Vec::new() };
    let ws_list = if enable_ws { state.trackers_ws.read().await.clone() } else { Vec::new() };
        let db_clone = state.db.clone(); let ih_hex_clone = info_hash_hex.clone(); let ih_raw_clone = info_hash_raw.clone(); let timeout = state.timeout;
    let fut = async move { if let Err(e)=aggregated_fetch(&db_clone, &ih_hex_clone, &ih_raw_clone, &params_full, &http_list, &udp_list, &ws_list, timeout).await { wlog!(ih=%ih_hex_clone, ?e, "on-demand aggregation failed"); } };
        match tokio::time::timeout(Duration::from_millis(max_wait), fut).await {
            Ok(_) => info!(ih=%info_hash_hex, elapsed_ms=started.elapsed().as_millis(), "on-demand aggregation done"),
            Err(_) => wlog!(ih=%info_hash_hex, waited_ms=max_wait, "on-demand aggregation timeout"),
        }
        peers = load_peers(&state.db, &info_hash_hex, numwant).await.unwrap_or_default();
    }

    // Build response
    let (complete, incomplete) = count_seeders_leechers(&state.db, &info_hash_hex).await.unwrap_or((0,0));
    let min_interval = std::env::var("MIN_INTERVAL_SECONDS").ok().and_then(|s| s.parse::<i64>().ok()).unwrap_or(state.interval_seconds/2);
    let compact_flag = q.compact.unwrap_or(1)==1;
    let no_peer_id = q.no_peer_id.unwrap_or(0)==1;
    info!(ih=%info_hash_hex, peers_out=peers.len(), compact=?compact_flag, fast_mode=?effective_fast_mode, initial_empty=initial_peers.is_empty(), "announce served");
    let mut body = Vec::new();
    body.extend_from_slice(b"d8:intervali"); body.extend_from_slice(state.interval_seconds.to_string().as_bytes());
    body.extend_from_slice(b"e12:min intervali"); body.extend_from_slice(min_interval.to_string().as_bytes());
    body.extend_from_slice(b"e10:tracker id"); body.extend_from_slice(TRACKER_ID.len().to_string().as_bytes()); body.push(b':'); body.extend_from_slice(TRACKER_ID.as_bytes());
    body.extend_from_slice(b"8:completei"); body.extend_from_slice(complete.to_string().as_bytes());
    body.extend_from_slice(b"e10:incompletei"); body.extend_from_slice(incomplete.to_string().as_bytes());
    body.extend_from_slice(b"e5:peers");
    if compact_flag {
        let mut blob = Vec::with_capacity(peers.len()*6);
        for (ip, port, _pid) in peers.iter() { if let Ok(addr)=ip.parse::<std::net::IpAddr>() { if let std::net::IpAddr::V4(v4)=addr { blob.extend_from_slice(&v4.octets()); blob.extend_from_slice(&(*port as u16).to_be_bytes()); } } }
        body.extend_from_slice(blob.len().to_string().as_bytes()); body.push(b':'); body.extend_from_slice(&blob);
    } else {
        body.push(b'l');
        for (ip, port, pid_opt) in peers.iter() {
            body.push(b'd');
            body.extend_from_slice(b"2:ip"); body.extend_from_slice(ip.len().to_string().as_bytes()); body.push(b':'); body.extend_from_slice(ip.as_bytes());
            body.extend_from_slice(b"4:porti"); body.extend_from_slice(port.to_string().as_bytes()); body.push(b'e');
            if !no_peer_id { if let Some(pid)=pid_opt { if pid.len()==20 { body.extend_from_slice(b"7:peer id20:"); body.extend_from_slice(pid); } } }
            body.push(b'e');
        }
        body.push(b'e');
    }
    body.push(b'e');
    Ok((StatusCode::OK, [(axum::http::header::CONTENT_TYPE, axum::http::HeaderValue::from_static("text/plain"))], body).into_response())
}

// removed /peers endpoint (non-standard)

async fn load_peers(db: &Pool<Sqlite>, ih_hex: &str, limit: usize) -> Result<Vec<(String, u16, Option<Vec<u8>>)>> {
    let rows = sqlx::query_as::<_, (String, i64, Option<String>)>("SELECT ip, port, peer_id FROM peers WHERE info_hash = ? LIMIT ?").bind(ih_hex).bind(limit as i64).fetch_all(db).await?;
    Ok(rows.into_iter().map(|(ip, port, pid)| {
        let decoded = pid.and_then(|s| hex::decode(&s).ok());
        (ip, port as u16, decoded)
    }).collect())
}

fn now_secs() -> i64 { use std::time::{SystemTime, UNIX_EPOCH}; SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64 }

async fn upsert_peer(db: &Pool<Sqlite>, ih_hex: &str, ip: &str, port: u16, left: Option<u64>, peer_id: Option<&[u8]>) -> Result<()> {
    sqlx::query(r#"INSERT INTO peers (info_hash, ip, port, left, peer_id, last_seen) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(info_hash, ip, port) DO UPDATE SET left=excluded.left, peer_id=COALESCE(excluded.peer_id, peers.peer_id), last_seen=excluded.last_seen"#)
        .bind(ih_hex)
        .bind(ip)
        .bind(port as i64)
        .bind(left.map(|v| v as i64))
        .bind(peer_id.map(|p| hex::encode(p)))
        .bind(now_secs())
        .execute(db)
        .await?;
    Ok(())
}
async fn remove_peer(db: &Pool<Sqlite>, ih_hex: &str, ip: &str, port: u16) -> Result<()> {
    sqlx::query("DELETE FROM peers WHERE info_hash=? AND ip=? AND port=?")
        .bind(ih_hex)
        .bind(ip)
        .bind(port as i64)
        .execute(db)
        .await?;
    Ok(())
}

async fn prune_old_peers(db: &Pool<Sqlite>, ttl_secs: i64) -> Result<()> {
    if ttl_secs <= 0 { return Ok(()); }
    let cutoff = now_secs() - ttl_secs;
    sqlx::query("DELETE FROM peers WHERE last_seen IS NOT NULL AND last_seen < ?")
        .bind(cutoff)
        .execute(db)
        .await?;
    Ok(())
}

async fn count_seeders_leechers(db: &Pool<Sqlite>, ih_hex: &str) -> Result<(i64, i64)> {
    let row = sqlx::query_as::<_, (i64, i64)>(
        "SELECT COALESCE(SUM(CASE WHEN left=0 THEN 1 END),0) as complete, COALESCE(SUM(CASE WHEN left>0 OR left IS NULL THEN 1 END),0) as incomplete FROM peers WHERE info_hash=?"
    )
    .bind(ih_hex)
    .fetch_one(db)
    .await?;
    Ok(row)
}

async fn register_info_hash(db: &Pool<Sqlite>, ih_hex: &str) -> Result<()> {
    sqlx::query(r#"INSERT OR IGNORE INTO info_hashes (ih) VALUES (?)"#).bind(ih_hex).execute(db).await?; Ok(())
}

struct FetchSummary { attempted: usize, ok: usize, peers_added: usize }

// TLS 回退客户端（rustls 初次失败后尝试 native-tls）
static NATIVE_TLS_CLIENT: Lazy<reqwest::Client> = Lazy::new(|| {
    reqwest::Client::builder()
        .user_agent("AdySec-Tracker-NativeTLS")
        .timeout(Duration::from_secs(15))
        .build()
        .expect("build native-tls client")
});

fn encode_lowercase(bytes: &[u8]) -> String { let mut out = String::with_capacity(bytes.len()*3); for b in bytes { out.push('%'); out.push(hex_char(b >> 4)); out.push(hex_char(b & 0x0f)); } out }
fn hex_char(n: u8) -> char { match n { 0..=9 => (b'0'+n) as char, 10..=15 => (b'a'+(n-10)) as char, _ => '?' } }

#[derive(Clone)]
struct UpstreamAnnounceParams {
    peer_id_encoded: String,
    port: u16,
    left: u64,
    event: String,
    numwant: u32,
}

async fn fetch_from_upstreams(state: &AppState, trackers: &[String], ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams) -> Result<FetchSummary> {
    let client = reqwest::Client::builder().timeout(state.timeout).user_agent("AdySec-Tracker").build()?;
    let ih_encoded = encode_lowercase(ih_raw); // 使用小写编码
    let mut handles = Vec::new();
    let mut attempted = 0usize;
    let _max_concurrency = state.agg_max_concurrency; // 读取避免 dead_code，同时为后续动态调节预留
    for t in trackers.iter() { attempted += 1;
        // 并发限制
        let _permit = state.semaphore.clone().acquire_owned().await.ok();
        let Ok(mut url) = reqwest::Url::parse(t) else { continue };
        if !url.path().ends_with("announce") { let mut path = url.path().to_string(); if !path.ends_with('/') { path.push('/'); } path.push_str("announce"); url.set_path(&path); }
        // 随机 key（十六进制）
        let key = format!("{:08X}", rand::thread_rng().gen::<u32>());
        url.set_query(Some(&format!(
            "info_hash={}&peer_id={}&port={}&uploaded=0&downloaded=0&left={}&event={}&numwant={}&compact=1&no_peer_id=1&supportcrypto=1&redundant=0&key={}",
            ih_encoded,
            params.peer_id_encoded,
            params.port,
            params.left,
            params.event,
            params.numwant,
            key
        )));
        let db = state.db.clone(); let ih = ih_hex.to_string(); let u = url.clone(); let client = client.clone();
        let handle = tokio::spawn(async move {
            let first = client.get(u.clone()).send().await;
            let result = match first {
                Ok(resp) => {
                    if resp.status().is_success() {
                        match resp.bytes().await {
                            Ok(bytes) => {
                                // 若返回失败信息，有助于定位问题
                                if bytes.windows(7).any(|w| w == b"7:failure") {
                                    let snippet = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
                                    wlog!(tracker=%u, body=%snippet, "tracker failure returned");
                                }
                                match parse_and_store(&db, &ih, &bytes).await {
                                    Ok(n) => {
                                        if n == 0 { wlog!(tracker=%u, ih=%ih, "no peers parsed from upstream"); }
                                        (true, n)
                                    }
                                    Err(e) => { wlog!(?e, tracker=%u, ih=%ih, "parse/store failed"); (false, 0) }
                                }
                            }
                            Err(e) => { wlog!(?e, tracker=%u, ih=%ih, "read body failed"); (false, 0) }
                        }
                    } else {
                        wlog!(status=?resp.status(), tracker=%u, ih=%ih, "tracker http error"); (false, 0)
                    }
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    let enable_native = std::env::var("ENABLE_NATIVE_TLS_FALLBACK").map(|v| v=="1").unwrap_or(true);
                    if enable_native && (err_msg.contains("tls") || err_msg.contains("handshake") || err_msg.contains("certificate")) {
                        wlog!(?e, tracker=%u, ih=%ih, "rustls failed, trying native-tls fallback");
                        match NATIVE_TLS_CLIENT.get(u.clone()).send().await {
                            Ok(resp2) => {
                                if resp2.status().is_success() {
                                    match resp2.bytes().await {
                                        Ok(bytes) => {
                                            if bytes.windows(7).any(|w| w == b"7:failure") {
                                                let snippet = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
                                                wlog!(tracker=%u, body=%snippet, "tracker failure returned (native-tls)");
                                            }
                                            match parse_and_store(&db, &ih, &bytes).await {
                                                Ok(n) => { if n==0 { wlog!(tracker=%u, ih=%ih, "no peers parsed (native-tls)"); } (true,n) }
                                                Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "parse/store failed (native-tls)" ); (false,0) }
                                            }
                                        }
                                        Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "read body failed (native-tls)" ); (false,0) }
                                    }
                                } else { wlog!(status=?resp2.status(), tracker=%u, ih=%ih, "tracker http error (native-tls)" ); (false,0) }
                            }
                            Err(e2) => { wlog!(?e2, tracker=%u, ih=%ih, "native-tls fallback also failed" ); (false,0) }
                        }
                    } else {
                        wlog!(?e, tracker=%u, ih=%ih, "tracker request failed"); (false, 0)
                    }
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

async fn poll_loop(state: AppState) {
    info!(interval=?state.poll_interval, "poll loop started (unlimited concurrency)");
    let state = std::sync::Arc::new(state);
    loop {
        match load_all_info_hashes(&state.db).await {
            Ok(list) => {
                for ih in list {
                    if let Ok(raw) = hex::decode(&ih) {
                        let st = state.clone();
                        tokio::spawn(async move {
                            let subset = {
                                let http = st.trackers_http.read().await.clone();
                                select_trackers(&http, st.upstream_strategy, st.batch_size, &st.rr_index)
                            };
                            let upstream_left_env = std::env::var("UPSTREAM_LEFT").ok().and_then(|s| s.parse::<u64>().ok()).unwrap_or(16384);
                            let upstream_numwant_env = std::env::var("UPSTREAM_NUMWANT").ok().and_then(|s| s.parse::<u32>().ok()).unwrap_or(200);
                            let params = UpstreamAnnounceParams { peer_id_encoded: (*PEER_ID_ENC).clone(), port: 6881, left: upstream_left_env, event: "started".to_string(), numwant: upstream_numwant_env };
                            match fetch_from_upstreams(&st, &subset, &ih, &raw, &params).await {
                                Ok(summary) => info!(ih=%ih, peers_added=summary.peers_added, trackers_ok=summary.ok, attempted=summary.attempted, "poll fetch done"),
                                Err(e) => wlog!(ih=%ih, ?e, "poll fetch failed"),
                            }
                        });
                    }
                }
            }
            Err(e) => wlog!(?e, "load info_hash list failed"),
        }
        tokio::time::sleep(state.poll_interval).await;
    }
}

async fn load_all_info_hashes(db: &Pool<Sqlite>) -> Result<Vec<String>> {
    let rows = sqlx::query_as::<_, (String,)>("SELECT ih FROM info_hashes").fetch_all(db).await?;
    Ok(rows.into_iter().map(|(s,)| s).collect())
}

// 对外可测试的 info_hash 解析函数：支持以下形式
// 1. 40 个十六进制字符（大小写均可），可带 0x 前缀
// 2. 百分号编码的 20 字节（%xx%yy ...）
// 3. 原始长度恰好 20 的 ASCII 字符串（容错某些错误客户端）
pub(crate) fn parse_info_hash_inputs(info_hash: Option<&String>, ih: Option<&String>) -> Result<Vec<u8>> {
    if let Some(s) = info_hash {
        let trimmed = s.trim();
        let hex_candidate = if trimmed.starts_with("0x") { &trimmed[2..] } else { trimmed };
        // hex（大小写混合允许）
        if hex_candidate.len() == 40 && hex_candidate.chars().all(|c| c.is_ascii_hexdigit()) {
            let bytes = hex::decode(hex_candidate).context("hex decode info_hash")?;
            if bytes.len() == 20 { return Ok(bytes); } else { anyhow::bail!("hex decoded length !=20"); }
        }
        // 百分号编码
        if trimmed.contains('%') {
            let decoded = percent_decode_str(trimmed).collect::<Vec<u8>>();
            if decoded.len() == 20 { return Ok(decoded); }
        }
        // 原始 20 字节（仅当长度恰好为 20）
        if trimmed.len() == 20 { return Ok(trimmed.as_bytes().to_vec()); }
        anyhow::bail!("unsupported info_hash format: length={} value='{}'", trimmed.len(), trimmed);
    } else if let Some(s) = ih {
        let trimmed = s.trim();
        let hex_candidate = if trimmed.starts_with("0x") { &trimmed[2..] } else { trimmed };
        if hex_candidate.len() == 40 && hex_candidate.chars().all(|c| c.is_ascii_hexdigit()) {
            let bytes = hex::decode(hex_candidate).context("hex decode ih")?;
            if bytes.len() == 20 { return Ok(bytes); } else { anyhow::bail!("ih hex decoded len !=20"); }
        }
        anyhow::bail!("ih param not 40 hex characters");
    } else {
        anyhow::bail!("missing info_hash/ih param");
    }
}

// 扩展：允许使用原始查询字符串中的百分号编码（避免 UTF-8 解码破坏高位字节）
pub(crate) fn parse_info_hash_full(info_hash_decoded: Option<&String>, ih_hex: Option<&String>, raw_query: Option<&str>) -> Result<Vec<u8>> {
    // 先尝试从 raw_query 中抓取未被 axum 解码的 info_hash= 部分
    if let Some(qs) = raw_query {
        if let Some(pos) = qs.find("info_hash=") {
            let start = pos + "info_hash=".len();
            let end = qs[start..].find('&').map(|e| start + e).unwrap_or(qs.len());
            let raw_enc = &qs[start..end];
            // 仅当包含至少一个 '%' 时尝试手动解码
            if raw_enc.contains('%') {
                let mut bytes = Vec::with_capacity(20);
                let mut i = 0;
                while i < raw_enc.len() {
                    let b = raw_enc.as_bytes()[i];
                    if b == b'%' && i + 2 < raw_enc.len() {
                        let h1 = raw_enc.as_bytes()[i+1];
                        let h2 = raw_enc.as_bytes()[i+2];
                        let hex_pair = [h1, h2];
                        if let Ok(v) = hex::decode(std::str::from_utf8(&hex_pair).unwrap()) {
                            bytes.push(v[0]);
                            i += 3;
                            continue;
                        }
                    }
                    // 非百分号编码字符，直接加入；对于 BT info_hash 一般不会出现
                    bytes.push(b);
                    i += 1;
                }
                if bytes.len() == 20 { return Ok(bytes); }
            }
        }
    }
    // 回退到解码后字符串/ih 解析
    parse_info_hash_inputs(info_hash_decoded, ih_hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{sqlite::{SqliteConnectOptions, SqlitePoolOptions}, Pool, Sqlite};
    use std::str::FromStr;

    async fn memory_pool() -> Pool<Sqlite> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:").unwrap().create_if_missing(true);
        let pool = SqlitePoolOptions::new().max_connections(1).connect_with(opts).await.unwrap();
        init_db(&pool).await.unwrap();
        pool
    }

    fn ih_zero_hex() -> String { hex::encode([0u8;20]) }

    #[tokio::test]
    async fn parse_compact_inserts_peer() {
        let pool = memory_pool().await;
        let ih_hex = ih_zero_hex();
        // bencode: d5:peers6:<6 bytes>e
        let peer_bytes = [1u8,2,3,4,0x1A,0xE1]; // ip 1.2.3.4 port 6881
        let mut body = b"d5:peers6:".to_vec(); body.extend_from_slice(&peer_bytes); body.push(b'e');
        let added = parse_and_store(&pool, &ih_hex, &Bytes::from(body)).await.unwrap();
        assert_eq!(added, 1, "should parse one compact peer");
        let peers = load_peers(&pool, &ih_hex, 10).await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].0, "1.2.3.4");
        assert_eq!(peers[0].1, 6881);
    }

    #[tokio::test]
    async fn parse_non_compact_inserts_peer() {
        let pool = memory_pool().await;
        let ih_hex = ih_zero_hex();
        // bencode non-compact list: d5:peersl d2:ip7:1.2.3.44:porti6881ee e
        // => d5:peersld2:ip7:1.2.3.44:porti6881eee
        let body = b"d5:peersld2:ip7:1.2.3.44:porti6881eee".to_vec();
        let added = parse_and_store(&pool, &ih_hex, &Bytes::from(body)).await.unwrap();
        assert_eq!(added, 1, "should parse one dictionary peer");
        let peers = load_peers(&pool, &ih_hex, 10).await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].0, "1.2.3.4");
        assert_eq!(peers[0].1, 6881);
    }

    #[tokio::test]
    async fn prune_old_peers_removes_stale() {
        let pool = memory_pool().await;
        let ih_hex = ih_zero_hex();
        // Insert two peers
        upsert_peer(&pool, &ih_hex, "10.0.0.1", 51413, Some(0), Some(&[0u8;20])).await.unwrap();
        upsert_peer(&pool, &ih_hex, "10.0.0.2", 51413, Some(0), Some(&[1u8;20])).await.unwrap();
        // Force one to be old
        let old_ts = now_secs() - 1000;
        sqlx::query("UPDATE peers SET last_seen=? WHERE ip='10.0.0.1'").bind(old_ts).execute(&pool).await.unwrap();
        prune_old_peers(&pool, 10).await.unwrap();
        let peers = load_peers(&pool, &ih_hex, 10).await.unwrap();
        assert_eq!(peers.len(), 1, "only fresh peer should remain");
        assert_eq!(peers[0].0, "10.0.0.2");
    }

    #[tokio::test]
    async fn upsert_stores_peer_id_var_lengths() {
        let pool = memory_pool().await;
        let ih_hex = ih_zero_hex();
        let short_pid = b"SHORTPID"; // 8 bytes
        let long_pid = [2u8;20];
        upsert_peer(&pool, &ih_hex, "8.8.8.8", 6000, None, Some(short_pid)).await.unwrap();
        upsert_peer(&pool, &ih_hex, "9.9.9.9", 6001, None, Some(&long_pid)).await.unwrap();
        let peers = load_peers(&pool, &ih_hex, 10).await.unwrap();
        // Sort for deterministic order
        let mut peers_sorted = peers.clone(); peers_sorted.sort_by_key(|p| p.0.clone());
        assert_eq!(peers_sorted.len(), 2);
        let p0 = &peers_sorted[0]; let p1 = &peers_sorted[1];
        if p0.0 == "8.8.8.8" { assert_eq!(p0.2.as_ref().unwrap().len(), 8); assert_eq!(p1.2.as_ref().unwrap().len(), 20); }
        else { assert_eq!(p0.2.as_ref().unwrap().len(), 20); assert_eq!(p1.2.as_ref().unwrap().len(), 8); }
    }

    #[test]
    fn parse_info_hash_hex_lowercase() {
        let s = "e831fcfaca5f0208009406b7b090014cef9228a9".to_string();
        let raw = parse_info_hash_inputs(Some(&s), None).unwrap();
        assert_eq!(hex::encode(&raw), s);
    }

    #[test]
    fn parse_info_hash_hex_uppercase() {
        let s_up = "E831FCFACA5F0208009406B7B090014CEF9228A9".to_string();
        let raw = parse_info_hash_inputs(Some(&s_up), None).unwrap();
        assert_eq!(hex::encode(&raw).to_uppercase(), s_up);
    }

    #[test]
    fn parse_info_hash_percent_encoded() {
        // %e8%31%fc%fa%ca%5f%02%08%00%94%06%b7%b0%90%01%4c%ef%92%28%a9 -> same bytes
        let s = "%e8%31%fc%fa%ca%5f%02%08%00%94%06%b7%b0%90%01%4c%ef%92%28%a9".to_string();
        let raw = parse_info_hash_inputs(Some(&s), None).unwrap();
        assert_eq!(hex::encode(&raw), "e831fcfaca5f0208009406b7b090014cef9228a9");
    }

    #[test]
    fn parse_info_hash_raw_20_bytes() {
        let raw20 = "ABCDEFGHIJKLMNOPQRST".to_string(); // length 20
        let out = parse_info_hash_inputs(Some(&raw20), None).unwrap();
        assert_eq!(out.len(), 20);
        assert_eq!(out, raw20.as_bytes());
    }

    #[test]
    fn parse_info_hash_invalid() {
        let bad = "12345".to_string();
        assert!(parse_info_hash_inputs(Some(&bad), None).is_err());
    }

    #[test]
    fn parse_info_hash_full_raw_query_percent() {
        let raw_q = "info_hash=%e8%31%fc%fa%ca%5f%02%08%00%94%06%b7%b0%90%01%4c%ef%92%28%a9&peer_id=-UT0001-AAAA&port=6881";
        let parsed = parse_info_hash_full(None, None, Some(raw_q)).unwrap();
        assert_eq!(hex::encode(parsed), "e831fcfaca5f0208009406b7b090014cef9228a9");
    }
}


fn select_trackers(list: &[String], strat: UpstreamStrategy, batch: usize, rr_index: &std::sync::Arc<std::sync::atomic::AtomicUsize>) -> Vec<String> {
    if list.is_empty() { return Vec::new(); }
    // 负载均衡模式：忽略 batch，轮转整个列表
    if *LOAD_BALANCE_ALL {
        let start = rr_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % list.len();
        let mut out = Vec::with_capacity(list.len());
        for i in 0..list.len() { out.push(list[(start + i) % list.len()].clone()); }
        return out;
    }
    match strat {
        UpstreamStrategy::All => list.to_vec(),
        UpstreamStrategy::Random => {
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            let mut v = list.to_vec();
            v.shuffle(&mut rng);
            v.truncate(batch.min(v.len()));
            v
        }
        UpstreamStrategy::RoundRobin => {
            let start = rr_index.fetch_add(batch, std::sync::atomic::Ordering::Relaxed) % list.len().max(1);
            let mut out = Vec::new();
            for i in 0..batch.min(list.len()) { out.push(list[(start + i) % list.len()].clone()); }
            out
        }
    }
}

async fn parse_and_store(db: &Pool<Sqlite>, ih_hex: &str, body: &Bytes) -> Result<usize> {
    let mut count = 0usize;
    // 尝试解析 compact 模式
    if let Some(idx) = body.windows(7).position(|w| w == b"5:peers") {
        let mut i = idx + 7; // 长度数字起始
        let mut len: usize = 0;
        while i < body.len() && body[i].is_ascii_digit() { len = len * 10 + (body[i]-b'0') as usize; i+=1; }
        if i < body.len() && body[i]==b':' { i+=1; if len>0 && i+len <= body.len() {
            let slice = &body[i..i+len];
            if len % 6 == 0 { // 认为是 compact
                for chunk in slice.chunks(6) { if chunk.len()==6 { let ip = std::net::Ipv4Addr::new(chunk[0],chunk[1],chunk[2],chunk[3]); let port = u16::from_be_bytes([chunk[4],chunk[5]]); if upsert_peer(db, ih_hex, &ip.to_string(), port, None, None).await.is_ok() { count+=1; } } }
            }
        }}
    }
    if count == 0 { // 解析非紧缩 peers 列表
        if let Some(start) = body.windows(8).position(|w| w == b"5:peersl") {
            let mut i = start + 8;
            while i < body.len() && body[i] != b'e' { // 列表结束
                if body[i] != b'd' { break; }
                i += 1; // skip 'd'
                let mut ip_opt: Option<String> = None; let mut port_opt: Option<u16> = None;
                while i < body.len() && body[i] != b'e' { // 字典内部
                    let mut key_len = 0usize; while i < body.len() && body[i].is_ascii_digit() { key_len = key_len*10 + (body[i]-b'0') as usize; i+=1; }
                    if i >= body.len() || body[i] != b':' { break; }
                    i+=1; if i + key_len > body.len() { break; }
                    let key = &body[i..i+key_len]; i+=key_len;
                    if i >= body.len() { break; }
                    match body[i] {
                        b'i' => { i+=1; let mut val: i64 = 0; let mut neg=false; if i<body.len() && body[i]==b'-' { neg=true; i+=1; } while i<body.len() && body[i].is_ascii_digit() { val = val*10 + (body[i]-b'0') as i64; i+=1; } if neg { val=-val; } if i<body.len() && body[i]==b'e' { i+=1; } if key==b"port" { if val>=0 && val<=u16::MAX as i64 { port_opt = Some(val as u16); } } }
                        b'l' => { i+=1; let mut depth=1; while i<body.len() && depth>0 { if body[i]==b'l' { depth+=1; } else if body[i]==b'e' { depth-=1; } i+=1; } }
                        b'd' => { i+=1; let mut depth=1; while i<body.len() && depth>0 { if body[i]==b'd' { depth+=1; } else if body[i]==b'e' { depth-=1; } i+=1; } }
                        _ if body[i].is_ascii_digit() => { let mut vlen=0usize; while i<body.len() && body[i].is_ascii_digit() { vlen = vlen*10 + (body[i]-b'0') as usize; i+=1; } if i<body.len() && body[i]==b':' { i+=1; } if i+vlen > body.len() { break; } let val_bytes=&body[i..i+vlen]; i+=vlen; if key==b"ip" { if let Ok(s)=std::str::from_utf8(val_bytes) { ip_opt = Some(s.to_string()); } } }
                        _ => { break; }
                    }
                }
                if i < body.len() && body[i]==b'e' { i+=1; }
                if let (Some(ip), Some(port)) = (ip_opt, port_opt) { if upsert_peer(db, ih_hex, &ip, port, None, None).await.is_ok() { count+=1; } }
            }
        }
    }
    Ok(count)
}

// Invoke the legacy Python spider to aggregate peers and upsert them into DB
async fn run_python_spider_and_upsert(state: &AppState, ih_hex: &str, trackers_file: &str) -> Result<usize> {
    // Determine python binary
    let py = std::env::var("PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());
    // Build command: python3 4-tracker_spider.py --json --trackers <file> --info-hash <ih>
    let mut cmd = Command::new(py);
    cmd.arg("4-tracker_spider.py")
        .arg("--json")
        .arg("--trackers").arg(trackers_file)
        .arg("--info-hash").arg(ih_hex)
        .arg("--peer-id").arg("AdySec-Tracker");
    // Execute and capture stdout
    let output = cmd.output().await?;
    if !output.status.success() {
        return Err(anyhow::anyhow!("python spider exit status {:?}", output.status));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Parse JSON: { "peers": [{"ip":"x","port":n}], "count": m }
    let v: serde_json::Value = serde_json::from_str(&stdout).context("parse python spider json")?;
    let mut inserted = 0usize;
    if let Some(arr) = v.get("peers").and_then(|x| x.as_array()) {
        for p in arr {
            if let (Some(ip), Some(port)) = (p.get("ip").and_then(|x| x.as_str()), p.get("port").and_then(|x| x.as_u64())) {
                if upsert_peer(&state.db, ih_hex, ip, port as u16, None, None).await.is_ok() { inserted += 1; }
            }
        }
    }
    Ok(inserted)
}

// removed /stats and /debug_fetch endpoints (non-standard)

// 多协议聚合抓取（HTTP / UDP / WS）
async fn aggregated_fetch(db: &Pool<Sqlite>, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, http_list: &[String], udp_list: &[String], ws_list: &[String], timeout: Duration) -> Result<()> {
    // HTTP
    if !http_list.is_empty() {
    let temp_state = AppState { db: db.clone(), trackers_http: Arc::new(RwLock::new(Vec::new())), trackers_udp: Arc::new(RwLock::new(Vec::new())), trackers_ws: Arc::new(RwLock::new(Vec::new())), poll_interval: Duration::from_secs(0), upstream_strategy: UpstreamStrategy::All, batch_size: http_list.len(), interval_seconds: 0, rr_index: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)), timeout, fast_mode: FastMode::Off, fast_max_wait: Duration::from_secs(0), agg_max_concurrency: 0, semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(http_list.len().max(1))) };
        let _ = fetch_from_upstreams(&temp_state, http_list, ih_hex, ih_raw, params).await;
    }
    // UDP (限制 50)
    if !udp_list.is_empty() {
    for u in udp_list.iter().take(50) { if let Err(e) = fetch_udp_tracker(db, ih_hex, ih_raw, params, u, timeout).await { wlog!(tracker=%u, ih=%ih_hex, ?e, "udp fetch failed"); } }
    }
    // WS (限制 10)
    if !ws_list.is_empty() {
    for w in ws_list.iter().take(10) { if let Err(e) = fetch_ws_tracker(db, ih_hex, ih_raw, params, w, timeout).await { wlog!(tracker=%w, ih=%ih_hex, ?e, "ws fetch failed"); } }
    }
    Ok(())
}

async fn fetch_udp_tracker(db: &Pool<Sqlite>, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, tracker: &str, timeout: Duration) -> Result<()> {
    use rand::Rng; use tokio::net::UdpSocket; use tokio::time::timeout as tok_timeout;
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
    for chunk in buf[20..n2].chunks(6) { if chunk.len()==6 { let ip = std::net::Ipv4Addr::new(chunk[0],chunk[1],chunk[2],chunk[3]); let port = u16::from_be_bytes([chunk[4],chunk[5]]); let _ = upsert_peer(db, ih_hex, &ip.to_string(), port, None, None).await; } }
    Ok(())
}

async fn fetch_ws_tracker(db: &Pool<Sqlite>, ih_hex: &str, ih_raw: &[u8], params: &UpstreamAnnounceParams, tracker: &str, timeout: Duration) -> Result<()> {
    use tokio_tungstenite::connect_async; use futures_util::{SinkExt, StreamExt};
    let (ws, _) = tokio::time::timeout(timeout, connect_async(tracker)).await??; let (mut write, mut read) = ws.split();
    let msg = serde_json::json!({"action":"announce","info_hash":hex::encode(ih_raw),"peer_id":params.peer_id_encoded,"uploaded":0,"downloaded":0,"left":params.left,"port":params.port,"event":params.event,"numwant":params.numwant});
    write.send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string())).await?;
    if let Ok(Some(frame)) = tokio::time::timeout(timeout, read.next()).await { if let Ok(text) = frame?.to_text() { if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) { if let Some(arr) = v.get("peers").and_then(|x| x.as_array()) { for p in arr { if let (Some(ip), Some(port)) = (p.get("ip").and_then(|x| x.as_str()), p.get("port").and_then(|x| x.as_u64())) { let pid_field = p.get("peer_id").and_then(|x| x.as_str()).map(|s| percent_decode_str(s).collect::<Vec<u8>>()); let _ = upsert_peer(db, ih_hex, ip, port as u16, None, pid_field.as_deref()).await; } } } } } }
    Ok(())
}


// Multi-protocol tracker loader (http/https, udp, ws/wss)
async fn load_trackers_multi(path: &str) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let content = tokio::fs::read_to_string(path).await.unwrap_or_default();
    let mut http = Vec::new();
    let mut udp = Vec::new();
    let mut ws = Vec::new();
    for line in content.lines() {
        let s = line.trim();
        if s.is_empty() || s.starts_with('#') { continue; }
        if s.starts_with("http://") || s.starts_with("https://") { http.push(s.to_string()); }
        else if s.starts_with("udp://") { udp.push(s.to_string()); }
        else if s.starts_with("ws://") || s.starts_with("wss://") { ws.push(s.to_string()); }
    }
    Ok((http, udp, ws))
}

// （保留单 URL 版本在多 URL 重试逻辑中等价实现，已移除冗余函数）

// 更鲁棒的下载：支持多个 URL、重试与指数退避，并在 TLS 失败时尝试 native-tls 回退
async fn download_and_write_trackers_multi(urls: &[String], path: &str, attempts_per_url: usize) -> Result<()> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).user_agent("AdySec-Tracker").build()?;
    let enable_native = std::env::var("ENABLE_NATIVE_TLS_FALLBACK").map(|v| v=="1").unwrap_or(true);
    let mut last_err: Option<anyhow::Error> = None;
    for url in urls {
        for i in 0..attempts_per_url.max(1) {
            // 尝试 rustls
            match client.get(url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let text = resp.text().await?;
                        tokio::fs::write(path, &text).await?;
                        return Ok(());
                    } else {
                        let e = anyhow::anyhow!("status {} for {}", resp.status(), url);
                        wlog!(?e, attempt=i+1, url=%url, "trackers download non-success status");
                        last_err = Some(e);
                    }
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    wlog!(?e, attempt=i+1, url=%url, "trackers download request error (rustls)");
                    last_err = Some(anyhow::anyhow!(e));
                    if enable_native && (err_msg.contains("tls") || err_msg.contains("handshake") || err_msg.contains("certificate")) {
                        match NATIVE_TLS_CLIENT.get(url).send().await {
                            Ok(resp2) => {
                                if resp2.status().is_success() {
                                    let text = resp2.text().await?;
                                    tokio::fs::write(path, &text).await?;
                                    return Ok(());
                                } else {
                                    let e2 = anyhow::anyhow!("status {} for {} (native)", resp2.status(), url);
                                    wlog!(?e2, attempt=i+1, url=%url, "trackers download non-success status (native)");
                                    last_err = Some(e2);
                                }
                            }
                            Err(e2) => {
                                wlog!(?e2, attempt=i+1, url=%url, "trackers download request error (native)");
                                last_err = Some(anyhow::anyhow!(e2));
                            }
                        }
                    }
                }
            }
            // 指数退避：500ms * 2^i
            let backoff_ms = 500u64.saturating_mul(1u64 << i.min(8));
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("all urls exhausted for trackers download")))
}
