use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool, sqlite::{SqlitePoolOptions, SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous}};
use tokio::sync::OnceCell;
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BtPeer {
    pub info_hash: String,
    pub ip: String,
    pub port: u16,
    pub left: Option<u64>,
    pub last_seen: i64,
}

#[derive(Debug, Clone)]
pub struct TorrentStats {
    pub info_hash: String,
    pub seeders: usize,
    pub leechers: usize,
    pub total_peers: usize,
    pub last_seen: i64,
    pub completed: u64,
}

pub struct PeerStore {
    hot_peers: Arc<RwLock<HashMap<String, HashMap<String, BtPeer>>>>,
    pool: OnceCell<SqlitePool>,
    db_url: String,
    hot_limit_total: usize,
    hot_limit_per_hash: usize,
}

impl PeerStore {
    pub fn new() -> Self {
        let db_path = std::env::var("PEER_DB_PATH").unwrap_or_else(|_| "tracker_peers.db".to_string());
        let db_url = if db_path.starts_with("sqlite:") {
            db_path
        } else {
            format!("sqlite://{}", db_path)
        };
        let hot_limit_total = std::env::var("HOT_PEER_LIMIT_TOTAL")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(300_000)
            .max(10_000);
        let hot_limit_per_hash = std::env::var("HOT_PEER_LIMIT_PER_INFOHASH")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(3_000)
            .max(100);

        Self {
            hot_peers: Arc::new(RwLock::new(HashMap::new())),
            pool: OnceCell::new(),
            db_url,
            hot_limit_total,
            hot_limit_per_hash,
        }
    }

    pub async fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.pool().await?;
        Ok(())
    }

    async fn pool(&self) -> Result<&SqlitePool, Box<dyn std::error::Error>> {
        self.pool
            .get_or_try_init(|| async {
                let opts = SqliteConnectOptions::from_str(&self.db_url)
                    .map_err(|e| sqlx::Error::Configuration(Box::new(e)))?
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal)
                    .foreign_keys(true);

                let pool = SqlitePoolOptions::new()
                    .max_connections(8)
                    .connect_with(opts)
                    .await?;

                sqlx::query("PRAGMA temp_store = MEMORY;")
                    .execute(&pool)
                    .await?;

                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS peers_latest (
                        info_hash TEXT NOT NULL,
                        ip TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        left_bytes INTEGER NULL,
                        last_seen INTEGER NOT NULL,
                        PRIMARY KEY (info_hash, ip, port)
                    )",
                )
                .execute(&pool)
                .await?;

                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS torrent_counters (
                        info_hash TEXT PRIMARY KEY,
                        completed INTEGER NOT NULL DEFAULT 0,
                        last_seen INTEGER NOT NULL DEFAULT 0
                    )",
                )
                .execute(&pool)
                .await?;
                sqlx::query("CREATE TABLE IF NOT EXISTS server_stats (
                        key TEXT PRIMARY KEY,
                        value INTEGER NOT NULL
                    )",
                )
                .execute(&pool)
                .await?;
                sqlx::query(
                    "CREATE TABLE IF NOT EXISTS announce_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        info_hash TEXT NOT NULL,
                        ip TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        event TEXT NOT NULL,
                        left_bytes INTEGER NULL,
                        ts INTEGER NOT NULL
                    )",
                )
                .execute(&pool)
                .await?;

                sqlx::query("CREATE INDEX IF NOT EXISTS idx_peers_infohash_last_seen ON peers_latest(info_hash, last_seen DESC)")
                    .execute(&pool)
                    .await?;
                sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_infohash_ts ON announce_events(info_hash, ts DESC)")
                    .execute(&pool)
                    .await?;

                Ok::<SqlitePool, sqlx::Error>(pool)
            })
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    pub async fn upsert_peer(
        &self,
        info_hash: &str,
        ip: &str,
        port: u16,
        left: Option<u64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("{}:{}", ip, port);
        let now = now_ts();

        let mut guard = self.hot_peers.write().await;
        let by_hash = guard
            .entry(info_hash.to_string())
            .or_insert_with(HashMap::new);

        if let Some(existing) = by_hash.get_mut(&key) {
            existing.last_seen = now;
            if left.is_some() {
                existing.left = left;
            }
        } else {
            by_hash.insert(
                key,
                BtPeer {
                    info_hash: info_hash.to_string(),
                    ip: ip.to_string(),
                    port,
                    left,
                    last_seen: now,
                },
            );
        }

        if by_hash.len() > self.hot_limit_per_hash {
            trim_oldest(by_hash, self.hot_limit_per_hash);
        }
        trim_total_hot(&mut guard, self.hot_limit_total);

        drop(guard);

        let pool = self.pool().await?;
        let left_i64 = left.map(|v| v as i64);

        sqlx::query(
            "INSERT INTO peers_latest (info_hash, ip, port, left_bytes, last_seen)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(info_hash, ip, port) DO UPDATE SET
               left_bytes = COALESCE(excluded.left_bytes, peers_latest.left_bytes),
               last_seen = excluded.last_seen",
        )
        .bind(info_hash)
        .bind(ip)
        .bind(port as i64)
        .bind(left_i64)
        .bind(now)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO torrent_counters (info_hash, completed, last_seen)
             VALUES (?1, 0, ?2)
             ON CONFLICT(info_hash) DO UPDATE SET last_seen = excluded.last_seen",
        )
        .bind(info_hash)
        .bind(now)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO announce_events (info_hash, ip, port, event, left_bytes, ts)
             VALUES (?1, ?2, ?3, 'announce', ?4, ?5)",
        )
        .bind(info_hash)
        .bind(ip)
        .bind(port as i64)
        .bind(left_i64)
        .bind(now)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn remove_peer(
        &self,
        info_hash: &str,
        ip: &str,
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = format!("{}:{}", ip, port);
        let mut guard = self.hot_peers.write().await;
        if let Some(by_hash) = guard.get_mut(info_hash) {
            by_hash.remove(&key);
            if by_hash.is_empty() {
                guard.remove(info_hash);
            }
        }

        let pool = self.pool().await?;
        let now = now_ts();
        sqlx::query("DELETE FROM peers_latest WHERE info_hash = ?1 AND ip = ?2 AND port = ?3")
            .bind(info_hash)
            .bind(ip)
            .bind(port as i64)
            .execute(pool)
            .await?;

        sqlx::query(
            "INSERT INTO announce_events (info_hash, ip, port, event, left_bytes, ts)
             VALUES (?1, ?2, ?3, 'stopped', NULL, ?4)",
        )
        .bind(info_hash)
        .bind(ip)
        .bind(port as i64)
        .bind(now)
        .execute(pool)
        .await?;

        sqlx::query(
            "INSERT INTO torrent_counters (info_hash, completed, last_seen)
             VALUES (?1, 0, ?2)
             ON CONFLICT(info_hash) DO UPDATE SET last_seen = excluded.last_seen",
        )
        .bind(info_hash)
        .bind(now)
        .execute(pool)
        .await?;

        Ok(())
    }

    pub async fn increment_completed(&self, info_hash: &str) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let now = now_ts();
        sqlx::query(
            "INSERT INTO torrent_counters (info_hash, completed, last_seen)
             VALUES (?1, 1, ?2)
             ON CONFLICT(info_hash) DO UPDATE SET completed = completed + 1, last_seen = excluded.last_seen",
        )
        .bind(info_hash)
        .bind(now)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn count_announces(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let row = sqlx::query("SELECT COUNT(*) as c FROM announce_events")
            .fetch_one(pool)
            .await?;
        let c: i64 = row.get("c");
        Ok(c as u64)
    }

    pub async fn get_accumulated_uptime(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        if let Ok(row) = sqlx::query("SELECT value FROM server_stats WHERE key='uptime'")
            .fetch_one(pool)
            .await
        {
            let v: i64 = row.get("value");
            Ok(v as u64)
        } else {
            Ok(0)
        }
    }

    pub async fn set_accumulated_uptime(&self, secs: u64) -> Result<(), Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        sqlx::query(
            "INSERT INTO server_stats (key,value) VALUES ('uptime', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(secs as i64)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn list_peers(
        &self,
        info_hash: &str,
        limit: usize,
    ) -> Result<Vec<BtPeer>, Box<dyn std::error::Error>> {
        let mut out: Vec<BtPeer> = {
            let guard = self.hot_peers.read().await;
            guard
            .get(info_hash)
            .map(|m| m.values().take(limit).cloned().collect())
            .unwrap_or_default()
        };

        if out.len() < limit {
            let pool = self.pool().await?;
            let rows = sqlx::query(
                "SELECT ip, port, left_bytes, last_seen
                 FROM peers_latest
                 WHERE info_hash = ?1
                 ORDER BY last_seen DESC
                 LIMIT ?2",
            )
            .bind(info_hash)
            .bind(limit as i64)
            .fetch_all(pool)
            .await?;

            let mut seen: HashMap<String, ()> = out
                .iter()
                .map(|p| (format!("{}:{}", p.ip, p.port), ()))
                .collect();

            for row in rows {
                let ip: String = row.try_get("ip")?;
                let port: i64 = row.try_get("port")?;
                let key = format!("{}:{}", ip, port);
                if seen.contains_key(&key) {
                    continue;
                }
                let left_bytes: Option<i64> = row.try_get("left_bytes")?;
                let last_seen: i64 = row.try_get("last_seen")?;
                out.push(BtPeer {
                    info_hash: info_hash.to_string(),
                    ip: ip.clone(),
                    port: port as u16,
                    left: left_bytes.map(|v| v as u64),
                    last_seen,
                });
                seen.insert(key, ());
                if out.len() >= limit {
                    break;
                }
            }
        }

        Ok(out)
    }

    pub async fn peer_stats(
        &self,
        info_hash: &str,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let row = sqlx::query(
            "SELECT
                SUM(CASE WHEN left_bytes = 0 THEN 1 ELSE 0 END) AS complete,
                SUM(CASE WHEN left_bytes = 0 THEN 0 ELSE 1 END) AS incomplete
             FROM peers_latest
             WHERE info_hash = ?1",
        )
        .bind(info_hash)
        .fetch_one(pool)
        .await?;

        let complete: Option<i64> = row.try_get("complete")?;
        let incomplete: Option<i64> = row.try_get("incomplete")?;
        Ok((complete.unwrap_or(0) as usize, incomplete.unwrap_or(0) as usize))
    }

    pub async fn list_all_infohashes(
        &self,
        limit: usize,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT info_hash FROM torrent_counters ORDER BY last_seen DESC LIMIT ?1",
        )
        .bind(limit as i64)
        .fetch_all(pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row.try_get::<String, _>("info_hash")?);
        }
        Ok(out)
    }

    pub async fn count_infohashes(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let row = sqlx::query("SELECT COUNT(*) AS cnt FROM torrent_counters")
            .fetch_one(pool)
            .await?;
        let cnt: i64 = row.try_get("cnt")?;
        Ok(cnt as usize)
    }

    pub async fn list_torrent_stats(
        &self,
        limit: usize,
    ) -> Result<Vec<TorrentStats>, Box<dyn std::error::Error>> {
        let pool = self.pool().await?;
        let rows = sqlx::query(
            "SELECT
                p.info_hash AS info_hash,
                SUM(CASE WHEN p.left_bytes = 0 THEN 1 ELSE 0 END) AS seeders,
                SUM(CASE WHEN p.left_bytes = 0 THEN 0 ELSE 1 END) AS leechers,
                COUNT(*) AS total_peers,
                MAX(p.last_seen) AS last_seen,
                COALESCE(tc.completed, 0) AS completed
             FROM peers_latest p
             LEFT JOIN torrent_counters tc ON tc.info_hash = p.info_hash
             GROUP BY p.info_hash
             ORDER BY total_peers DESC
             LIMIT ?1",
        )
        .bind(limit as i64)
        .fetch_all(pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(TorrentStats {
                info_hash: row.try_get::<String, _>("info_hash")?,
                seeders: row.try_get::<i64, _>("seeders")? as usize,
                leechers: row.try_get::<i64, _>("leechers")? as usize,
                total_peers: row.try_get::<i64, _>("total_peers")? as usize,
                last_seen: row.try_get::<i64, _>("last_seen")?,
                completed: row.try_get::<i64, _>("completed")? as u64,
            });
        }

        Ok(out)
    }

}

fn trim_oldest(map: &mut HashMap<String, BtPeer>, keep: usize) {
    if map.len() <= keep {
        return;
    }
    let mut rows: Vec<(String, i64)> = map
        .iter()
        .map(|(k, v)| (k.clone(), v.last_seen))
        .collect();
    rows.sort_by_key(|(_, ts)| *ts);
    let remove_n = rows.len().saturating_sub(keep);
    for (key, _) in rows.into_iter().take(remove_n) {
        map.remove(&key);
    }
}

fn trim_total_hot(
    hot: &mut HashMap<String, HashMap<String, BtPeer>>,
    total_limit: usize,
) {
    let mut total: usize = hot.values().map(|m| m.len()).sum();
    if total <= total_limit {
        return;
    }

    let target = total_limit.saturating_sub(total_limit / 10);
    while total > target {
        let mut oldest_hash: Option<String> = None;
        let mut oldest_key: Option<String> = None;
        let mut oldest_ts = i64::MAX;

        for (ih, peers) in hot.iter() {
            for (key, peer) in peers {
                if peer.last_seen < oldest_ts {
                    oldest_ts = peer.last_seen;
                    oldest_hash = Some(ih.clone());
                    oldest_key = Some(key.clone());
                }
            }
        }

        let (Some(ih), Some(key)) = (oldest_hash, oldest_key) else {
            break;
        };
        if let Some(by_hash) = hot.get_mut(&ih) {
            if by_hash.remove(&key).is_some() {
                total = total.saturating_sub(1);
            }
            if by_hash.is_empty() {
                hot.remove(&ih);
            }
        }
    }
}

fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
