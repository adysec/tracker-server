# Tracker Server

轻量级 BitTorrent tracker，使用内存热点+SQLite冷存储。内置上游聚合和DHT爬虫，提供一个简单的暗黑风格仪表盘。

## 构建与运行

```bash
cargo build --release
./target/release/tracker-server --bind 0.0.0.0:1337
```

常用命令行参数：
- `--bind` 绑定地址
- `--interval` announce间隔
- `--min-interval` 最小announce间隔
- `--peer-ttl` peer超时

少量环境变量可选，例如 `LOAD_BALANCE_ALL`、`UPSTREAM_STRATEGY`、`FAST_ANNOUNCE_MODE`、`POLL_INTERVAL_SECS` 和 `SUPPRESS_WARN`。DHT相关配置已写死，不通过环境变量设置。

## 接口

- `GET /`：仪表盘，显示种子数、peer 数、累积 queries 和前100活跃种子
- `GET /announce`：标准 BEP‑3 tracker 接口，返回 bencode

> **Queries 为数据库中的总计 announce 次数，跨重启保持累加。**

## 存储

默认数据库 `tracker_peers.db`，可用 `PEER_DB_PATH` 修改。内存缓存由热/冷策略管理。

> 为了支持高并发写入，SQLite 池大小提升至 32，busy timeout 设为 5 秒，并且所有对数据库的更新都会异步发送到后台任务队列，主路径不会被阻塞。后台任务会在最多 100 ms 或 1000 条记录后批量提交，这样每秒超过 1 万次的 announce 也能平稳处理。出现类似 `sqlx::pool::acquire` 慢日志时考虑增加池大小或降低写频率。
