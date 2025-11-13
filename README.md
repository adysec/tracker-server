# tracker-server

> 一个面向 BitTorrent 客户端的“多协议聚合 + 缓存”前置 Tracker 服务，仅暴露标准 `/announce` 接口，通过同时（或提前）向多种上游 tracker 请求来最大化 peer 数量、降低单点与空列表概率。


## ⭐ 特性速览

- 纯标准接口：只提供 `/announce`（去除所有非标准调试/统计端点）。
- 多协议聚合：自动分类并并发请求 HTTP(S) / UDP / WebSocket(WebTorrent) trackers。
- 快速模式（Fast Announce）：四种策略 `wait` / `timeout` / `async` / `off`，并有首见空缓存的智能降级。
- 按需聚合：响应前若缓存不足自动限时补抓（HTTP 全量 + UDP 50 + WS 10）。
- 负载均衡：`LOAD_BALANCE_ALL=1` 轮转全列表，避免少数上游过载、其余闲置。
- 高并发：默认并发阈值取 HTTP tracker 数量（至少 50），可通过 `AGG_MAX_CONCURRENCY` 调整。
- TLS 回退：Rustls 失败自动尝试 native-tls 以获得更高的兼容性。
- Python 兼容路径：可调用同目录下脚本 `4-tracker_spider.py` 以最大化和旧实现的抓取一致性。
- 宽松 `peer_id` 处理：接受非 20 字节长度（仅日志提示），上游可回退到内部伪装 ID。
- 信息哈希解析兼容：hex / 百分号编码 / 原始 20 字节容错。
- 可静默告警：`SUPPRESS_WARN=1` 关闭所有 warn 级输出，仅保留 info。
- TTL 清理：每次 announce 清理过期 peers，防止无限膨胀。

---

## 📋 目录 (Table of Contents)
1. [架构概述](#架构概述)
2. [安装与构建](#安装与构建)
3. [快速开始](#快速开始)
4. [环境变量配置](#环境变量配置)
5. [使用说明 /announce](#使用说明-announce)
6. [info_hash 兼容性详解](#info_hash-兼容性详解)
7. [负载均衡与并发](#负载均衡与并发)
8. [Python 爬虫兼容模式](#python-爬虫兼容模式)
9. [性能与实践建议](#性能与实践建议)
10. [Roadmap](#roadmap)
11. [贡献指南](#贡献指南)
12. [许可证](#许可证)
13. [作者](#作者)
14. [致谢](#致谢)

---

## 架构概述
tracker-server 作为客户端与多组上游 tracker 之间的前置层：

```
Client ── announce ─▶ tracker-server ──┬─ HTTP trackers
									   ├─ UDP trackers
									   └─ WS(WebTorrent) trackers
						▲  缓存 (SQLite + TTL)
						└─ 后台轮询 / 按需聚合
```

核心流程：
1. 客户端 `/announce` 请求到来 → 解析参数与 info_hash → 本地 upsert / register。  
2. Fast 模式（可阻塞或异步）选取一批上游立即抓取。  
3. 若缓存不足（`peers < numwant/2`），触发按需聚合（多协议并发 + 超时限制）。  
4. 汇总与清理（TTL）后构造标准 bencode 响应。  
5. 后台任务对已登记 info_hash 定期轮询 HTTP 上游填充缓存。  

---

## 安装与构建
```sh
git clone <repo-url> tracker-server
cd tracker-server
cargo build --release
```
生成的二进制当前名与包名一致：`tracker-server`（可在 `Cargo.toml` 中通过 `package.name` 重命名）。

运行：
```sh
./target/release/tracker-server
```

---

## 快速开始
最小化运行（默认使用 `trackers.txt`）：
```sh
FAST_ANNOUNCE_MODE=timeout \
UPSTREAM_STRATEGY=round_robin \
LOAD_BALANCE_ALL=1 \
cargo run
```

示例客户端请求：
```sh
curl 'http://127.0.0.1:8080/announce?ih=<40hex>&peer_id=-qB4630-ABCDEFGHIJKLMNOPQRST&port=51413&left=0&numwant=80'
```

---

## 环境变量配置
| 变量 | 说明 | 默认 |
|------|------|------|
| `BIND` | 监听地址 | `0.0.0.0:8080` |
| `DATABASE_URL` | SQLite 连接串 | `sqlite:trackers.db` |
| `POLL_INTERVAL_SECONDS` | 后台轮询间隔 | `60` |
| `UPSTREAM_STRATEGY` | 上游策略 `all|round_robin|random` | `round_robin` |
| `UPSTREAM_BATCH_SIZE` | fast/poll 批量大小（被 LOAD_BALANCE_ALL 覆盖时忽略） | `10` |
| `FAST_ANNOUNCE_MODE` | `wait|timeout|async|off` | `timeout` |
| `FAST_ANNOUNCE_MAX_WAIT_MS` | timeout 阻塞最长毫秒 | `2500` |
| `UPSTREAM_TIMEOUT_SECONDS` | 单上游超时 | `10` |
| `INTERVAL_SECONDS` | 响应 interval | `900` |
| `MIN_INTERVAL_SECONDS` | min interval | `interval/2` |
| `UPSTREAM_LEFT` | 上游 announce left 默认 | `16384` |
| `UPSTREAM_NUMWANT` | 上游 numwant 上限 | `200` |
| `AGG_MAX_CONCURRENCY` | HTTP 并发信号量大小 | `>=HTTP数, 至少50` |
| `LOAD_BALANCE_ALL` | 全量轮转所有 HTTP 上游 | `0` |
| `SUPPRESS_WARN` | 静默 warn 日志 | `0` |
| `ENABLE_UDP` | 按需聚合启用 UDP | `1` |
| `ENABLE_WS` | 按需聚合启用 WS | `1` |
| `ON_DEMAND_MAX_WAIT_MS` | 按需聚合最大等待 | `4000` |
| `PEER_TTL_SECONDS` | Peer TTL（<=0禁用） | `3600` |
| `ENABLE_NATIVE_TLS_FALLBACK` | rustls 失败回退 | `1` |
| `USE_PY_SPIDER` | 先调用 Python 爬虫 | `1` |
| `PY_SPIDER_TIMEOUT_MS` | Python 爬虫单次超时 | `5000` |
| `PYTHON_BIN` | Python 可执行名 | `python3` |
| `TRACKERS_FILE` | tracker 列表文件 | `trackers.txt` |

要点：
* 首次空缓存 + `timeout` 会临时升级为 `wait`。
* 开启 `LOAD_BALANCE_ALL` 后 fast/poll 忽略 batch，整表轮换。
* 开启 `SUPPRESS_WARN` 后调试信息减少，仅保留 info 级关键统计。

---

## 使用说明 /announce
请求参数（与标准一致）：
| 参数 | 描述 | 必需 | 备注 |
|------|------|------|------|
| `info_hash` / `ih` | 百分号编码 20 字节或 40 hex | 是 | 二选一即可 |
| `peer_id` | 百分号编码 peer id | 是 | 长度可≠20（兼容） |
| `port` | 客户端监听端口 | 是 | `>0` |
| `left` | 剩余字节 | 否 | `completed` 自动置0 |
| `event` | started/completed/stopped | 否 | |
| `numwant` | 期望 peers 数 | 否 | 上限 200 |
| `compact` | 1=紧缩 0=列表 | 否 | 默认1 |
| `no_peer_id` | 非紧缩省略 peer_id | 否 | 默认0 |

响应字段：`interval` / `min interval` / `tracker id` / `complete` / `incomplete` / `peers`。

示例：
```sh
curl 'http://127.0.0.1:8080/announce?ih=e831fcfaca5f0208009406b7b090014cef9228a9&peer_id=-qB4630-ABCDEFGHIJKLMNOPQRST&port=51413&left=0&numwant=80&compact=1'
```

---

## info_hash 兼容性详解
支持：
1. 百分号编码 20 原始字节：`%aa%bb...`
2. 40 十六进制：`e831fcfa...`
3. 参数别名：`ih=` 与 `info_hash=` 任选其一。
4. 可选 `0x` 前缀。
5. 原始长度 20 ASCII 容错（不推荐）。

实现策略：优先从原始查询串抓取百分号编码，避免已解码后高位字节损坏；失败即返回 `failure reason: bad info_hash format`。

---

## 负载均衡与并发
* `LOAD_BALANCE_ALL=1`：fast 与 poll 阶段对 HTTP trackers 做整列表轮询，每次起点单步递增，确保热度均匀。
* 正常模式：`round_robin` / `random` / `all` 受 `UPSTREAM_BATCH_SIZE` 限制。
* 并发：通过信号量控制，默认至少 50；避免在 tracker 很多时出现串行等待。
* 按需聚合始终尝试“尽可能多”协议（HTTP 全量 + UDP/WS 截取）。

---

## Python 爬虫兼容模式
开启 `USE_PY_SPIDER=1` 时：
1. 优先执行 `4-tracker_spider.py --json` 聚合。  
2. 若成功写入 peers → 继续正常响应。  
3. 若失败或超时 → 回退 Rust 原生抓取流程。  

用途：弥补某些 tracker 在 Python 宽松 TLS / 不同 UA 下的可访问性差异，最大限度保持旧脚本行为一致。

---

## 性能与实践建议
| 场景 | 建议 |
|------|------|
| 初次大量 info_hash | 使用 `wait` 或 `timeout` 提前填充缓存 |
| 稳定运行降噪 | 设置 `SUPPRESS_WARN=1` |
| 极端高延迟上游 | 减小 `FAST_ANNOUNCE_MAX_WAIT_MS` / `ON_DEMAND_MAX_WAIT_MS` |
| 失败率高 | 观察 info 日志中 trackers_ok vs attempted，调整策略或剔除失效 tracker |
| TLS 异常多 | 确认 `ENABLE_NATIVE_TLS_FALLBACK=1` 生效 |

---

## Roadmap
- [ ] Prometheus metrics 暴露
- [ ] IPv6 peers 输出支持
- [ ] 更细粒度的熔断与重试策略
- [ ] 可选后台 UDP/WS 轮询
- [ ] DHT / WebRTC 集成（实验）
- [ ] GitHub Action 自动化质量门禁（fmt/clippy/test）

### 自动更新 tracker 列表
本仓库包含每日自动刷新 `trackers.txt` 的工作流：`.github/workflows/update-trackers.yml`。

机制：
1. 计划任务（cron）在 UTC 03:00 运行。
2. 执行 `scripts/update_trackers.sh` 拉取多个公开源并去重排序。
3. 写入 `tracker-server/trackers.txt`（或根目录 `trackers.txt`），添加时间戳与来源注释。
4. 自动提交（`chore: daily tracker list update`）。

自定义：
* 修改 `scripts/update_trackers.sh` 中 `SOURCES` 数组添加或移除源。
* 调整 workflow cron 表达式改变频率。
* 若希望保留历史列表，可在脚本中追加 `cp` 到带日期副本目录。

注意：公共源可能偶尔失效；脚本对失败的 URL 打印警告但继续处理其他来源。

欢迎提交 Issue / PR 讨论优先级。

---

## 贡献指南
1. Fork 仓库并创建特性分支。  
2. 保持最小侵入：不破坏现有 `/announce` 行为。  
3. 提交前运行：`cargo fmt && cargo clippy && cargo test`。  
4. 在 PR 中描述动机、实现与风险。  

---

## 作者
AdySec <admin@adysec.com>

---

## 致谢
* BitTorrent 协议与社区文档。
* 开源依赖：`axum`、`tokio`、`sqlx`、`reqwest`、`tokio-tungstenite` 等。
* 以及所有提交 tracker 列表与兼容性反馈的贡献者。

如果本项目对你有帮助，欢迎 Star 🌟。

---

## 附录：文件结构
```
src/main.rs          主服务入口 / 核心逻辑
trackers.txt         上游 tracker 列表（支持注释 '#')
4-tracker_spider.py  Python 兼容抓取脚本（--json 输出模式）
Cargo.toml           Rust 构建与依赖
README.md            项目文档（当前文件）
```
