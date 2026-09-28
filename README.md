# go-telemetry-ingest

容忍有限乱序的设备遥测接收与汇总服务（Go，仅依赖标准库）。

## 核心语义

每台设备的事件携带**严格递增序号** `sequence`（从 1 开始）与**事件时间** `eventTime`。

1. **乱序窗口与去重**
   - 设备登记时配置乱序窗口 `window`：下一个期望序号 `acked+1` 之外，还允许提前
     `window` 个序号到达（接收上沿 = `acked + 1 + window`）。超过上沿的事件以
     `out_of_window` 拒绝并保留拒绝记录，不影响汇总。
   - 相同序号、相同内容重复提交：幂等返回**首次处理结果**（`duplicate=true`），
     绝不重复累计。
   - 相同序号、不同内容：以 `conflict` 拒绝，可用 `errors.Is(err, ErrConflict)`
     判定；原始事件与已完成汇总都不被改写。
   - 事件时间必须随序号严格递增（与窗口内已到的相邻序号比较）。

2. **连续确认位置（`ackedSeq`）**
   - 只有前面的缺口全部补齐，确认位置才能随连续事件向前推进；收到很大的序号
     不会越过缺失事件（大序号只在窗口内 buffered）。
   - 多个请求并发填补相邻缺口时，通过「设备级互斥 + 存储版本 CAS（乐观重试）」
     保证推进结果不丢失、不重复。

3. **恰好一次汇总**
   - 事件只有在被连续确认时才计入汇总（count/sum/min/max/首末事件时间）。
   - **汇总与确认位置在同一个持久化事务（同一 WAL 帧）中更新**：重复接收、
     服务重启都不会重复累加或漏算。

4. **关闭水位（`closedSeq`）**
   - 推进水位会把 `(ackedSeq, closedSeq]` 区间一次性结算：已到达事件按序计入
     汇总，缺失序号合并为**永久封闭缺口**（sealed gaps）。
   - 晚于水位到达（`seq <= closedSeq`）的事件以 `late` 拒绝并保留原因，
     不能改写已完成汇总，封闭缺口不能再填补。
   - 确认位置永远不回退；若确认已超前于水位，则只抬水位、不动汇总。
   - 水位推进与迟到事件并发时，由存储版本 CAS 线性化：双方基于同一设备版本，
     结果确定（迟到事件要么被计入、要么得到唯一一条 late 记录，二者恰好其一）。

5. **查询能力**：设备登记、事件接收、水位推进、缺口查询、原始事件查询、
   汇总查询、拒绝原因查询、设备列表。

## 架构

```
HTTP 层 (http.go, net/http ServeMux)
        │
   Service (service.go)                 状态机：窗口判定 / drain 推进 /
        │                                水位结算 / 去重冲突 / 查询
   Store 接口 (store.go)                Load + 版本 CAS（UpdateDevice）
        ├── MemoryStore (memory_store.go)   内存实现（测试/无盘部署）
        └── FileStore  (file_store.go)      WAL(CRC32 帧 + fsync) + 快照压缩
```

- **设备版本 CAS**：每次提交要求 `next.version == prev.version+1` 且存储中当前
  版本仍为 `prev`，否则返回 `ErrConflictVersion`，Service 自动基于最新版本重试。
- **WAL**：每帧 = `magic|type|len|payload|crc32`，每帧是一次完整设备状态，
  追加后 `fsync`。一帧即一个原子持久化边界。
- **快照压缩**：WAL 超过阈值后，在提交锁内写全量快照（tmp → fsync → 原子
  rename → 目录 fsync → 清空 WAL）。
- **崩溃恢复**：先载快照再顺序重放 WAL；尾部半帧 / CRC 错误视为写中途崩溃，
  自动截断到最后一个完好帧。

## 作为库使用

```go
import gti "github.com/chris64233/go-telemetry-ingest"

store, _ := gti.OpenFileStore("./data")        // 或 gti.NewMemoryStore()
svc := gti.NewService(store)

svc.RegisterDevice("sensor-1", 10)             // 允许提前 10 个序号

out, err := svc.Ingest("sensor-1", gti.Event{
    Sequence: 1, EventTime: t, Metric: "temp", Value: 23.5,
})
// out.Status: "acked" | "buffered"
// 拒绝时返回 (*IngestOutcome, *gti.RejectedError)，out.Reason 为
// conflict / out_of_window / late

res, _ := svc.AdvanceWatermark("sensor-1", 100) // 关闭水位，结算缺口
sum, _ := svc.Summary("sensor-1")
gaps, _ := svc.Gaps("sensor-1")                 // open / sealed 缺口
```

## HTTP API

时间统一为 RFC3339Nano（建议 UTC）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/v1/devices/{id}` | 登记设备，body `{"window":10}`；同配置重复登记幂等返回 200 |
| `GET` | `/v1/devices` | 设备列表 |
| `POST` | `/v1/devices/{id}/events` | 接收事件 |
| `POST` | `/v1/devices/{id}/watermark` | 推进关闭水位，body `{"closedSeq":5}` |
| `GET` | `/v1/devices/{id}/gaps` | 缺口（open/sealed） |
| `GET` | `/v1/devices/{id}/events?from=&limit=` | 原始事件（按序号升序） |
| `GET` | `/v1/devices/{id}/summary` | 汇总 |
| `GET` | `/v1/devices/{id}/rejections?limit=` | 拒绝原因（按时间倒序） |

事件 body：

```json
{"sequence":1,"eventTime":"2026-09-28T00:00:00Z","metric":"temp","value":23.5}
```

状态码：成功/重复 `200`，登记 `201`；冲突/超窗/迟到 `409`（body 含 `reason`）；
设备不存在 `404`；入参非法 `400`。

### 运行服务

```bash
go run ./cmd/telemetry-server -addr :8080 -data ./data
# 不传 -data 使用内存存储（重启丢失，仅演示用）
```

快速体验：

```bash
curl -XPUT localhost:8080/v1/devices/d1 -d '{"window":3}'
curl -XPOST localhost:8080/v1/devices/d1/events \
  -d '{"sequence":2,"eventTime":"2026-09-28T00:00:02Z","metric":"temp","value":2}'
curl -XPOST localhost:8080/v1/devices/d1/events \
  -d '{"sequence":1,"eventTime":"2026-09-28T00:00:01Z","metric":"temp","value":1}'
curl localhost:8080/v1/devices/d1/summary
```

## 测试

```bash
go test -race ./...
```

覆盖：

- 顺序/乱序接收、窗口边界（`window=0` 与上沿）、连续推进与汇总；
- 相同内容幂等、同序号不同内容冲突、超窗拒绝；
- 缺口查询随填补收敛；
- 水位推进结算 buffered、合并封闭缺口、迟到拒绝、幂等空推进、确认位置不回退；
- 并发填补相邻缺口（200 事件 + 重复首事件）不丢不重；
- 水位推进与迟到事件并发（内存/文件两种存储）结果确定；
- WAL 重启不重复累加、重复提交幂等、水位/拒绝记录跨重启保留；
- 快照压缩后重启状态完整；崩溃尾部半帧自动截断恢复；
- CAS 版本冲突自动重试；
- HTTP 端到端（登记幂等、acked/buffered/duplicate/conflict/late、各查询、错误码）。

## 范围与取舍

- 单进程部署：进程内每设备互斥 + 存储 CAS；多实例共享同一文件目录不在范围内。
- 设备状态以整帧写入 WAL（实现简单、恢复确定），高频场景可改为事件日志以减小写放大。
- 拒绝记录与原始事件按设备累积，查询提供分页上限（默认 100，最大 500）；
  长期保留/归档策略不在本次范围内。
