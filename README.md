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

## 标定版本

设备更换传感器后，后续读数需要使用新的标定参数；旧标定下产生的迟到数据
只能留在历史区，不能改变当前统计。

1. **标定记录**：每条记录固定「设备、参数摘要 `parameterDigest`、生效序号
   `effectiveSeq`、操作者 `operator`」四要素，标定号 `calibrationId` 全局
   唯一，记录一旦写入不可变。设备登记时自动存在基线版本 `baseline`
   （生效序号 1）。**生效序号不能早于已经确认的读数**（必须
   `effectiveSeq > ackedSeq`），也不能早于已有标定；校验失败不写入任何状态，
   当前有效版本保持不变。
2. **读数绑定单一版本**：每条读数在首次到达时，按序号选择「生效序号
   ≤ 读数序号」的最新标定版本并**一次性绑定**，同时保存采用依据（版本号、
   绑定时刻的生效序号与参数摘要）。此后再发生标定切换也不会改绑——包括
   已经到达但仍 buffered 的读数。读数接收、标定切换、窗口汇总与水位结算
   全部在设备级互斥 + 存储版本 CAS 下串行化，每个序号恰好采用一个版本。
3. **迟到读数不回写当前聚合**：读数序号 ≤ 关闭水位时按 `late` 拒绝并留痕
   （记录其按序号本应采用的标定版本），既不进入顶层总计，也不改写任何标定
   版本的聚合。同一读数号提交不同内容仍返回 `conflict`。
4. **标定重放**：相同标定号带完全相同的设备/参数摘要/生效序号/操作者重放时，
   幂等返回首次结果（`duplicate=true`）；设备不同（标定号已属于其他设备）
   或任一要素变化，返回 `ErrCalibrationConflict`（HTTP 409）。标定失败
   （冲突或入参非法）不改变当前有效版本。
5. **查询**：
   - 设备视图 `GET /v1/devices/{id}`：设备配置、标定历史、当前有效版本、
     确认位置与关闭水位；
   - 标定历史 `GET /v1/devices/{id}/calibrations`；
   - 原始事件查询中每条读数带 `calibrationId`、`calibrationEffectiveSeq`、
     `calibrationDigest`（采用依据）；
   - 汇总中 `currentCalibrationId` + `current` 为**当前值**（当前有效版本
     区间内的 count/sum/min/max），`history` 为各版本聚合列表，旧版本条目
     `current=false` 表示**历史诊断值**；版本生效区间完全落入关闭水位后
     `frozen=true`，迟到读数无法再回写。顶层 `eventCount/sum/...` 仍为
     全部已确认读数的总计（跨版本）。

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

// 更换传感器：登记新标定版本（标定号/参数摘要/生效序号/操作者）。
// 生效序号必须 > 当前 ackedSeq；同号重放幂等，内容变化返回
// ErrCalibrationConflict。
cal, err := svc.ApplyCalibration("sensor-1", gti.CalibrationRequest{
    ID: "cal-2026-10-07", ParameterDigest: "sha256:abcd",
    EffectiveSeq: 101, Operator: "ops@site-a",
})
// cal.CurrentCalibrationID 为当前有效版本；序号 101 起的读数自动绑定新版本
cals, current, _ := svc.ListCalibrations("sensor-1")
sum, _ := svc.Summary("sensor-1")
// sum.Current / sum.CurrentCalibrationID 为当前值；
// sum.History 中 current=false 的条目为历史诊断值，frozen=true 表示
// 该版本区间已被水位永久关闭，迟到读数不会回写。
gaps, _ := svc.Gaps("sensor-1")                 // open / sealed 缺口
```

## HTTP API

时间统一为 RFC3339Nano（建议 UTC）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/v1/devices/{id}` | 登记设备，body `{"window":10}`；同配置重复登记幂等返回 200 |
| `GET` | `/v1/devices` | 设备列表 |
| `GET` | `/v1/devices/{id}` | 设备视图：配置、标定历史、当前有效版本、确认/关闭位置 |
| `POST` | `/v1/devices/{id}/events` | 接收事件 |
| `PUT` | `/v1/devices/{id}/calibrations` | 登记/重放标定版本 `{"calibrationId":"c1","parameterDigest":"sha256:..","effectiveSeq":10,"operator":"ops"}` |
| `GET` | `/v1/devices/{id}/calibrations` | 标定历史（按生效序号升序）与当前有效版本 |
| `POST` | `/v1/devices/{id}/watermark` | 推进关闭水位，body `{"closedSeq":5}` |
| `GET` | `/v1/devices/{id}/gaps` | 缺口（open/sealed） |
| `GET` | `/v1/devices/{id}/events?from=&limit=` | 原始事件（按序号升序） |
| `GET` | `/v1/devices/{id}/summary` | 汇总 |
| `GET` | `/v1/devices/{id}/rejections?limit=` | 拒绝原因（按时间倒序） |

事件 body：

```json
{"sequence":1,"eventTime":"2026-09-28T00:00:00Z","metric":"temp","value":23.5}
```

事件响应额外含 `calibrationId`（本读数采用的标定版本）；事件查询的每条记录
还含 `calibrationEffectiveSeq` 与 `calibrationDigest`（采用依据）。

状态码：成功/重复 `200`，设备与标定首次登记 `201`；读数冲突/超窗/迟到
`409`（body 含 `reason`），标定重放内容不一致或标定号已属其他设备 `409`；
标定生效序号不晚于已确认读数 `400`；设备不存在 `404`；入参非法 `400`。

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

更换传感器后登记新标定（序号 100 起生效），并查看标定历史与当前值：

```bash
curl -XPUT localhost:8080/v1/devices/d1/calibrations \
  -d '{"calibrationId":"cal-2","parameterDigest":"sha256:abcd","effectiveSeq":100,"operator":"ops"}'
curl localhost:8080/v1/devices/d1/calibrations
curl localhost:8080/v1/devices/d1
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
- 标定：基线绑定、切换边界、生效序号不得早于已确认读数、buffered 读数
  切换后不改绑、旧标定迟到读数只留痕不回写（历史/当前聚合均不变）、
  同读数号不同内容冲突；
- 标定重放：相同标定号幂等返回原结果，设备/参数/生效序号/操作者变化冲突，
  标定失败不改变当前有效版本，同号跨设备冲突；
- 汇总竞态：读数、标定重放、水位推进、汇总查询并发下每读数恰好绑定一个
  版本、各版本聚合计数不重不漏、版本区间关闭后冻结；
- 标定记录、读数采用依据与按版本聚合跨 WAL 重启保留；
- HTTP 标定接口（登记/重放/冲突/非法生效序号/设备视图/标定历史/汇总）。

## 范围与取舍

- 单进程部署：进程内每设备互斥 + 存储 CAS；多实例共享同一文件目录不在范围内。
- 设备状态以整帧写入 WAL（实现简单、恢复确定），高频场景可改为事件日志以减小写放大。
- 拒绝记录与原始事件按设备累积，查询提供分页上限（默认 100，最大 500）；
  长期保留/归档策略不在本次范围内。
