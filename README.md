# go-telemetry-ingest

设备遥测接收与汇总服务：在配置的序号窗口内容忍乱序到达，按序号幂等去重，
维护每台设备的连续确认位置，并对已确认事件做恰好一次汇总。

## 核心语义

- **有限乱序窗口**：设备登记时指定 `window_size`，序号落在 `(Ack, Ack+window_size]`
  内的事件被接收；超出窗口的事件被拒绝并记录原因（`beyond_window`）。
- **按序号去重**：相同序号、相同内容重复提交返回原结果（`duplicate`），不产生任何
  状态变更；相同序号、不同内容返回冲突错误（HTTP 409）。
- **连续确认位置（Ack）**：只有缺口全部补齐才向前推进，收到较大序号不会越过缺失
  事件。所有写操作在单把互斥锁内串行提交，并发填补相邻缺口不会丢失推进结果。
- **恰好一次汇总**：Ack 推进时，新确认的事件与 Ack、汇总在**同一次原子快照写盘**
  （临时文件 + rename）中落盘；重复接收或服务重启都不会重复累加。
- **关闭水位**：水位推进采用乐观并发控制（`expected_version` 必须等于当前设备版本），
  只允许单调前进。事件时间不晚于水位的事件被拒绝并记录原因（`late_event`），
  拒绝会持久化但绝不改写已完成的汇总。水位推进与迟到事件在同一设备锁内串行，
  依据同一设备版本得出确定结果。

## 构建与测试

```sh
go test -race ./...
go run ./cmd/server   # 环境变量 ADDR（默认 :8080）、STORE_PATH（为空则纯内存）
```

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/devices` | 登记设备 `{"id":"dev-1","window_size":100}` |
| GET | `/devices/{id}` | 设备状态（Ack、水位、版本、汇总） |
| POST | `/devices/{id}/events` | 接收事件 `{"seq":1,"event_time":"...","value":1.5,"payload":"..."}` |
| GET | `/devices/{id}/events` | 原始事件列表；`?seq=N` 查询单条 |
| POST | `/devices/{id}/watermark` | 推进水位 `{"watermark":"...","expected_version":3}` |
| GET | `/devices/{id}/gaps` | 缺口查询（缺失序号与已缓冲序号） |
| GET | `/devices/{id}/summary` | 汇总查询（count、total_value、首末事件时间） |
| GET | `/devices/{id}/rejections` | 拒绝记录（含原因与判定时的设备版本） |

事件接收响应 `{"status":"accepted|duplicate|rejected","reason":"...","ack":N,"version":M}`。

### 示例

```sh
curl -X POST localhost:8080/devices -d '{"id":"dev-1","window_size":100}'
curl -X POST localhost:8080/devices/dev-1/events \
  -d '{"seq":2,"event_time":"2026-09-28T00:00:02Z","value":2}'
curl localhost:8080/devices/dev-1/gaps      # ack=0, missing=[1], buffered=[2]
curl -X POST localhost:8080/devices/dev-1/events \
  -d '{"seq":1,"event_time":"2026-09-28T00:00:01Z","value":1}'
curl localhost:8080/devices/dev-1/summary   # ack=2, count=2, total=3
```

## 代码结构

- `device.go` — 事件、汇总、设备、拒绝记录等核心类型
- `store.go` — 线程安全持久化存储（互斥锁 + JSON 原子快照），所有变更在同一持久化边界落盘
- `service.go` — 登记、接收、水位推进、缺口/事件/汇总查询等业务逻辑
- `server.go` — HTTP API
- `cmd/server/main.go` — 可执行入口
- `service_test.go` / `server_test.go` — 乱序、去重、冲突、并发、重启、水位等自动化测试
