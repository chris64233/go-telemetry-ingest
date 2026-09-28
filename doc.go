// Package gotelemetryingest implements a device telemetry ingestion
// service that tolerates bounded out-of-order arrival.
//
// 每台设备的事件携带严格递增的序号（Sequence）与事件时间（EventTime）。
// 服务在配置的序号窗口内接受乱序到达、按序号去重；只有缺口全部补齐后
// 连续确认位置（AckedSeq）才能向前推进；汇总与确认位置在同一个持久化
// 事务中更新，保证恰好一次（exactly-once）计入。
//
// 核心组件：
//   - Service：接收/确认推进/关闭水位/查询的状态机（service.go）
//   - Store：设备状态持久化抽象（store.go），FileStore 为 WAL+快照实现，
//     MemoryStore 为内存实现
//   - HTTP API：见 http.go
package gotelemetryingest
