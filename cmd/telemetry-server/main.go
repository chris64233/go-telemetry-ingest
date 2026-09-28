// Command telemetry-server 启动设备遥测接收服务。
//
// 用法：
//
//	telemetry-server -addr :8080 -data ./data
//
// 不传 -data 时使用纯内存存储（重启丢失，仅用于演示/测试）。
package main

import (
	"flag"
	"log"

	gotelemetryingest "github.com/chris64233/go-telemetry-ingest"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataDir := flag.String("data", "", "data directory for WAL+snapshot (empty = in-memory)")
	flag.Parse()

	if err := gotelemetryingest.ListenAndServe(*addr, *dataDir); err != nil {
		log.Fatal(err)
	}
}
