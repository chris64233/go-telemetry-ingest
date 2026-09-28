package main

import (
	"log"
	"net/http"
	"os"

	gotelemetryingest "github.com/chris64233/go-telemetry-ingest"
)

func main() {
	path := os.Getenv("STORE_PATH") // 为空则仅内存存储
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	store, err := gotelemetryingest.OpenStore(path)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	svc := gotelemetryingest.NewService(store)
	log.Printf("listening on %s (store=%q)", addr, path)
	log.Fatal(http.ListenAndServe(addr, gotelemetryingest.NewHandler(svc)))
}
