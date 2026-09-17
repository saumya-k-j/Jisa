// Command alertd is the alert delivery sidecar described in SPEC 3.13. It
// accepts confirmed alerts from the engine and delivers them to HTTP
// subscribers with at-least-once semantics, signing and retrying each
// delivery. It never sits on the ingestion hot path.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/saumya-k-j/Jisa/go/delivery"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil || v < 1 {
		return fallback
	}
	return v
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	addr := env("JISA_DELIVERY_ADDR", ":8081")
	secret := os.Getenv("JISA_DELIVERY_SECRET")
	if secret == "" {
		log.Fatal("JISA_DELIVERY_SECRET is required: subscribers cannot verify unsigned deliveries")
	}

	var subscribers []string
	for _, raw := range strings.Split(env("JISA_DELIVERY_SUBSCRIBERS", ""), ",") {
		if s := strings.TrimSpace(raw); s != "" {
			subscribers = append(subscribers, s)
		}
	}
	if len(subscribers) == 0 {
		log.Fatal("JISA_DELIVERY_SUBSCRIBERS is required (comma-separated URLs)")
	}

	deadPath := env("JISA_DELIVERY_DEAD_LETTER", "dead_letter.jsonl")
	deadFile, err := os.OpenFile(deadPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("dead-letter file %s: %v", deadPath, err)
	}
	defer deadFile.Close()

	dispatcher := delivery.NewDispatcher(delivery.Config{
		Subscribers: subscribers,
		Secret:      []byte(secret),
		MaxAttempts: envInt("JISA_DELIVERY_MAX_ATTEMPTS", 6),
		Backoff: delivery.NewBackoff(
			500*time.Millisecond, 30*time.Second, time.Now().UnixNano()),
		DeadLetter: deadFile,
		Client:     &http.Client{Timeout: 10 * time.Second},
	})

	server := &http.Server{
		Addr:              addr,
		Handler:           delivery.NewServer(dispatcher, delivery.NewDedupe(envInt("JISA_DELIVERY_DEDUPE_MAX", 8192))).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("alertd listening on %s, %d subscriber(s), dead-letter %s",
			addr, len(subscribers), deadPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	stats := dispatcher.Stats()
	log.Printf("stopped: accepted=%d duplicate=%d delivered=%d retried=%d dead_lettered=%d",
		stats.Accepted, stats.Duplicate, stats.Delivered, stats.Retried, stats.DeadLettered)
}
