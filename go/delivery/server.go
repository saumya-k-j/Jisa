package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// deliveryTimeout bounds one alert's full retry schedule.
const deliveryTimeout = 5 * time.Minute

// Alert is the ingest shape, matching the engine's alerts table
// (python/api/engine.py).
type Alert struct {
	StreamID int64  `json:"stream_id"`
	TsNs     int64  `json:"ts_ns"`
	Layer    string `json:"layer"`
	Detail   string `json:"detail"`
}

// Server exposes the ingest and status endpoints of SPEC 3.13.
type Server struct {
	dispatcher *Dispatcher
	dedupe     *Dedupe
	started    time.Time
}

// NewServer returns a Server that hands accepted alerts to dispatcher.
func NewServer(dispatcher *Dispatcher, dedupe *Dedupe) *Server {
	return &Server{dispatcher: dispatcher, dedupe: dedupe, started: time.Now()}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/alerts", s.handleAlerts)
	mux.HandleFunc("/healthz", s.handleHealthz)
	return mux
}

// DeriveKey is the fallback idempotency key: a digest over the alert's
// identity, so the same alert posted twice collapses to one delivery even when
// the caller supplies no key of its own.
func DeriveKey(a Alert) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%s|%s", a.StreamID, a.TsNs, a.Layer, a.Detail)))
	return hex.EncodeToString(sum[:])
}

func deliveryID(key string) string {
	sum := sha256.Sum256([]byte("delivery:" + key))
	return "dlv_" + hex.EncodeToString(sum[:])[:24]
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	body, err := readAll(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unreadable body"})
		return
	}
	var alert Alert
	if err := json.Unmarshal(body, &alert); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed JSON body"})
		return
	}
	if alert.Layer == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "layer is required"})
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = DeriveKey(alert)
	}

	id, duplicate := s.dedupe.Check(key, deliveryID(key))
	if duplicate {
		s.dispatcher.markDuplicate()
		writeJSON(w, http.StatusOK, map[string]any{"delivery_id": id, "duplicate": true})
		return
	}

	s.dispatcher.markAccepted()
	// Answer immediately: the retry schedule can run for minutes and the
	// engine must not block on a slow subscriber. The queue is in-process, so
	// deliveries in flight are lost if the process dies (DECISIONS D-051).
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
		defer cancel()
		_ = s.dispatcher.Deliver(ctx, key, body)
	}()

	writeJSON(w, http.StatusAccepted, map[string]any{"delivery_id": id, "duplicate": false})
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	stats := s.dispatcher.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"uptime_s":      int64(time.Since(s.started).Seconds()),
		"accepted":      stats.Accepted,
		"duplicate":     stats.Duplicate,
		"delivered":     stats.Delivered,
		"retried":       stats.Retried,
		"dead_lettered": stats.DeadLettered,
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
