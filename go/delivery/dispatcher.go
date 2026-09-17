package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// maxBodyBytes bounds how much of an inbound request body is read.
const maxBodyBytes = 1 << 20

// Config wires a Dispatcher. Zero values fall back to production defaults.
type Config struct {
	Subscribers []string
	Secret      []byte
	MaxAttempts int
	Backoff     *Backoff
	DeadLetter  io.Writer
	Client      *http.Client
	Now         func() time.Time
}

// Stats is the delivery counter set reported by /healthz.
type Stats struct {
	Accepted     int64 `json:"accepted"`
	Duplicate    int64 `json:"duplicate"`
	Delivered    int64 `json:"delivered"`
	Retried      int64 `json:"retried"`
	DeadLettered int64 `json:"dead_lettered"`
}

// Dispatcher delivers alert payloads to every configured subscriber.
type Dispatcher struct {
	cfg Config

	deadMu sync.Mutex

	accepted     int64
	duplicate    int64
	delivered    int64
	retried      int64
	deadLettered int64
}

// NewDispatcher returns a Dispatcher with defaults applied to cfg.
func NewDispatcher(cfg Config) *Dispatcher {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Backoff == nil {
		cfg.Backoff = NewBackoff(500*time.Millisecond, 30*time.Second, time.Now().UnixNano())
	}
	return &Dispatcher{cfg: cfg}
}

// Stats returns a snapshot of the delivery counters.
func (d *Dispatcher) Stats() Stats {
	return Stats{
		Accepted:     atomic.LoadInt64(&d.accepted),
		Duplicate:    atomic.LoadInt64(&d.duplicate),
		Delivered:    atomic.LoadInt64(&d.delivered),
		Retried:      atomic.LoadInt64(&d.retried),
		DeadLettered: atomic.LoadInt64(&d.deadLettered),
	}
}

func (d *Dispatcher) markAccepted()  { atomic.AddInt64(&d.accepted, 1) }
func (d *Dispatcher) markDuplicate() { atomic.AddInt64(&d.duplicate, 1) }

// Deliver sends body to every subscriber under the given idempotency key,
// retrying retryable failures. It returns the first error encountered; any
// delivery that fails permanently or exhausts its attempts is dead-lettered
// before Deliver returns, so a failure is recorded rather than dropped.
func (d *Dispatcher) Deliver(ctx context.Context, key string, body []byte) error {
	var firstErr error
	for _, subscriber := range d.cfg.Subscribers {
		if err := d.deliverOne(ctx, subscriber, key, body); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (d *Dispatcher) deliverOne(ctx context.Context, url, key string, body []byte) error {
	var lastErr error

	for attempt := 0; attempt < d.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&d.retried, 1)
			timer := time.NewTimer(d.cfg.Backoff.Delay(attempt - 1))
			select {
			case <-ctx.Done():
				timer.Stop()
				lastErr = ctx.Err()
				d.deadLetter(url, key, body, attempt, lastErr)
				return lastErr
			case <-timer.C:
			}
		}

		status, err := d.attempt(ctx, url, key, body)
		switch {
		case err != nil:
			lastErr = err
			if ctx.Err() != nil {
				d.deadLetter(url, key, body, attempt+1, lastErr)
				return lastErr
			}
		case status >= 200 && status < 300:
			atomic.AddInt64(&d.delivered, 1)
			return nil
		case retryableStatus(status):
			lastErr = fmt.Errorf("delivery: %s returned %d", url, status)
		default:
			// 4xx other than 429 will not succeed on a retry; retrying only
			// burns the attempt budget and delays the dead-letter record.
			lastErr = fmt.Errorf("delivery: %s returned %d (permanent)", url, status)
			d.deadLetter(url, key, body, attempt+1, lastErr)
			return lastErr
		}
	}

	d.deadLetter(url, key, body, d.cfg.MaxAttempts, lastErr)
	return lastErr
}

func (d *Dispatcher) attempt(ctx context.Context, url, key string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	// Stable across every attempt, so a subscriber that has already applied
	// this delivery can recognise the replay and drop it.
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Jisa-Signature", Sign(d.cfg.Secret, d.cfg.Now().Unix(), body))

	resp, err := d.cfg.Client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func (d *Dispatcher) deadLetter(url, key string, body []byte, attempts int, cause error) {
	atomic.AddInt64(&d.deadLettered, 1)
	if d.cfg.DeadLetter == nil {
		return
	}

	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	record := map[string]any{
		"idempotency_key": key,
		"subscriber":      url,
		"attempts":        attempts,
		"reason":          reason,
		"recorded_at":     d.cfg.Now().UTC().Format(time.RFC3339),
	}
	if json.Valid(body) {
		record["body"] = json.RawMessage(body)
	} else {
		record["body_raw"] = string(body)
	}

	line, err := json.Marshal(record)
	if err != nil {
		return
	}

	d.deadMu.Lock()
	defer d.deadMu.Unlock()
	_, _ = d.cfg.DeadLetter.Write(append(line, '\n'))
}

func readAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
}
