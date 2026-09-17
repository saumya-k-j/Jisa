package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testDispatcher(t *testing.T, subs []string, dead *syncBuf, maxAttempts int) *Dispatcher {
	t.Helper()
	return NewDispatcher(Config{
		Subscribers: subs,
		Secret:      []byte("whsec_test"),
		MaxAttempts: maxAttempts,
		Backoff:     NewBackoff(time.Millisecond, 4*time.Millisecond, 1),
		DeadLetter:  dead,
		Client:      &http.Client{Timeout: 2 * time.Second},
		Now:         func() time.Time { return time.Unix(1750000000, 0) },
	})
}

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestDeliverReachesEverySubscriber(t *testing.T) {
	var a, b int32
	sa := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&a, 1)
	}))
	defer sa.Close()
	sb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&b, 1)
	}))
	defer sb.Close()

	d := testDispatcher(t, []string{sa.URL, sb.URL}, &syncBuf{}, 3)
	if err := d.Deliver(context.Background(), "key-1", []byte(`{"layer":"rules"}`)); err != nil {
		t.Fatalf("Deliver() = %v", err)
	}
	if atomic.LoadInt32(&a) != 1 || atomic.LoadInt32(&b) != 1 {
		t.Fatalf("subscriber hit counts = %d, %d; want 1, 1", a, b)
	}
	if got := d.Stats().Delivered; got != 2 {
		t.Fatalf("Delivered = %d, want 2", got)
	}
}

func TestDeliverCarriesAValidSignatureAndIdempotencyKey(t *testing.T) {
	var gotKey, gotSig string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		gotSig = r.Header.Get("Jisa-Signature")
		gotBody, _ = readAll(r)
	}))
	defer srv.Close()

	body := []byte(`{"layer":"conformal","detail":"score=0.9"}`)
	d := testDispatcher(t, []string{srv.URL}, &syncBuf{}, 3)
	if err := d.Deliver(context.Background(), "key-abc", body); err != nil {
		t.Fatalf("Deliver() = %v", err)
	}
	if gotKey != "key-abc" {
		t.Fatalf("Idempotency-Key = %q, want %q", gotKey, "key-abc")
	}
	if err := Verify([]byte("whsec_test"), gotSig, gotBody); err != nil {
		t.Fatalf("signature on delivered body did not verify: %v", err)
	}
}

func TestDeliverRetriesRetryableStatusThenSucceeds(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusTooManyRequests, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var attempts int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if atomic.AddInt32(&attempts, 1) < 3 {
					w.WriteHeader(status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			d := testDispatcher(t, []string{srv.URL}, &syncBuf{}, 5)
			if err := d.Deliver(context.Background(), "key-1", []byte("{}")); err != nil {
				t.Fatalf("Deliver() = %v", err)
			}
			if n := atomic.LoadInt32(&attempts); n != 3 {
				t.Fatalf("attempts = %d, want 3", n)
			}
			if d.Stats().Retried == 0 {
				t.Fatal("Retried counter never incremented")
			}
		})
	}
}

func TestDeliverDoesNotRetryPermanentFailure(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	dead := &syncBuf{}
	d := testDispatcher(t, []string{srv.URL}, dead, 5)
	if err := d.Deliver(context.Background(), "key-1", []byte("{}")); err == nil {
		t.Fatal("Deliver() = nil, want error on permanent failure")
	}
	if n := atomic.LoadInt32(&attempts); n != 1 {
		t.Fatalf("attempts = %d, want 1 (400 must not be retried)", n)
	}
	if !strings.Contains(dead.String(), "key-1") {
		t.Fatal("permanent failure was not dead-lettered")
	}
}

func TestDeliverDeadLettersAfterMaxAttempts(t *testing.T) {
	var attempts int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	dead := &syncBuf{}
	d := testDispatcher(t, []string{srv.URL}, dead, 3)
	if err := d.Deliver(context.Background(), "key-xyz", []byte(`{"layer":"ewma"}`)); err == nil {
		t.Fatal("Deliver() = nil, want error after exhausting attempts")
	}
	if n := atomic.LoadInt32(&attempts); n != 3 {
		t.Fatalf("attempts = %d, want MaxAttempts=3", n)
	}
	if d.Stats().DeadLettered != 1 {
		t.Fatalf("DeadLettered = %d, want 1", d.Stats().DeadLettered)
	}

	var rec map[string]any
	line := strings.TrimSpace(dead.String())
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("dead-letter line is not JSON: %v (%q)", err, line)
	}
	if rec["idempotency_key"] != "key-xyz" {
		t.Fatalf("dead-letter idempotency_key = %v, want key-xyz", rec["idempotency_key"])
	}
}

func TestDeliverKeepsTheIdempotencyKeyStableAcrossRetries(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	d := testDispatcher(t, []string{srv.URL}, &syncBuf{}, 4)
	_ = d.Deliver(context.Background(), "stable-key", []byte("{}"))

	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 4 {
		t.Fatalf("saw %d attempts, want 4", len(keys))
	}
	for i, k := range keys {
		if k != "stable-key" {
			t.Fatalf("attempt %d used key %q, want stable-key", i, k)
		}
	}
}

func TestDeliverStopsWhenContextIsCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	d := testDispatcher(t, []string{srv.URL}, &syncBuf{}, 100)
	done := make(chan struct{})
	go func() { _ = d.Deliver(ctx, "key-1", []byte("{}")); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Deliver() ignored a cancelled context")
	}
}
