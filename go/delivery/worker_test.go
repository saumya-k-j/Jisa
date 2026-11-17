package delivery

// Worker behaviour over the durable store, from SPEC 3.15: claim -> attempt
// each pending subscriber -> write exactly one outcome. HTTP side uses
// httptest receivers; retry *scheduling* lives in the store, and the 3.13
// jittered backoff is reused unchanged.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type hitlog struct {
	mu   sync.Mutex
	keys []string
}

func (h *hitlog) add(k string) {
	h.mu.Lock()
	h.keys = append(h.keys, k)
	h.mu.Unlock()
}

func (h *hitlog) byKey() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := map[string]int{}
	for _, k := range h.keys {
		m[k]++
	}
	return m
}

func receiver(t *testing.T, log *hitlog, status func() int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r.Header.Get("Idempotency-Key"))
		w.WriteHeader(status())
	}))
	t.Cleanup(srv.Close)
	return srv
}

func always(code int) func() int { return func() int { return code } }

// runWorkers starts n workers over s and returns a stop function.
func runWorkers(t *testing.T, s *Store, n int) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < n; i++ {
		w := NewWorker(s, NewDispatcher(Config{Secret: []byte("test-secret")}))
		go w.Run(ctx)
	}
	return cancel
}

func waitState(t *testing.T, s *Store, key, want string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if st, _ := docState(t, s, key); st == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	st, _ := docState(t, s, key)
	t.Fatalf("key %s state=%q, want %q within %v", key, st, want, within)
}

func TestWorkerDeliversAndMarksDelivered(t *testing.T) {
	log := &hitlog{}
	srv := receiver(t, log, always(200))
	s := testStore(t, func(c *StoreConfig) { c.Subscribers = []string{srv.URL} })

	stop := runWorkers(t, s, 1)
	defer stop()

	if _, _, err := s.Accept(ctxT(t), "key-deliver", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	waitState(t, s, "key-deliver", "delivered", 15*time.Second)
	if n := log.byKey()["key-deliver"]; n != 1 {
		t.Fatalf("receiver saw key %d times, want 1", n)
	}
}

func TestWorkerRetryableFailureIsRedeliveredLater(t *testing.T) {
	var calls atomic.Int64
	log := &hitlog{}
	srv := receiver(t, log, func() int {
		if calls.Add(1) == 1 {
			return 503
		}
		return 200
	})
	s := testStore(t, func(c *StoreConfig) {
		c.Subscribers = []string{srv.URL}
		c.Backoff = NewBackoff(10*time.Millisecond, 100*time.Millisecond, 42)
	})

	stop := runWorkers(t, s, 1)
	defer stop()

	if _, _, err := s.Accept(ctxT(t), "key-503", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	waitState(t, s, "key-503", "delivered", 20*time.Second)
	if got := calls.Load(); got < 2 {
		t.Fatalf("receiver called %d times, want >=2 (one 503, one success)", got)
	}
	_, doc := docState(t, s, "key-503")
	if att, _ := doc["attempts"].(int32); att < 2 {
		t.Fatalf("attempts=%d, want >=2 after a rescheduled claim", att)
	}
}

func TestWorkerPermanent4xxFailsAndDeadLetters(t *testing.T) {
	var dead syncBuffer
	log := &hitlog{}
	srv := receiver(t, log, always(404))
	s := testStore(t, func(c *StoreConfig) {
		c.Subscribers = []string{srv.URL}
		c.DeadLetter = &dead
	})

	stop := runWorkers(t, s, 1)
	defer stop()

	if _, _, err := s.Accept(ctxT(t), "key-404", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	waitState(t, s, "key-404", "failed", 15*time.Second)
	if n := log.byKey()["key-404"]; n != 1 {
		t.Fatalf("permanent 4xx retried: receiver saw %d posts, want 1", n)
	}
	var rec map[string]any
	if err := json.Unmarshal(dead.Bytes(), &rec); err != nil {
		t.Fatalf("dead-letter not written/parsable: %v (%q)", err, dead.String())
	}
}

// SPEC 3.15: a subscriber that already accepted is removed from
// pending_subscribers and never re-posted when the alert is rescheduled for
// the other subscriber.
func TestDeliveredSubscriberNotRePosted(t *testing.T) {
	okLog := &hitlog{}
	okSrv := receiver(t, okLog, always(200))

	var flakyCalls atomic.Int64
	flakyLog := &hitlog{}
	flakySrv := receiver(t, flakyLog, func() int {
		if flakyCalls.Add(1) == 1 {
			return 503
		}
		return 200
	})

	s := testStore(t, func(c *StoreConfig) {
		c.Subscribers = []string{okSrv.URL, flakySrv.URL}
		c.Backoff = NewBackoff(10*time.Millisecond, 100*time.Millisecond, 7)
	})
	stop := runWorkers(t, s, 1)
	defer stop()

	if _, _, err := s.Accept(ctxT(t), "key-partial", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	waitState(t, s, "key-partial", "delivered", 20*time.Second)

	if n := okLog.byKey()["key-partial"]; n != 1 {
		t.Fatalf("healthy subscriber re-posted: %d posts, want exactly 1", n)
	}
	if n := flakyLog.byKey()["key-partial"]; n != 2 {
		t.Fatalf("flaky subscriber: %d posts, want 2 (503 then 200)", n)
	}
}

// SPEC 3.15: atomic claims mean two workers never deliver the same alert --
// with a healthy receiver every key is posted exactly once even with many
// workers racing. Run under -race.
func TestManyWorkersEachAlertDeliveredOnce(t *testing.T) {
	log := &hitlog{}
	srv := receiver(t, log, always(200))
	s := testStore(t, func(c *StoreConfig) { c.Subscribers = []string{srv.URL} })

	stop := runWorkers(t, s, 4)
	defer stop()

	const n = 40
	ctx := ctxT(t)
	for i := 0; i < n; i++ {
		if _, _, err := s.Accept(ctx, key36(i), []byte(testBody)); err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		waitState(t, s, key36(i), "delivered", 30*time.Second)
	}
	for k, c := range log.byKey() {
		if c != 1 {
			t.Fatalf("key %s delivered %d times, want exactly once", k, c)
		}
	}
}

// syncBuffer is an io.Writer safe for concurrent dead-letter appends.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string { return string(s.Bytes()) }

func (s *syncBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.b.Bytes()...)
}
