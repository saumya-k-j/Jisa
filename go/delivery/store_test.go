package delivery

// Tests written from SPEC 3.15 before the implementation existed. They run
// only when MONGODB_URI points at a deployment (the compose replica set of
// deploy/docker-compose.mongo.yml); without it they skip, so `go test ./...`
// stays green in environments without MongoDB.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const testBody = `{"stream_id":1,"ts_ns":1750000000000,"layer":"cusum","detail":"score=9.2>thr=8.0"}`

// testStore opens a Store against MONGODB_URI in a database unique to this
// test, dropped on cleanup. Skips when MONGODB_URI is unset.
func testStore(t *testing.T, mut func(*StoreConfig)) *Store {
	t.Helper()
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		t.Skip("MONGODB_URI unset; durable-store tests need the compose replica set")
	}
	cfg := StoreConfig{
		URI:         uri,
		Database:    fmt.Sprintf("jisa_test_%d", time.Now().UnixNano()),
		Subscribers: []string{"http://127.0.0.1:1/unreachable"},
		TTLSeconds:  3600,
		Lease:       30 * time.Second,
		MaxAttempts: 6,
		Backoff:     NewBackoff(time.Millisecond, 50*time.Millisecond, 42),
	}
	if mut != nil {
		mut(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := OpenStore(ctx, cfg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = s.alerts.Database().Drop(ctx)
		_ = s.Close(ctx)
	})
	return s
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// SPEC 3.15: a duplicate-key error on idempotency_key means "already
// accepted": 200-equivalent with the original delivery id, one document.
func TestAcceptDuplicateKeyMeansAlreadyAccepted(t *testing.T) {
	s := testStore(t, nil)
	ctx := ctxT(t)

	id1, dup, err := s.Accept(ctx, "key-1", []byte(testBody))
	if err != nil || dup {
		t.Fatalf("first Accept: id=%q dup=%v err=%v", id1, dup, err)
	}
	id2, dup, err := s.Accept(ctx, "key-1", []byte(testBody))
	if err != nil {
		t.Fatalf("second Accept: %v", err)
	}
	if !dup {
		t.Fatal("second Accept with the same key must report duplicate")
	}
	if id2 != id1 {
		t.Fatalf("duplicate must return the original delivery id: %q != %q", id2, id1)
	}
	n, err := s.alerts.CountDocuments(ctx, bson.D{})
	if err != nil || n != 1 {
		t.Fatalf("want exactly 1 document, got %d (err %v)", n, err)
	}
}

// SPEC 3.15: accept is durable -- a key accepted before a restart is still a
// duplicate after one. (This is the capability D-051 explicitly lacked.)
func TestAcceptSurvivesRestart(t *testing.T) {
	s := testStore(t, nil)
	ctx := ctxT(t)

	if _, _, err := s.Accept(ctx, "key-restart", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	cfg := s.cfg
	reopened, err := OpenStore(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close(ctx)

	_, dup, err := reopened.Accept(ctx, "key-restart", []byte(testBody))
	if err != nil {
		t.Fatalf("Accept after reopen: %v", err)
	}
	if !dup {
		t.Fatal("key accepted before restart must be a duplicate after restart")
	}
}

// SPEC 3.15: the claim is a single atomic findOneAndUpdate; under contention
// every document is claimed exactly once.
func TestClaimIsAtomicUnderContention(t *testing.T) {
	s := testStore(t, nil)
	ctx := ctxT(t)

	const docs = 50
	for i := 0; i < docs; i++ {
		if _, _, err := s.Accept(ctx, fmt.Sprintf("key-%03d", i), []byte(testBody)); err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				c, err := s.Claim(ctx, fmt.Sprintf("w%d", worker))
				if err != nil {
					t.Errorf("Claim: %v", err)
					return
				}
				if c == nil {
					return
				}
				mu.Lock()
				seen[c.Key]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != docs {
		t.Fatalf("claimed %d distinct keys, want %d", len(seen), docs)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %s claimed %d times; atomic claim must hand each document to exactly one worker", k, n)
		}
	}
}

// SPEC 3.15: filter is next_attempt_at <= now; a future retry is not
// claimable, and the earliest next_attempt_at is claimed first.
func TestClaimRespectsAndOrdersNextAttemptAt(t *testing.T) {
	s := testStore(t, nil)
	ctx := ctxT(t)

	for i, key := range []string{"due-later", "due-first", "due-second"} {
		if _, _, err := s.Accept(ctx, key, []byte(testBody)); err != nil {
			t.Fatalf("Accept %d: %v", i, err)
		}
	}
	now := time.Now().UTC()
	set := func(key string, at time.Time) {
		if _, err := s.alerts.UpdateOne(ctx,
			bson.D{{Key: "idempotency_key", Value: key}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "next_attempt_at", Value: at}}}}); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	set("due-later", now.Add(time.Hour))
	set("due-first", now.Add(-2*time.Second))
	set("due-second", now.Add(-time.Second))

	c1, err := s.Claim(ctx, "w0")
	if err != nil || c1 == nil || c1.Key != "due-first" {
		t.Fatalf("first claim: %+v err=%v, want due-first", c1, err)
	}
	c2, err := s.Claim(ctx, "w0")
	if err != nil || c2 == nil || c2.Key != "due-second" {
		t.Fatalf("second claim: %+v err=%v, want due-second", c2, err)
	}
	c3, err := s.Claim(ctx, "w0")
	if err != nil || c3 != nil {
		t.Fatalf("third claim: %+v err=%v, want nil (only future work remains)", c3, err)
	}
}

// SPEC 3.15: in_flight with an expired lease is claimable again (worker died
// mid-delivery), and attempts increments on each claim.
func TestLeaseExpiryReclaimsInFlight(t *testing.T) {
	s := testStore(t, func(c *StoreConfig) { c.Lease = 2 * time.Second })
	ctx := ctxT(t)

	if _, _, err := s.Accept(ctx, "key-lease", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	c1, err := s.Claim(ctx, "dying-worker")
	if err != nil || c1 == nil {
		t.Fatalf("claim: %+v err=%v", c1, err)
	}
	if c1.Attempts != 1 {
		t.Fatalf("first claim attempts=%d, want 1", c1.Attempts)
	}

	// Worker dies here: no outcome is ever written.
	if c, _ := s.Claim(ctx, "w2"); c != nil {
		t.Fatalf("lease still live: claim must return nil, got %+v", c)
	}
	time.Sleep(2500 * time.Millisecond)

	c2, err := s.Claim(ctx, "w2")
	if err != nil || c2 == nil {
		t.Fatalf("claim after lease expiry: %+v err=%v", c2, err)
	}
	if c2.Key != "key-lease" || c2.Attempts != 2 {
		t.Fatalf("reclaim got key=%s attempts=%d, want key-lease/2", c2.Key, c2.Attempts)
	}
}

// SPEC 3.15 state machine outcomes.
func TestOutcomeTransitions(t *testing.T) {
	var dead bytes.Buffer
	s := testStore(t, func(c *StoreConfig) {
		c.Subscribers = []string{"http://a/hook", "http://b/hook"}
		c.DeadLetter = &dead
	})
	ctx := ctxT(t)

	state := func(key string) (st string, doc bson.M) {
		t.Helper()
		res := s.alerts.FindOne(ctx, bson.D{{Key: "idempotency_key", Value: key}})
		if err := res.Decode(&doc); err != nil {
			t.Fatalf("find %s: %v", key, err)
		}
		st, _ = doc["state"].(string)
		return st, doc
	}

	// delivered: state delivered, delivered_at set, nothing pending.
	if _, _, err := s.Accept(ctx, "key-ok", []byte(testBody)); err != nil {
		t.Fatal(err)
	}
	c, _ := s.Claim(ctx, "w0")
	if c == nil {
		t.Fatal("claim key-ok")
	}
	if err := s.Delivered(ctx, c); err != nil {
		t.Fatalf("Delivered: %v", err)
	}
	st, doc := state("key-ok")
	if st != "delivered" || doc["delivered_at"] == nil {
		t.Fatalf("want delivered with delivered_at set, got state=%s doc=%v", st, doc)
	}

	// rescheduled: back to pending, future next_attempt_at, delivered
	// subscriber removed so it is never re-posted.
	if _, _, err := s.Accept(ctx, "key-retry", []byte(testBody)); err != nil {
		t.Fatal(err)
	}
	c, _ = s.Claim(ctx, "w0")
	if c == nil || len(c.Pending) != 2 {
		t.Fatalf("claim key-retry: %+v", c)
	}
	before := time.Now().UTC()
	if err := s.Reschedule(ctx, c, []string{"http://b/hook"}); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	st, doc = state("key-retry")
	naa, _ := doc["next_attempt_at"].(bson.DateTime)
	// next_attempt_at = now + full-jitter delay; the delay can draw ~0 and
	// Mongo stores millisecond precision, so compare against before
	// truncated to the stored precision rather than demanding strict After.
	if st != "pending" || naa.Time().Before(before.Truncate(time.Millisecond)) {
		t.Fatalf("want pending with future next_attempt_at, got state=%s next=%v", st, naa.Time())
	}
	pend, _ := doc["pending_subscribers"].(bson.A)
	if len(pend) != 1 || pend[0] != "http://b/hook" {
		t.Fatalf("pending_subscribers must shrink to the undelivered one, got %v", pend)
	}
	if naa.Time().After(before.Add(s.cfg.Backoff.capDelay + time.Second)) {
		t.Fatalf("next_attempt_at beyond backoff cap: %v", naa.Time())
	}

	// key-retry is due again almost immediately (jitter can draw ~0), so
	// drain it first or the next claim hands it out instead of key-dead.
	cr, err := s.Claim(ctx, "w0")
	if err != nil || cr == nil || cr.Key != "key-retry" {
		t.Fatalf("re-claim of rescheduled alert: %+v err=%v, want key-retry", cr, err)
	}
	if err := s.Delivered(ctx, cr); err != nil {
		t.Fatalf("Delivered(key-retry): %v", err)
	}

	// failed: terminal, dead-letter record appended (3.13 format).
	if _, _, err := s.Accept(ctx, "key-dead", []byte(testBody)); err != nil {
		t.Fatal(err)
	}
	c, _ = s.Claim(ctx, "w0")
	if c == nil || c.Key != "key-dead" {
		t.Fatalf("claim key-dead, got %+v", c)
	}
	if err := s.Failed(ctx, c, fmt.Errorf("delivery: http://a/hook returned 404 (permanent)")); err != nil {
		t.Fatalf("Failed: %v", err)
	}
	st, _ = state("key-dead")
	if st != "failed" {
		t.Fatalf("want failed, got %s", st)
	}
	var rec map[string]any
	if err := json.Unmarshal(dead.Bytes(), &rec); err != nil {
		t.Fatalf("dead-letter line is not JSON: %v (%q)", err, dead.String())
	}
	if rec["idempotency_key"] != "key-dead" {
		t.Fatalf("dead-letter record: %v", rec)
	}
}

// SPEC 3.15 indexes: unique idempotency_key, {state,next_attempt_at} for
// claims, TTL on delivered_at with the configured expireAfterSeconds.
func TestIndexes(t *testing.T) {
	s := testStore(t, func(c *StoreConfig) { c.TTLSeconds = 1234 })
	ctx := ctxT(t)

	cur, err := s.alerts.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var idx []struct {
		Key                bson.D `bson:"key"`
		Unique             bool   `bson:"unique"`
		ExpireAfterSeconds *int32 `bson:"expireAfterSeconds"`
	}
	if err := cur.All(ctx, &idx); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}

	has := func(d bson.D, field string) bool {
		for _, e := range d {
			if e.Key == field {
				return true
			}
		}
		return false
	}
	var unique, claim, ttl bool
	for _, ix := range idx {
		switch {
		case has(ix.Key, "idempotency_key"):
			unique = ix.Unique
		case has(ix.Key, "state") && has(ix.Key, "next_attempt_at"):
			claim = true
		case has(ix.Key, "delivered_at"):
			ttl = ix.ExpireAfterSeconds != nil && *ix.ExpireAfterSeconds == 1234
		}
	}
	if !unique || !claim || !ttl {
		t.Fatalf("index set wrong: unique=%v claim=%v ttl=%v (%v)", unique, claim, ttl, idx)
	}
}
