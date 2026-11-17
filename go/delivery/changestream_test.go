package delivery

// Change-stream wakeup and resume-token persistence, from SPEC 3.15. Needs
// the replica set (change streams do not exist on standalone mongod).

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// SPEC 3.15: an insert wakes the watcher without polling delay.
func TestWatcherSeesInsert(t *testing.T) {
	s := testStore(t, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	woke := make(chan struct{}, 16)
	done := make(chan error, 1)
	go func() { done <- s.Watch(ctx, func() { woke <- struct{}{} }) }()

	// Give the stream a moment to open before inserting.
	time.Sleep(2 * time.Second)
	if _, _, err := s.Accept(ctxT(t), "key-wake", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	select {
	case <-woke:
	case <-time.After(20 * time.Second):
		t.Fatal("watcher did not observe the insert within 10s")
	}
	cancel()
	if err := <-done; err != nil && ctx.Err() == nil {
		t.Fatalf("Watch returned: %v", err)
	}
}

// SPEC 3.15: the resume token is persisted to stream_state, and a watcher
// restarted from it observes inserts that happened while it was down.
func TestResumeTokenSurvivesRestart(t *testing.T) {
	s := testStore(t, nil)

	// First watcher: see one insert, which forces a token write.
	ctx1, cancel1 := context.WithCancel(context.Background())
	woke := make(chan struct{}, 16)
	done1 := make(chan error, 1)
	go func() { done1 <- s.Watch(ctx1, func() { woke <- struct{}{} }) }()
	time.Sleep(2 * time.Second)

	if _, _, err := s.Accept(ctxT(t), "key-before-restart", []byte(testBody)); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	select {
	case <-woke:
	case <-time.After(20 * time.Second):
		t.Fatal("first watcher never woke")
	}
	cancel1()
	<-done1

	// Token must be durable in stream_state. The write is asynchronous
	// with respect to the wakeup callback, so poll briefly.
	var state bson.M
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := s.tokens.FindOne(ctxT(t), bson.D{{Key: "_id", Value: "alerts_watch"}}).Decode(&state)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("resume token not persisted: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if state["token"] == nil {
		t.Fatalf("stream_state doc has no token: %v", state)
	}

	// Insert while no watcher is running.
	if _, _, err := s.Accept(ctxT(t), "key-during-downtime", []byte(testBody)); err != nil {
		t.Fatalf("Accept during downtime: %v", err)
	}

	// Second watcher resumes from the stored token and must see the
	// downtime insert.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	woke2 := make(chan struct{}, 16)
	done2 := make(chan error, 1)
	go func() { done2 <- s.Watch(ctx2, func() { woke2 <- struct{}{} }) }()

	select {
	case <-woke2:
	case <-time.After(20 * time.Second):
		t.Fatal("restarted watcher did not replay the insert that happened while it was down")
	}
	cancel2()
	if err := <-done2; err != nil && ctx2.Err() == nil {
		t.Fatalf("Watch returned: %v", err)
	}
}
