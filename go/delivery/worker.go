package delivery

// Worker: claims durable alerts and attempts delivery, SPEC 3.15. The claim
// (findOneAndUpdate) is what makes concurrency safe; the worker itself is a
// plain loop. Wakeups come from the change stream; a ticker sweeps for due
// retries and expired leases, which the stream cannot signal.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// sweepInterval is the backstop poll for due retries and expired leases.
const sweepInterval = 500 * time.Millisecond

// Worker drains the store. Run as many as JISA_DELIVERY_WORKERS; each is
// identified in the claim's worker field for post-mortem reads.
type Worker struct {
	store *Store
	disp  *Dispatcher
	id    string
	wake  chan struct{}
}

// NewWorker wires a worker to a store and a dispatcher (used for the HTTP
// attempt and signing only; scheduling lives in the store).
func NewWorker(s *Store, d *Dispatcher) *Worker {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	return &Worker{
		store: s,
		disp:  d,
		id:    "w-" + hex.EncodeToString(buf),
		wake:  make(chan struct{}, 1),
	}
}

// Run blocks until ctx is cancelled: watch the change stream for inserts,
// claim and process everything due, sleep until woken or the next sweep.
func (w *Worker) Run(ctx context.Context) {
	go func() {
		for ctx.Err() == nil {
			err := w.store.Watch(ctx, func() {
				select {
				case w.wake <- struct{}{}:
				default:
				}
			})
			if ctx.Err() != nil {
				return
			}
			// Stream broke (e.g. failover window); back off briefly and
			// reopen. The sweep keeps progress in the meantime.
			_ = err
			time.Sleep(sweepInterval)
		}
	}()

	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		w.drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-ticker.C:
		}
	}
}

func (w *Worker) drain(ctx context.Context) {
	for {
		c, err := w.store.Claim(ctx, w.id)
		if err != nil || c == nil {
			return
		}
		w.process(ctx, c)
	}
}

// process attempts every pending subscriber once and writes exactly one
// outcome: delivered, rescheduled (3.13 jittered backoff, store-persisted),
// or failed with a dead-letter record.
func (w *Worker) process(ctx context.Context, c *Claimed) {
	var retry []string
	var lastErr error
	permanent := false

	for _, url := range c.Pending {
		status, err := w.disp.attempt(ctx, url, c.Key, c.Body)
		switch {
		case err != nil:
			lastErr = err
			retry = append(retry, url)
		case status >= 200 && status < 300:
			w.disp.markDelivered()
		case retryableStatus(status):
			lastErr = fmt.Errorf("delivery: %s returned %d", url, status)
			retry = append(retry, url)
		default:
			lastErr = fmt.Errorf("delivery: %s returned %d (permanent)", url, status)
			retry = append(retry, url)
			permanent = true
		}
	}

	switch {
	case len(retry) == 0:
		_ = w.store.Delivered(ctx, c)
	case permanent || int(c.Attempts) >= w.store.cfg.MaxAttempts:
		c.Pending = retry
		_ = w.store.Failed(ctx, c, lastErr)
	default:
		w.disp.markRetried()
		_ = w.store.Reschedule(ctx, c, retry)
	}
}
