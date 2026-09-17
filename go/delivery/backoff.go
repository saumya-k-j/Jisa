package delivery

import (
	"math/rand"
	"sync"
	"time"
)

// Backoff produces retry delays with full jitter: the delay for attempt n is
// drawn uniformly from [0, min(base*2^n, cap)]. Full jitter rather than plain
// exponential because synchronised retries from many senders re-converge into
// the bursts that caused the failure.
type Backoff struct {
	base     time.Duration
	capDelay time.Duration

	mu  sync.Mutex
	rng *rand.Rand
}

// NewBackoff returns a Backoff seeded deterministically, so a replayed run
// draws the same delays.
func NewBackoff(base, capDelay time.Duration, seed int64) *Backoff {
	if base <= 0 {
		base = time.Millisecond
	}
	if capDelay < base {
		capDelay = base
	}
	return &Backoff{base: base, capDelay: capDelay, rng: rand.New(rand.NewSource(seed))}
}

// Delay returns the jittered delay to wait before the given retry attempt.
func (b *Backoff) Delay(attempt int) time.Duration {
	ceiling := b.capDelay
	if attempt > 0 {
		// Double step by step and stop at the cap, so a large attempt count
		// cannot overflow the shift into a small or negative duration.
		scaled := b.base
		for i := 0; i < attempt && scaled < b.capDelay; i++ {
			scaled *= 2
		}
		if scaled < ceiling {
			ceiling = scaled
		}
	} else if b.base < ceiling {
		ceiling = b.base
	}
	if ceiling <= 0 {
		return 0
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Duration(b.rng.Int63n(int64(ceiling) + 1))
}
