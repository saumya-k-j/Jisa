package delivery

import (
	"testing"
	"time"
)

// Full jitter: delay for attempt n is drawn uniformly from [0, min(base*2^n, cap)].
func TestDelayStaysWithinFullJitterBound(t *testing.T) {
	base, capd := 100*time.Millisecond, 30*time.Second
	b := NewBackoff(base, capd, 1)

	for attempt := 0; attempt < 10; attempt++ {
		ceiling := base << uint(attempt)
		if ceiling > capd || ceiling <= 0 {
			ceiling = capd
		}
		for i := 0; i < 200; i++ {
			d := b.Delay(attempt)
			if d < 0 || d > ceiling {
				t.Fatalf("attempt %d: delay %v outside [0,%v]", attempt, d, ceiling)
			}
		}
	}
}

func TestDelayNeverExceedsCapAtHighAttempts(t *testing.T) {
	capd := 5 * time.Second
	b := NewBackoff(100*time.Millisecond, capd, 7)
	for attempt := 40; attempt < 70; attempt++ {
		if d := b.Delay(attempt); d > capd {
			t.Fatalf("attempt %d: delay %v exceeds cap %v (overflow?)", attempt, d, capd)
		}
	}
}

func TestDelayIsDeterministicForAGivenSeed(t *testing.T) {
	mk := func() []time.Duration {
		b := NewBackoff(100*time.Millisecond, 30*time.Second, 99)
		out := make([]time.Duration, 12)
		for i := range out {
			out[i] = b.Delay(i % 6)
		}
		return out
	}
	a, c := mk(), mk()
	for i := range a {
		if a[i] != c[i] {
			t.Fatalf("draw %d differs across identical seeds: %v vs %v", i, a[i], c[i])
		}
	}
}

// Guards against a degenerate implementation that always returns 0 or the ceiling.
func TestDelayActuallyJitters(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, 30*time.Second, 3)
	seen := map[time.Duration]bool{}
	for i := 0; i < 100; i++ {
		seen[b.Delay(4)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("only %d distinct delays in 100 draws; jitter looks degenerate", len(seen))
	}
}
