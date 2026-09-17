package delivery

import "sync"

// Dedupe remembers idempotency keys that have already been accepted, bounded
// by capacity. Eviction is insertion-ordered rather than LRU: the aim is to
// stop unbounded growth, and a key old enough to be evicted is past any
// realistic retry window.
type Dedupe struct {
	mu    sync.Mutex
	max   int
	ids   map[string]string
	order []string
}

// NewDedupe returns a Dedupe holding at most max keys.
func NewDedupe(max int) *Dedupe {
	if max < 1 {
		max = 1
	}
	return &Dedupe{max: max, ids: make(map[string]string, max)}
}

// Check records key with the given delivery id when it is new, returning
// (id, false). When key has been seen it returns the original id and true,
// so a caller can reply to a replay with the same delivery id it first issued.
func (d *Dedupe) Check(key, id string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if existing, ok := d.ids[key]; ok {
		return existing, true
	}
	if len(d.order) >= d.max {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.ids, oldest)
	}
	d.ids[key] = id
	d.order = append(d.order, key)
	return id, false
}

// Len reports how many keys are currently retained.
func (d *Dedupe) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.ids)
}
