// Command alertbench measures the durable accept path (SPEC 3.15
// verification): accepted alerts per second and accept latency quantiles,
// with the majority-write-concern insert on the measured path. Durable mode
// is asserted via /healthz before measuring, so the numbers cannot silently
// come from the in-memory path.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

var (
	base    = flag.String("url", "http://127.0.0.1:8085", "alertd base URL")
	n       = flag.Int("n", 5000, "alerts to accept")
	c       = flag.Int("c", 32, "concurrent posters")
	warmup  = flag.Int("warmup", 200, "unmeasured warmup accepts")
	keybase = flag.String("keybase", fmt.Sprintf("bench-%d", time.Now().UnixNano()), "idempotency key prefix")
)

func postOne(client *http.Client, i int, measure bool) (time.Duration, error) {
	body := fmt.Sprintf(`{"stream_id":%d,"ts_ns":%d,"layer":"cusum","detail":"bench"}`, i%7, time.Now().UnixNano())
	req, _ := http.NewRequest("POST", *base+"/v1/alerts", bytes.NewReader([]byte(body)))
	req.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%07d", *keybase, i))
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	_ = measure
	return time.Since(start), nil
}

func main() {
	flag.Parse()

	// Refuse to measure the wrong mode.
	resp, err := http.Get(*base + "/healthz")
	if err != nil {
		fmt.Fprintf(os.Stderr, "alertbench: healthz: %v\n", err)
		os.Exit(2)
	}
	var hz struct {
		Durable bool `json:"durable"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&hz)
	resp.Body.Close()
	if !hz.Durable {
		fmt.Fprintln(os.Stderr, "alertbench: alertd is not in durable mode; refusing to produce numbers for the wrong path")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for i := 0; i < *warmup; i++ {
		if _, err := postOne(client, -i-1, false); err != nil {
			fmt.Fprintf(os.Stderr, "alertbench: warmup: %v\n", err)
			os.Exit(2)
		}
	}

	lat := make([]time.Duration, *n)
	var idx int64
	var mu sync.Mutex
	next := func() int {
		mu.Lock()
		defer mu.Unlock()
		i := int(idx)
		idx++
		return i
	}

	var wg sync.WaitGroup
	errs := make(chan error, *c)
	wall := time.Now()
	for w := 0; w < *c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := &http.Client{Timeout: 30 * time.Second}
			for {
				i := next()
				if i >= *n {
					return
				}
				d, err := postOne(cl, i, true)
				if err != nil {
					errs <- fmt.Errorf("post %d: %w", i, err)
					return
				}
				lat[i] = d
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(wall)
	close(errs)
	if err := <-errs; err != nil {
		fmt.Fprintf(os.Stderr, "alertbench: %v\n", err)
		os.Exit(1)
	}

	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	q := func(p float64) float64 {
		i := int(p * float64(len(lat)-1))
		return float64(lat[i].Microseconds()) / 1000.0
	}
	out, _ := json.Marshal(map[string]any{
		"n":             *n,
		"concurrency":   *c,
		"wall_s":        elapsed.Seconds(),
		"accepts_per_s": float64(*n) / elapsed.Seconds(),
		"p50_ms":        q(0.50),
		"p99_ms":        q(0.99),
		"max_ms":        q(1.0),
	})
	fmt.Printf("RESULT %s\n", out)
}
