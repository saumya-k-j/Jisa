// Command faultcheck is the fault-injection harness for the durable delivery
// store (SPEC 3.15 verification). It follows the engine's record/replay
// discipline: a seeded deterministic generator produces the load, every POST
// the receiver sees is appended to a JSONL record, and every assertion is
// evaluated off that record, so a run can be inspected after the fact.
//
// Scenarios:
//
//	primary  -- kill the replica-set primary mid-delivery (docker kill),
//	            assert zero lost / zero duplicated per key, measure the
//	            accept outage and the time to resume delivery.
//	worker   -- SIGKILL one of two alertd instances mid-delivery; the
//	            survivor's workers reclaim expired leases. Assert zero lost;
//	            raw replay POSTs (same Idempotency-Key) are counted and
//	            reported, per-key delivery is asserted unique.
//	replay   -- re-POST every accepted request; assert zero new documents
//	            (every response is duplicate:true with the original
//	            delivery_id) and zero new deliveries.
//
// Std-lib only, like the rest of go/. Docker is driven through the CLI.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type post struct {
	T   time.Time `json:"t"`
	Key string    `json:"key"`
}

type recorder struct {
	mu    sync.Mutex
	posts []post
	file  *os.File
	delay time.Duration
}

func (r *recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	p := post{T: time.Now(), Key: req.Header.Get("Idempotency-Key")}
	r.mu.Lock()
	r.posts = append(r.posts, p)
	line, _ := json.Marshal(p)
	_, _ = r.file.Write(append(line, '\n'))
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (r *recorder) snapshot() []post {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]post(nil), r.posts...)
}

func (r *recorder) byKey() map[string]int {
	m := map[string]int{}
	for _, p := range r.snapshot() {
		m[p.Key]++
	}
	return m
}

type result struct {
	Scenario        string `json:"scenario"`
	Seed            int64  `json:"seed"`
	N               int    `json:"n"`
	Accepted        int    `json:"accepted"`
	DeliveredUnique int    `json:"delivered_unique"`
	RawPosts        int    `json:"raw_posts"`
	DupPosts        int    `json:"dup_posts"`
	Lost            int    `json:"lost"`
	AcceptOutageMs  int64  `json:"accept_outage_ms,omitempty"`
	ResumeMs        int64  `json:"resume_delivery_ms,omitempty"`
	Pass            bool   `json:"pass"`
	Reason          string `json:"reason,omitempty"`
}

var (
	scenario = flag.String("scenario", "", "primary|worker|replay")
	n        = flag.Int("n", 200, "alerts to generate")
	seed     = flag.Int64("seed", 1, "generator seed (keys are derived from it)")
	alertd   = flag.String("alertd", "./alertd", "path to the alertd binary")
	record   = flag.String("record", "faultcheck_record.jsonl", "receiver record path")
	timeout  = flag.Duration("timeout", 180*time.Second, "overall scenario timeout")
)

func main() {
	flag.Parse()
	if os.Getenv("MONGODB_URI") == "" {
		fatal("MONGODB_URI is required")
	}

	f, err := os.Create(*record)
	if err != nil {
		fatal("record file: %v", err)
	}
	defer f.Close()
	rec := &recorder{file: f}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatal("listen: %v", err)
	}
	go func() { _ = http.Serve(ln, rec) }()
	recvURL := "http://" + ln.Addr().String() + "/hook"

	res := result{Scenario: *scenario, Seed: *seed, N: *n, Pass: true}
	switch *scenario {
	case "primary":
		runPrimary(rec, recvURL, &res)
	case "worker":
		runWorker(rec, recvURL, &res)
	case "replay":
		runReplay(rec, recvURL, &res)
	default:
		fatal("unknown -scenario %q", *scenario)
	}

	posts := rec.byKey()
	res.RawPosts = len(rec.snapshot())
	res.DeliveredUnique = len(posts)
	for _, c := range posts {
		if c > 1 {
			res.DupPosts += c - 1
		}
	}
	res.Lost = res.Accepted - res.DeliveredUnique
	if res.Lost != 0 {
		res.Pass = false
		res.Reason += fmt.Sprintf("lost=%d;", res.Lost)
	}

	out, _ := json.Marshal(res)
	fmt.Printf("RESULT %s\n", out)
	if !res.Pass {
		os.Exit(1)
	}
}

func key(i int) string { return fmt.Sprintf("fc-%d-%05d", *seed, i) }

func body(i int) string {
	return fmt.Sprintf(`{"stream_id":%d,"ts_ns":%d,"layer":"cusum","detail":"faultcheck seed=%d i=%d"}`,
		i%7, time.Now().UnixNano(), *seed, i)
}

// startAlertd launches one alertd instance in durable mode and waits for
// /healthz. Extra env entries override defaults.
func startAlertd(recvURL, addr string, extraEnv ...string) *exec.Cmd {
	cmd := exec.Command(*alertd)
	cmd.Env = append(os.Environ(),
		"JISA_DELIVERY_ADDR="+addr,
		"JISA_DELIVERY_SECRET=whsec_faultcheck",
		"JISA_DELIVERY_SUBSCRIBERS="+recvURL,
		"JISA_DELIVERY_DEAD_LETTER="+os.DevNull,
		"JISA_DELIVERY_DB="+dbName(),
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatal("start alertd: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://127.0.0.1" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return cmd
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fatal("alertd on %s never became healthy", addr)
	return nil
}

var dbOnce struct {
	sync.Once
	name string
}

// dbName isolates each harness run in its own database.
func dbName() string {
	dbOnce.Do(func() {
		dbOnce.name = fmt.Sprintf("jisa_fc_%d_%d", os.Getpid(), time.Now().UnixNano()%1e6)
	})
	return dbOnce.name
}

type acceptResp struct {
	DeliveryID string `json:"delivery_id"`
	Duplicate  bool   `json:"duplicate"`
}

// acceptOne POSTs one alert, retrying until the service accepts it (during
// failover accepts legitimately fail; at-least-once starts at the caller).
// Returns the response and the time of the successful accept.
func acceptOne(base string, i int) (acceptResp, time.Time) {
	payload := []byte(body(i))
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("POST", base+"/v1/alerts", bytes.NewReader(payload))
		req.Header.Set("Idempotency-Key", key(i))
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			var ar acceptResp
			dec := json.NewDecoder(resp.Body)
			code := resp.StatusCode
			derr := dec.Decode(&ar)
			resp.Body.Close()
			if (code == 202 || code == 200) && derr == nil {
				return ar, time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fatal("alert %d never accepted", i)
	return acceptResp{}, time.Time{}
}

func waitDelivered(rec *recorder, want int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(rec.byKey()) >= want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// currentPrimary asks each compose node whether it is the writable primary.
func currentPrimary() string {
	nodes := map[string]string{
		"jisa-mongo1": "27017",
		"jisa-mongo2": "27018",
		"jisa-mongo3": "27019",
	}
	for c, port := range nodes {
		out, err := exec.Command("docker", "exec", c, "mongosh", "--port", port,
			"--quiet", "--eval", "db.hello().isWritablePrimary").Output()
		if err == nil && strings.TrimSpace(string(out)) == "true" {
			return c
		}
	}
	return ""
}

func runPrimary(rec *recorder, recvURL string, res *result) {
	cmd := startAlertd(recvURL, ":8085")
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	base := "http://127.0.0.1:8085"

	half := *n / 2
	var lastBefore, firstAfter time.Time
	for i := 0; i < half; i++ {
		_, t := acceptOne(base, i)
		lastBefore = t
	}

	primary := currentPrimary()
	if primary == "" {
		fatal("no writable primary found")
	}
	tKill := time.Now()
	if err := exec.Command("docker", "kill", primary).Run(); err != nil {
		fatal("docker kill %s: %v", primary, err)
	}
	defer func() { _ = exec.Command("docker", "start", primary).Run() }()

	for i := half; i < *n; i++ {
		_, t := acceptOne(base, i)
		if firstAfter.IsZero() {
			firstAfter = t
		}
	}
	res.Accepted = *n
	res.AcceptOutageMs = firstAfter.Sub(lastBefore).Milliseconds()

	if !waitDelivered(rec, *n, *timeout) {
		res.Pass = false
		res.Reason += "not all delivered;"
	}
	for _, p := range rec.snapshot() {
		if p.T.After(tKill) {
			res.ResumeMs = p.T.Sub(tKill).Milliseconds()
			break
		}
	}
	for k, c := range rec.byKey() {
		if c != 1 {
			res.Pass = false
			res.Reason += fmt.Sprintf("key %s posted %d times;", k, c)
		}
	}
}

func runWorker(rec *recorder, recvURL string, res *result) {
	rec.delay = 250 * time.Millisecond // keep deliveries in flight at kill time

	a := startAlertd(recvURL, ":8085", "JISA_DELIVERY_LEASE_SECONDS=3", "JISA_DELIVERY_WORKERS=2")
	b := startAlertd(recvURL, ":8086", "JISA_DELIVERY_LEASE_SECONDS=3", "JISA_DELIVERY_WORKERS=2")
	defer func() { _ = b.Process.Kill(); _, _ = b.Process.Wait() }()
	killed := false
	defer func() {
		if !killed {
			_ = a.Process.Kill()
			_, _ = a.Process.Wait()
		}
	}()

	base := "http://127.0.0.1:8085"
	for i := 0; i < *n; i++ {
		acceptOne(base, i)
	}
	res.Accepted = *n

	// SIGKILL instance A while deliveries are in flight: no outcome writes,
	// leases expire, B finishes the work.
	for len(rec.byKey()) < *n/4 {
		time.Sleep(50 * time.Millisecond)
	}
	if err := a.Process.Signal(syscall.SIGKILL); err != nil {
		fatal("sigkill: %v", err)
	}
	_, _ = a.Process.Wait()
	killed = true

	if !waitDelivered(rec, *n, *timeout) {
		res.Pass = false
		res.Reason += "not all delivered after worker kill;"
	}
	// Raw replays with the same Idempotency-Key are the documented
	// at-least-once residue of a kill between POST and outcome write; they
	// are counted in dup_posts by main. Delivery is asserted per key.
}

func runReplay(rec *recorder, recvURL string, res *result) {
	cmd := startAlertd(recvURL, ":8085")
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	base := "http://127.0.0.1:8085"

	ids := make([]string, *n)
	for i := 0; i < *n; i++ {
		ar, _ := acceptOne(base, i)
		if ar.Duplicate {
			res.Pass = false
			res.Reason += fmt.Sprintf("first accept of %d reported duplicate;", i)
		}
		ids[i] = ar.DeliveryID
	}
	res.Accepted = *n
	if !waitDelivered(rec, *n, *timeout) {
		res.Pass = false
		res.Reason += "not all delivered;"
	}
	before := len(rec.snapshot())

	// Replay every request: every response must be duplicate:true with the
	// original delivery id, and nothing new may reach the receiver.
	for i := 0; i < *n; i++ {
		ar, _ := acceptOne(base, i)
		if !ar.Duplicate || ar.DeliveryID != ids[i] {
			res.Pass = false
			res.Reason += fmt.Sprintf("replay %d: duplicate=%v id=%s want id=%s;", i, ar.Duplicate, ar.DeliveryID, ids[i])
		}
	}
	time.Sleep(3 * time.Second)
	if after := len(rec.snapshot()); after != before {
		res.Pass = false
		res.Reason += fmt.Sprintf("replay caused %d new deliveries;", after-before)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "faultcheck: "+format+"\n", args...)
	os.Exit(2)
}
