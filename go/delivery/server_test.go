package delivery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testServer(t *testing.T, upstream string) *Server {
	t.Helper()
	d := NewDispatcher(Config{
		Subscribers: []string{upstream},
		Secret:      []byte("whsec_test"),
		MaxAttempts: 2,
		Backoff:     NewBackoff(time.Millisecond, 2*time.Millisecond, 1),
		DeadLetter:  &syncBuf{},
		Client:      &http.Client{Timeout: 2 * time.Second},
		Now:         func() time.Time { return time.Unix(1750000000, 0) },
	})
	return NewServer(d, NewDedupe(128))
}

const sampleAlert = `{"stream_id":1,"ts_ns":42,"layer":"cusum","detail":"score=9"}`

func post(t *testing.T, h http.Handler, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/alerts", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPostAlertAcceptsANewAlert(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()

	rec := post(t, testServer(t, up.URL).Handler(), sampleAlert, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if got["delivery_id"] == "" || got["delivery_id"] == nil {
		t.Fatalf("no delivery_id in response: %s", rec.Body.String())
	}
}

func TestPostAlertIsIdempotentOnRepeat(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	h := testServer(t, up.URL).Handler()

	first := post(t, h, sampleAlert, map[string]string{"Idempotency-Key": "key-1"})
	second := post(t, h, sampleAlert, map[string]string{"Idempotency-Key": "key-1"})

	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", first.Code)
	}
	if second.Code != http.StatusOK {
		t.Fatalf("repeat status = %d, want 200", second.Code)
	}

	var a, b map[string]any
	json.Unmarshal(first.Body.Bytes(), &a)
	json.Unmarshal(second.Body.Bytes(), &b)
	if a["delivery_id"] != b["delivery_id"] {
		t.Fatalf("repeat returned a different delivery_id: %v vs %v", a["delivery_id"], b["delivery_id"])
	}
}

// With no caller-supplied key, identical alert content must collapse to one delivery.
func TestPostAlertDerivesAKeyFromAlertIdentity(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	h := testServer(t, up.URL).Handler()

	if rec := post(t, h, sampleAlert, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202", rec.Code)
	}
	if rec := post(t, h, sampleAlert, nil); rec.Code != http.StatusOK {
		t.Fatalf("identical alert status = %d, want 200", rec.Code)
	}
	other := `{"stream_id":2,"ts_ns":42,"layer":"cusum","detail":"score=9"}`
	if rec := post(t, h, other, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("distinct alert status = %d, want 202", rec.Code)
	}
}

func TestPostAlertRejectsBadInput(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	h := testServer(t, up.URL).Handler()

	for _, tc := range []struct{ name, body string }{
		{"not json", "{not json"},
		{"empty", ""},
		{"missing layer", `{"stream_id":1,"ts_ns":42,"detail":"x"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if rec := post(t, h, tc.body, nil); rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestHealthzReportsCounters(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	h := testServer(t, up.URL).Handler()

	post(t, h, sampleAlert, map[string]string{"Idempotency-Key": "key-1"})
	post(t, h, sampleAlert, map[string]string{"Idempotency-Key": "key-1"})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("healthz is not JSON: %v", err)
	}
	for _, k := range []string{"uptime_s", "accepted", "duplicate", "delivered", "retried", "dead_lettered"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("healthz missing %q: %s", k, rec.Body.String())
		}
	}
	if got["accepted"].(float64) != 1 {
		t.Fatalf("accepted = %v, want 1", got["accepted"])
	}
	if got["duplicate"].(float64) != 1 {
		t.Fatalf("duplicate = %v, want 1", got["duplicate"])
	}
}

func TestMethodNotAllowed(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer up.Close()
	req := httptest.NewRequest(http.MethodGet, "/v1/alerts", nil)
	rec := httptest.NewRecorder()
	testServer(t, up.URL).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
