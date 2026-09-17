package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// The signed payload is "<t>.<body>" -- this test computes the expected MAC
// independently of the production formatting code, so a change to either the
// payload construction or the header layout fails here.
func TestSignMatchesIndependentlyComputedMAC(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"stream_id":1,"ts_ns":42,"layer":"cusum","detail":"x"}`)
	const ts int64 = 1750000000

	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.%s", ts, body)
	want := fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))

	if got := Sign(secret, ts, body); got != want {
		t.Fatalf("Sign() = %q, want %q", got, want)
	}
}

func TestSignRoundTripsThroughVerify(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"layer":"conformal"}`)
	if err := Verify(secret, Sign(secret, 1750000000, body), body); err != nil {
		t.Fatalf("Verify() on a freshly signed body: %v", err)
	}
}

func TestVerifyRejectsAlteredInput(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte(`{"layer":"conformal"}`)
	header := Sign(secret, 1750000000, body)

	cases := []struct {
		name   string
		secret []byte
		header string
		body   []byte
	}{
		{"tampered body", secret, header, []byte(`{"layer":"rules"}`)},
		{"wrong secret", []byte("whsec_other"), header, body},
		{"wrong mac", secret, "t=1750000000,v1=" + strings.Repeat("00", 32), body},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := Verify(tc.secret, tc.header, tc.body); err == nil {
				t.Fatal("Verify() = nil, want error")
			}
		})
	}
}

func TestVerifyRejectsMalformedHeader(t *testing.T) {
	secret := []byte("whsec_test")
	body := []byte("{}")
	for _, header := range []string{
		"",
		"garbage",
		"t=1750000000",
		"v1=abcd",
		"t=notanumber,v1=abcd",
		"t=1750000000,v1=nothex!!",
	} {
		t.Run(header, func(t *testing.T) {
			if err := Verify(secret, header, body); err == nil {
				t.Fatalf("Verify(%q) = nil, want error", header)
			}
		})
	}
}
