// Package delivery carries confirmed alerts off the engine's critical path and
// delivers them to HTTP subscribers with at-least-once semantics (SPEC 3.13).
package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrBadSignature is returned when a signature does not match the body.
var ErrBadSignature = errors.New("delivery: signature does not match body")

// Sign returns the Jisa-Signature header value for body at unix time ts.
// The signed payload is "<ts>.<body>"; binding the timestamp into the MAC
// stops a captured header being reattached to a different payload.
func Sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.%s", ts, body)
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

// Verify reports whether header is a valid Jisa-Signature for body. It lets a
// subscriber confirm a payload arrived from this engine unaltered; it makes no
// authentication or authorization decision (SPEC 6).
func Verify(secret []byte, header string, body []byte) error {
	var tsField, macField string
	for _, field := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			tsField = v
		case "v1":
			macField = v
		}
	}
	if tsField == "" || macField == "" {
		return fmt.Errorf("delivery: malformed signature header %q", header)
	}
	ts, err := strconv.ParseInt(tsField, 10, 64)
	if err != nil {
		return fmt.Errorf("delivery: malformed timestamp in signature: %w", err)
	}
	got, err := hex.DecodeString(macField)
	if err != nil {
		return fmt.Errorf("delivery: malformed mac in signature: %w", err)
	}
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.%s", ts, body)
	// Constant-time compare: a timing-sensitive comparison here would leak the
	// expected mac one byte at a time.
	if !hmac.Equal(got, mac.Sum(nil)) {
		return ErrBadSignature
	}
	return nil
}
