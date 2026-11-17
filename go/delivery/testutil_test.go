package delivery

import (
	"fmt"
	"strconv"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// docState reads one alert document by idempotency key and returns its state
// and the raw document.
func docState(t *testing.T, s *Store, key string) (string, bson.M) {
	t.Helper()
	var doc bson.M
	res := s.alerts.FindOne(ctxT(t), bson.D{{Key: "idempotency_key", Value: key}})
	if err := res.Decode(&doc); err != nil {
		return "", nil
	}
	st, _ := doc["state"].(string)
	return st, doc
}

// key36 builds a short distinct idempotency key for bulk tests.
func key36(i int) string {
	return fmt.Sprintf("key-bulk-%s", strconv.FormatInt(int64(i), 36))
}
