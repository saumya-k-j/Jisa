package delivery

import (
	"fmt"
	"testing"
)

func TestCheckRecordsAnUnseenKey(t *testing.T) {
	d := NewDedupe(16)
	id, dup := d.Check("key-a", "delivery-1")
	if dup {
		t.Fatal("first sight of a key reported as duplicate")
	}
	if id != "delivery-1" {
		t.Fatalf("id = %q, want %q", id, "delivery-1")
	}
}

func TestCheckReturnsTheOriginalIDForADuplicate(t *testing.T) {
	d := NewDedupe(16)
	d.Check("key-a", "delivery-1")

	id, dup := d.Check("key-a", "delivery-2")
	if !dup {
		t.Fatal("repeat key not reported as duplicate")
	}
	if id != "delivery-1" {
		t.Fatalf("id = %q, want the original %q", id, "delivery-1")
	}
}

func TestDedupeIsBounded(t *testing.T) {
	const max = 32
	d := NewDedupe(max)
	for i := 0; i < max*4; i++ {
		d.Check(fmt.Sprintf("key-%d", i), fmt.Sprintf("delivery-%d", i))
	}
	if n := d.Len(); n > max {
		t.Fatalf("Len() = %d, exceeds bound %d", n, max)
	}
}
