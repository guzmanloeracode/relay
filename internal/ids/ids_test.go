package ids

import (
	"bytes"
	"sort"
	"testing"

	"github.com/google/uuid"
)

func TestRoundTrip(t *testing.T) {
	cases := []uuid.UUID{uuid.Nil, uuid.Max, New(), New(), uuid.MustParse("01934b5e-7f00-7abc-8def-0123456789ab")}
	for _, id := range cases {
		s := Encode(Event, id)
		if len(s) != len("evt_")+26 {
			t.Fatalf("length of %q", s)
		}
		got, err := Decode(Event, s)
		if err != nil || got != id {
			t.Fatalf("Decode(Encode(%s)) = %s, %v", id, got, err)
		}
	}
}

func TestSortsLikeTheUUID(t *testing.T) {
	var raw []uuid.UUID
	var enc []string
	for range 200 {
		id := uuid.New() // random bytes exercise every bit position
		raw = append(raw, id)
		enc = append(enc, Encode(Delivery, id))
	}
	sort.Slice(raw, func(i, j int) bool { return bytes.Compare(raw[i][:], raw[j][:]) < 0 })
	sort.Strings(enc)
	for i := range raw {
		if Encode(Delivery, raw[i]) != enc[i] {
			t.Fatalf("order differs at %d", i)
		}
	}
}

func TestDecodeRejects(t *testing.T) {
	good := Encode(Endpoint, New())
	bad := []string{
		"", "ep_", good[:len(good)-1], good + "0", "evt" + good[2:], // wrong length or prefix
		"ep_" + "u" + good[4:], // u is not in the alphabet
		"ep_" + "8" + good[4:], // first character above 7 overflows 128 bits
	}
	for _, s := range bad {
		if _, err := Decode(Endpoint, s); err == nil {
			t.Errorf("Decode(%q) accepted", s)
		}
	}
	if _, err := Decode(Endpoint, string(bytes.ToUpper([]byte(good[:3])))+good[3:]); err == nil {
		t.Error("prefix must be lower case")
	}
	upper := "ep_" + string(bytes.ToUpper([]byte(good[3:])))
	if _, err := Decode(Endpoint, upper); err != nil {
		t.Errorf("upper-case body should decode: %v", err)
	}
}
