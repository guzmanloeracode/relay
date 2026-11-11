// Package ids turns the UUIDv7 primary keys into the prefixed public IDs the API shows (evt_01JD…).
//
// The 16 bytes are written in Crockford base32, like a ULID. A UUIDv7 starts with its timestamp, so public IDs
// sort by creation time and the newest-first cursors in the API compare them directly.
package ids

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Prefixes of every public ID.
const (
	Tenant       = "ten"
	APIKey       = "key"
	Endpoint     = "ep"
	Subscription = "sub"
	Event        = "evt"
	Delivery     = "dlv"
	PushToken    = "ptk"
)

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

var decodeMap = func() [256]byte {
	var m [256]byte
	for i := range m {
		m[i] = 0xff
	}
	for i := range len(alphabet) {
		m[alphabet[i]] = byte(i)
		m[strings.ToUpper(alphabet)[i]] = byte(i)
	}
	return m
}()

// ErrInvalid is returned for a malformed ID or one with the wrong prefix.
var ErrInvalid = errors.New("invalid id")

// New returns a fresh UUIDv7.
func New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// FromTime returns the smallest UUIDv7 of the given millisecond, to compare IDs with a point in time.
func FromTime(t time.Time) uuid.UUID {
	var id uuid.UUID
	ms := uint64(t.UnixMilli())
	for i := range 6 {
		id[i] = byte(ms >> (40 - 8*i))
	}
	id[6] = 0x70 // version 7
	id[8] = 0x80 // RFC 9562 variant
	return id
}

// Encode writes id as prefix_ followed by 26 base32 characters.
func Encode(prefix string, id uuid.UUID) string {
	var out [26]byte
	// 128 bits in 26 groups of 5 bits: the first character carries the top 3 bits.
	hi := uint64(id[0])<<56 | uint64(id[1])<<48 | uint64(id[2])<<40 | uint64(id[3])<<32 |
		uint64(id[4])<<24 | uint64(id[5])<<16 | uint64(id[6])<<8 | uint64(id[7])
	lo := uint64(id[8])<<56 | uint64(id[9])<<48 | uint64(id[10])<<40 | uint64(id[11])<<32 |
		uint64(id[12])<<24 | uint64(id[13])<<16 | uint64(id[14])<<8 | uint64(id[15])
	for i := 25; i >= 0; i-- {
		out[i] = alphabet[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return prefix + "_" + string(out[:])
}

// Decode parses an ID made by Encode with the same prefix.
func Decode(prefix, s string) (uuid.UUID, error) {
	rest, ok := strings.CutPrefix(s, prefix+"_")
	if !ok || len(rest) != 26 {
		return uuid.Nil, ErrInvalid
	}
	if v := decodeMap[rest[0]]; v == 0xff || v > 7 {
		return uuid.Nil, ErrInvalid
	}
	var hi, lo uint64
	for i := range 26 {
		v := decodeMap[rest[i]]
		if v == 0xff {
			return uuid.Nil, ErrInvalid
		}
		hi = hi<<5 | lo>>59
		lo = lo<<5 | uint64(v)
	}
	var id uuid.UUID
	for i := range 8 {
		id[i] = byte(hi >> (56 - 8*i))
		id[8+i] = byte(lo >> (56 - 8*i))
	}
	return id, nil
}

// MustDecode is Decode for IDs known to be valid (tests and constants).
func MustDecode(prefix, s string) uuid.UUID {
	id, err := Decode(prefix, s)
	if err != nil {
		panic(err)
	}
	return id
}
