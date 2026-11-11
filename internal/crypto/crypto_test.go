package crypto

import (
	"bytes"
	"testing"
)

func TestBox(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	b, err := NewBox(key)
	if err != nil {
		t.Fatal(err)
	}
	a1, a2 := b.Seal([]byte("whsec_abc")), b.Seal([]byte("whsec_abc"))
	if bytes.Equal(a1, a2) {
		t.Fatal("nonce reused")
	}
	got, err := b.Open(a1)
	if err != nil || string(got) != "whsec_abc" {
		t.Fatalf("Open = %q, %v", got, err)
	}
	a1[len(a1)-1] ^= 1
	if _, err := b.Open(a1); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
	other, _ := NewBox(bytes.Repeat([]byte{8}, 32))
	if _, err := other.Open(a2); err == nil {
		t.Fatal("opened with the wrong key")
	}
	if _, err := b.Open([]byte{1, 2}); err == nil {
		t.Fatal("short input accepted")
	}
	if _, err := NewBox([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestHashKey(t *testing.T) {
	if len(HashKey("rk_x")) != 32 || bytes.Equal(HashKey("a"), HashKey("b")) {
		t.Fatal("HashKey")
	}
}
