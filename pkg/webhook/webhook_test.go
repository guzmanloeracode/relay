package webhook

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The test vector published with the Standard Webhooks specification.
const (
	specSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw" // gitleaks:allow (the specification's public test vector)
	specID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	specTS     = 1614265330
	specBody   = `{"test": 2432232314}`
	specSig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func TestSignMatchesSpecVector(t *testing.T) {
	got, err := Sign(specSecret, specID, time.Unix(specTS, 0), []byte(specBody))
	if err != nil || got != specSig {
		t.Fatalf("Sign = %q, %v; want %q", got, err, specSig)
	}
}

func headers(sig string, ts int64) http.Header {
	h := http.Header{}
	h.Set(HeaderID, specID)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderSignature, sig)
	return h
}

func TestVerify(t *testing.T) {
	now := time.Unix(specTS, 0)
	body := []byte(specBody)

	if err := verifyAt(specSecret, headers(specSig, specTS), body, 0, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	// Several signatures (a secret rotation): any match passes, unknown versions are skipped.
	if err := verifyAt(specSecret, headers("v2,abc v1,bm90LWl0 "+specSig, specTS), body, 0, now); err != nil {
		t.Fatalf("rotation list rejected: %v", err)
	}

	cases := map[string]struct {
		h    http.Header
		body []byte
		now  time.Time
		want error
	}{
		"tampered body":  {headers(specSig, specTS), []byte(`{"test": 1}`), now, ErrNoMatch},
		"wrong secret":   {headers(specSig, specTS), body, now, ErrNoMatch},
		"replayed later": {headers(specSig, specTS), body, now.Add(6 * time.Minute), ErrTimestampTooOld},
		"from future":    {headers(specSig, specTS), body, now.Add(-6 * time.Minute), ErrTimestampFuture},
		"no headers":     {http.Header{}, body, now, ErrMissingHeaders},
	}
	for name, c := range cases {
		secret := specSecret
		if name == "wrong secret" {
			secret = NewSecret()
		}
		if err := verifyAt(secret, c.h, c.body, DefaultTolerance, c.now); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", name, err, c.want)
		}
	}

	bad := headers(specSig, specTS)
	bad.Set(HeaderTimestamp, "yesterday")
	if err := verifyAt(specSecret, bad, body, 0, now); !errors.Is(err, ErrInvalidTimestamp) {
		t.Errorf("bad timestamp: %v", err)
	}
}

func TestSetHeadersSignsWithEverySecret(t *testing.T) {
	a, b := NewSecret(), NewSecret()
	if !strings.HasPrefix(a, "whsec_") || a == b {
		t.Fatalf("NewSecret: %q %q", a, b)
	}
	h := http.Header{}
	now := time.Now()
	if err := SetHeaders(h, []string{a, b}, "evt_1", now, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Fields(h.Get(HeaderSignature))); n != 2 {
		t.Fatalf("want 2 signatures, got %d", n)
	}
	for _, s := range []string{a, b} {
		if err := Verify(s, h, []byte("{}"), 0); err != nil {
			t.Fatalf("secret not accepted: %v", err)
		}
	}
	if err := SetHeaders(h, []string{"whsec_***"}, "evt_1", now, nil); !errors.Is(err, ErrInvalidSecret) {
		t.Fatalf("invalid secret: %v", err)
	}
}
