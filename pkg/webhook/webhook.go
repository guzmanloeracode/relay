// Package webhook signs and verifies Relay webhooks.
//
// Relay follows the Standard Webhooks specification (https://www.standardwebhooks.com): every request carries
// webhook-id (the event ID, the same on every retry), webhook-timestamp (Unix seconds) and webhook-signature, a
// space-separated list of "v1,<base64 HMAC-SHA256>" over "<id>.<timestamp>.<body>". Secrets look like
// "whsec_<base64>". During a secret rotation a request carries one signature per active secret.
//
// Receivers can use Verify directly:
//
//	err := webhook.Verify(secret, r.Header, body, 5*time.Minute)
package webhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names defined by Standard Webhooks.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
	secretPrefix    = "whsec_"
)

// DefaultTolerance is how far a timestamp may be from the receiver's clock.
const DefaultTolerance = 5 * time.Minute

// Errors returned by Verify.
var (
	ErrMissingHeaders   = errors.New("webhook: missing webhook-id, webhook-timestamp or webhook-signature")
	ErrInvalidTimestamp = errors.New("webhook: invalid timestamp")
	ErrTimestampTooOld  = errors.New("webhook: timestamp too old (possible replay)")
	ErrTimestampFuture  = errors.New("webhook: timestamp too far in the future")
	ErrNoMatch          = errors.New("webhook: no matching signature")
	ErrInvalidSecret    = errors.New("webhook: invalid secret")
)

// NewSecret returns a random signing secret ("whsec_" and 32 random bytes in base64).
func NewSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(b)
}

func secretKey(secret string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
	if err != nil || len(key) == 0 {
		return nil, ErrInvalidSecret
	}
	return key, nil
}

// Sign returns the "v1,<signature>" value for one secret.
func Sign(secret, id string, ts time.Time, body []byte) (string, error) {
	key, err := secretKey(secret)
	if err != nil {
		return "", err
	}
	return "v1," + base64.StdEncoding.EncodeToString(mac(key, id, ts.Unix(), body)), nil
}

func mac(key []byte, id string, unix int64, body []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(id))
	h.Write([]byte{'.'})
	h.Write([]byte(strconv.FormatInt(unix, 10)))
	h.Write([]byte{'.'})
	h.Write(body)
	return h.Sum(nil)
}

// SetHeaders adds the three Standard Webhooks headers, with one signature per secret.
func SetHeaders(h http.Header, secrets []string, id string, ts time.Time, body []byte) error {
	sigs := make([]string, 0, len(secrets))
	for _, s := range secrets {
		sig, err := Sign(s, id, ts, body)
		if err != nil {
			return err
		}
		sigs = append(sigs, sig)
	}
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, strings.Join(sigs, " "))
	return nil
}

// Verify checks a received webhook: the timestamp must be within tolerance of now (blocking replays of old
// requests) and one of the signatures must match the secret. Compare the webhook-id with the IDs you already
// processed to drop duplicates: delivery is at least once.
func Verify(secret string, h http.Header, body []byte, tolerance time.Duration) error {
	return verifyAt(secret, h, body, tolerance, time.Now())
}

func verifyAt(secret string, h http.Header, body []byte, tolerance time.Duration, now time.Time) error {
	id, tsRaw, sigs := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if id == "" || tsRaw == "" || sigs == "" {
		return ErrMissingHeaders
	}
	unix, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}
	if tolerance <= 0 {
		tolerance = DefaultTolerance
	}
	ts := time.Unix(unix, 0)
	if now.Sub(ts) > tolerance {
		return ErrTimestampTooOld
	}
	if ts.Sub(now) > tolerance {
		return ErrTimestampFuture
	}
	key, err := secretKey(secret)
	if err != nil {
		return err
	}
	want := mac(key, id, unix, body)
	for sig := range strings.FieldsSeq(sigs) {
		version, value, ok := strings.Cut(sig, ",")
		if !ok || version != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(value)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return ErrNoMatch
}
