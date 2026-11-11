// Package channels defines what a delivery channel (webhook, email, push) receives and returns.
package channels

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Channel names.
const (
	Webhook = "webhook"
	Email   = "email"
	Push    = "push"
)

// Message is the human-readable part of an event, used by the email and push channels.
type Message struct {
	Title string `json:"title"`
	Body  string `json:"body,omitempty"`
	URL   string `json:"url,omitempty"`
	HTML  string `json:"html,omitempty"`
}

// Job is one claimed delivery attempt with everything a channel needs.
type Job struct {
	DeliveryID     uuid.UUID
	EventID        uuid.UUID
	TenantID       uuid.UUID
	EndpointID     uuid.UUID
	TenantSlug     string
	EventType      string
	EventCreatedAt time.Time
	Payload        json.RawMessage
	Message        *Message
	Attempt        int
	Recipient      string   // email address or user ID
	URL            string   // webhook URL
	Secrets        []string // webhook signing secrets (current first)
}

// Outcome of one attempt.
type Outcome int

// Attempt outcomes.
const (
	Delivered Outcome = iota // done
	Retry                    // failed, try again later (if attempts remain)
	Dead                     // failed for good: straight to the dead-letter queue
	Skipped                  // nothing to send (a push user without devices)
)

func (o Outcome) String() string {
	return [...]string{"delivered", "retry", "dead", "skipped"}[o]
}

// Result describes one attempt.
type Result struct {
	Outcome      Outcome
	StatusCode   int           // HTTP status (or SMTP code) from the receiver, 0 if none
	Error        string        // why it failed or was skipped
	ResponseBody string        // the start of the receiver's answer
	RetryAfter   time.Duration // the receiver's Retry-After, if any
	Reason       string        // dead-letter or skip reason
	// EndpointFailure counts the attempt against the endpoint's failure streak (automatic disabling).
	EndpointFailure bool
	// DisableEndpoint disables the endpoint right away with this reason (a webhook answered 410 Gone).
	DisableEndpoint string
}

// Sender delivers jobs of one channel.
type Sender interface {
	Send(ctx context.Context, job *Job) Result
}
