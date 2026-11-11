package api

import (
	"encoding/json"
	"time"

	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// TenantLimits are a tenant's rate and concurrency limits.
type TenantLimits struct {
	IngestRate   int32 `json:"ingest_rate"`
	IngestBurst  int32 `json:"ingest_burst"`
	DeliveryRate int32 `json:"delivery_rate"`
	MaxInFlight  int32 `json:"max_in_flight"`
}

// TenantView is a tenant in responses.
type TenantView struct {
	ID         string       `json:"id"`
	Slug       string       `json:"slug"`
	Name       string       `json:"name"`
	Limits     TenantLimits `json:"limits"`
	Disabled   bool         `json:"disabled"`
	DisabledAt *time.Time   `json:"disabled_at"`
	CreatedAt  time.Time    `json:"created_at"`
	UpdatedAt  time.Time    `json:"updated_at"`
}

func tenantView(t db.Tenant) TenantView {
	return TenantView{
		ID: ids.Encode(ids.Tenant, t.ID), Slug: t.Slug, Name: t.Name,
		Limits:   TenantLimits{IngestRate: t.IngestRate, IngestBurst: t.IngestBurst, DeliveryRate: t.DeliveryRate, MaxInFlight: t.MaxInFlight},
		Disabled: t.DisabledAt != nil, DisabledAt: t.DisabledAt, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
	}
}

// KeyView is an API key in responses. Key is only set when the key is created.
type KeyView struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Tenant     *string    `json:"tenant"`
	Prefix     string     `json:"prefix"`
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	RevokedAt  *time.Time `json:"revoked_at"`
	Key        string     `json:"key,omitempty"`
}

func keyView(k db.ApiKey, tenantSlug *string) KeyView {
	return KeyView{
		ID: ids.Encode(ids.APIKey, k.ID), Name: k.Name, Tenant: tenantSlug, Prefix: k.Prefix, Scopes: k.Scopes,
		CreatedAt: k.CreatedAt, LastUsedAt: k.LastUsedAt, RevokedAt: k.RevokedAt,
	}
}

// EndpointView is an endpoint in responses. Secret is only set on creation and rotation.
type EndpointView struct {
	ID             string     `json:"id"`
	Channel        string     `json:"channel"`
	Description    string     `json:"description"`
	URL            *string    `json:"url"`
	EmailTo        []string   `json:"email_to"`
	MaxInFlight    int32      `json:"max_in_flight"`
	Status         string     `json:"status"`
	DisabledReason *string    `json:"disabled_reason"`
	DisabledAt     *time.Time `json:"disabled_at"`
	FailureStreak  int32      `json:"failure_streak"`
	FailingSince   *time.Time `json:"failing_since"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	Secret         string     `json:"secret,omitempty"`
}

func endpointView(e db.Endpoint) EndpointView {
	return EndpointView{
		ID: ids.Encode(ids.Endpoint, e.ID), Channel: e.Channel, Description: e.Description, URL: e.Url,
		EmailTo: e.EmailTo, MaxInFlight: e.MaxInFlight, Status: e.Status, DisabledReason: e.DisabledReason,
		DisabledAt: e.DisabledAt, FailureStreak: e.FailureStreak, FailingSince: e.FailingSince,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
	}
}

// SubscriptionView is a subscription in responses.
type SubscriptionView struct {
	ID         string    `json:"id"`
	EndpointID string    `json:"endpoint_id"`
	EventTypes []string  `json:"event_types"`
	CreatedAt  time.Time `json:"created_at"`
}

func subscriptionView(s db.Subscription) SubscriptionView {
	return SubscriptionView{
		ID: ids.Encode(ids.Subscription, s.ID), EndpointID: ids.Encode(ids.Endpoint, s.EndpointID),
		EventTypes: s.EventTypes, CreatedAt: s.CreatedAt,
	}
}

// EventView is an event in responses.
type EventView struct {
	ID             string          `json:"id"`
	Tenant         string          `json:"tenant"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Message        json.RawMessage `json:"message"`
	Targets        json.RawMessage `json:"targets"`
	IdempotencyKey *string         `json:"idempotency_key"`
	CreatedAt      time.Time       `json:"created_at"`
	DeliveryCount  int32           `json:"delivery_count"`
	Deliveries     []DeliveryView  `json:"deliveries,omitempty"`
}

func rawOrNull(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("null")
	}
	return b
}

func eventView(e db.Event, tenant string) EventView {
	return EventView{
		ID: ids.Encode(ids.Event, e.ID), Tenant: tenant, Type: e.Type, Payload: e.Payload,
		Message: rawOrNull(e.Message), Targets: rawOrNull(e.Targets), IdempotencyKey: e.IdempotencyKey,
		CreatedAt: e.CreatedAt, DeliveryCount: e.DeliveryCount,
	}
}

// DeliveryView is a delivery in responses; AttemptLog is only set on GET of one delivery.
type DeliveryView struct {
	ID             string        `json:"id"`
	EventID        string        `json:"event_id"`
	EndpointID     string        `json:"endpoint_id"`
	Channel        string        `json:"channel"`
	Recipient      *string       `json:"recipient"`
	Status         string        `json:"status"`
	Attempts       int32         `json:"attempts"`
	MaxAttempts    int32         `json:"max_attempts"`
	NextAttemptAt  *time.Time    `json:"next_attempt_at"`
	LastStatusCode *int32        `json:"last_status_code"`
	LastError      *string       `json:"last_error"`
	LastLatencyMs  *int32        `json:"last_latency_ms"`
	DeadReason     *string       `json:"dead_reason"`
	ReplayOf       *string       `json:"replay_of"`
	ReplayedAt     *time.Time    `json:"replayed_at"`
	CreatedAt      time.Time     `json:"created_at"`
	CompletedAt    *time.Time    `json:"completed_at"`
	AttemptLog     []AttemptView `json:"attempt_log,omitempty"`
}

func deliveryView(d db.Delivery) DeliveryView {
	v := DeliveryView{
		ID: ids.Encode(ids.Delivery, d.ID), EventID: ids.Encode(ids.Event, d.EventID),
		EndpointID: ids.Encode(ids.Endpoint, d.EndpointID), Channel: d.Channel, Status: d.Status,
		Attempts: d.Attempts, MaxAttempts: d.MaxAttempts, LastStatusCode: d.LastStatusCode, LastError: d.LastError,
		LastLatencyMs: d.LastLatencyMs, DeadReason: d.DeadReason, ReplayedAt: d.ReplayedAt, CreatedAt: d.CreatedAt,
		CompletedAt: d.CompletedAt,
	}
	if d.Recipient != "" {
		v.Recipient = &d.Recipient
	}
	if d.Status == "pending" {
		next := d.NextAttemptAt
		v.NextAttemptAt = &next
	}
	if d.ReplayOf != nil {
		s := ids.Encode(ids.Delivery, *d.ReplayOf)
		v.ReplayOf = &s
	}
	return v
}

// AttemptView is one attempt in a delivery's log.
type AttemptView struct {
	Attempt      int32     `json:"attempt"`
	StartedAt    time.Time `json:"started_at"`
	LatencyMs    int32     `json:"latency_ms"`
	StatusCode   *int32    `json:"status_code"`
	Error        *string   `json:"error"`
	ResponseBody *string   `json:"response_body"`
	Outcome      string    `json:"outcome"`
}

func attemptView(a db.DeliveryAttempt) AttemptView {
	return AttemptView{Attempt: a.Attempt, StartedAt: a.StartedAt, LatencyMs: a.LatencyMs, StatusCode: a.StatusCode,
		Error: a.Error, ResponseBody: a.ResponseBody, Outcome: a.Outcome}
}

// PushTokenView is a registered device in responses.
type PushTokenView struct {
	ID         string     `json:"id"`
	User       string     `json:"user"`
	Token      string     `json:"token"`
	Platform   string     `json:"platform"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

func pushTokenView(p db.PushToken) PushTokenView {
	return PushTokenView{ID: ids.Encode(ids.PushToken, p.ID), User: p.UserID, Token: p.Token, Platform: p.Platform,
		CreatedAt: p.CreatedAt, LastUsedAt: p.LastUsedAt}
}
