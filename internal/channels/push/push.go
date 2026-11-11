// Package push delivers event messages to a user's devices through the Expo Push API, checks the receipts
// later, and deletes tokens Expo reports as no longer registered.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// TokenPattern matches an Expo push token.
var TokenPattern = regexp.MustCompile(`^Expo(nent)?PushToken\[[A-Za-z0-9_\-:.]{1,200}\]$`)

// Expo is a minimal client for the Expo Push API.
type Expo struct {
	SendURL, ReceiptsURL, AccessToken string
	Client                            *http.Client
}

// Message is one push notification to one token.
type Message struct {
	To       string         `json:"to"`
	Title    string         `json:"title,omitempty"`
	Body     string         `json:"body,omitempty"`
	Data     map[string]any `json:"data,omitempty"`
	Sound    string         `json:"sound,omitempty"`
	Priority string         `json:"priority,omitempty"`
}

// Ticket is Expo's answer for one message.
type Ticket struct {
	Status  string `json:"status"` // ok or error
	ID      string `json:"id"`
	Message string `json:"message"`
	Details struct {
		Error string `json:"error"`
	} `json:"details"`
}

// Receipt is the final status of a ticket.
type Receipt = Ticket

// HTTPError is a non-2xx answer from Expo.
type HTTPError struct {
	Code int
	Body string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("expo answered %d: %s", e.Code, e.Body) }

func (e *Expo) post(ctx context.Context, url string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if e.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+e.AccessToken)
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{Code: resp.StatusCode, Body: strings.TrimSpace(string(raw[:min(len(raw), 300)]))}
	}
	return json.Unmarshal(raw, out)
}

// Send pushes up to 100 messages and returns one ticket per message, in order.
func (e *Expo) Send(ctx context.Context, msgs []Message) ([]Ticket, error) {
	var out struct {
		Data []Ticket `json:"data"`
	}
	if err := e.post(ctx, e.SendURL, msgs, &out); err != nil {
		return nil, err
	}
	if len(out.Data) != len(msgs) {
		return nil, fmt.Errorf("expo returned %d tickets for %d messages", len(out.Data), len(msgs))
	}
	return out.Data, nil
}

// Receipts fetches the receipts of up to 1000 tickets. Receipts that aren't ready are missing from the map.
func (e *Expo) Receipts(ctx context.Context, ticketIDs []string) (map[string]Receipt, error) {
	var out struct {
		Data map[string]Receipt `json:"data"`
	}
	if err := e.post(ctx, e.ReceiptsURL, map[string]any{"ids": ticketIDs}, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// Store is what the push channel reads and writes.
type Store interface {
	GetUserPushTokens(ctx context.Context, arg db.GetUserPushTokensParams) ([]db.GetUserPushTokensRow, error)
	DeletePushTokensByValue(ctx context.Context, arg db.DeletePushTokensByValueParams) (int64, error)
	InsertPushTickets(ctx context.Context, arg db.InsertPushTicketsParams) error
	TouchPushTokens(ctx context.Context, ids []uuid.UUID) error
}

// Sender delivers jobs to every device of the job's user.
type Sender struct {
	Store        Store
	Expo         *Expo
	ReceiptDelay time.Duration
	OnPruned     func(n int) // metrics hook
}

const maxDevices = 100

// Send pushes the job's message to the user's devices.
func (s *Sender) Send(ctx context.Context, j *channels.Job) channels.Result {
	if j.Message == nil {
		return channels.Result{Outcome: channels.Skipped, Reason: "no_message", Error: "the event has no message"}
	}
	tokens, err := s.Store.GetUserPushTokens(ctx, db.GetUserPushTokensParams{TenantID: j.TenantID, UserID: j.Recipient})
	if err != nil {
		return channels.Result{Outcome: channels.Retry, Error: "loading push tokens: " + err.Error()}
	}
	if len(tokens) == 0 {
		return channels.Result{Outcome: channels.Skipped, Reason: "no_devices", Error: "the user has no registered devices"}
	}
	if len(tokens) > maxDevices {
		tokens = tokens[len(tokens)-maxDevices:] // the most recently registered
	}
	data := map[string]any{"event_id": ids.Encode(ids.Event, j.EventID), "type": j.EventType}
	if j.Message.URL != "" {
		data["url"] = j.Message.URL
	}
	msgs := make([]Message, len(tokens))
	for i, t := range tokens {
		msgs[i] = Message{To: t.Token, Title: j.Message.Title, Body: j.Message.Body, Data: data, Sound: "default", Priority: "high"}
	}
	tickets, err := s.Expo.Send(ctx, msgs)
	if err != nil {
		var he *HTTPError
		if errors.As(err, &he) {
			if he.Code == http.StatusBadRequest {
				return channels.Result{Outcome: channels.Dead, StatusCode: he.Code, Error: he.Error(), Reason: "rejected"}
			}
			return channels.Result{Outcome: channels.Retry, StatusCode: he.Code, Error: he.Error()}
		}
		return channels.Result{Outcome: channels.Retry, Error: err.Error()}
	}

	var okIDs, okTokens, gone []string
	var used []uuid.UUID
	var retryable, permanent []string
	for i, t := range tickets {
		switch {
		case t.Status == "ok":
			okIDs = append(okIDs, t.ID)
			okTokens = append(okTokens, tokens[i].Token)
			used = append(used, tokens[i].ID)
		case t.Details.Error == "DeviceNotRegistered":
			gone = append(gone, tokens[i].Token)
		case t.Details.Error == "MessageRateExceeded":
			retryable = append(retryable, t.Details.Error)
		default:
			permanent = append(permanent, firstNonEmpty(t.Details.Error, t.Message))
		}
	}
	if len(gone) > 0 {
		if n, err := s.Store.DeletePushTokensByValue(ctx, db.DeletePushTokensByValueParams{TenantID: j.TenantID, Tokens: gone}); err == nil && s.OnPruned != nil {
			s.OnPruned(int(n))
		}
	}
	if len(okIDs) > 0 {
		_ = s.Store.InsertPushTickets(ctx, db.InsertPushTicketsParams{
			TicketIds: okIDs, Tokens: okTokens, TenantID: j.TenantID, DeliveryID: j.DeliveryID,
			CheckAfter: time.Now().Add(s.ReceiptDelay),
		})
		_ = s.Store.TouchPushTokens(ctx, used)
		res := channels.Result{Outcome: channels.Delivered, StatusCode: http.StatusOK}
		if n := len(tickets) - len(okIDs); n > 0 {
			res.ResponseBody = fmt.Sprintf("%d of %d devices accepted", len(okIDs), len(tickets))
		}
		return res
	}
	switch {
	case len(retryable) > 0:
		return channels.Result{Outcome: channels.Retry, StatusCode: http.StatusOK, Error: "expo: " + retryable[0]}
	case len(permanent) > 0:
		return channels.Result{Outcome: channels.Dead, StatusCode: http.StatusOK, Error: "expo: " + permanent[0], Reason: "rejected"}
	default:
		return channels.Result{Outcome: channels.Skipped, StatusCode: http.StatusOK, Reason: "no_devices",
			Error: "every device of the user is no longer registered"}
	}
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return "unknown error"
}
