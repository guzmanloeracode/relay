package push

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

type memStore struct {
	tokens  []db.GetUserPushTokensRow
	deleted []string
	tickets []string
	touched int
}

func (m *memStore) GetUserPushTokens(context.Context, db.GetUserPushTokensParams) ([]db.GetUserPushTokensRow, error) {
	return m.tokens, nil
}
func (m *memStore) DeletePushTokensByValue(_ context.Context, a db.DeletePushTokensByValueParams) (int64, error) {
	m.deleted = append(m.deleted, a.Tokens...)
	return int64(len(a.Tokens)), nil
}
func (m *memStore) InsertPushTickets(_ context.Context, a db.InsertPushTicketsParams) error {
	m.tickets = append(m.tickets, a.TicketIds...)
	return nil
}
func (m *memStore) TouchPushTokens(_ context.Context, ids []uuid.UUID) error {
	m.touched += len(ids)
	return nil
}

func tokens(values ...string) []db.GetUserPushTokensRow {
	out := make([]db.GetUserPushTokensRow, len(values))
	for i, v := range values {
		out[i] = db.GetUserPushTokensRow{ID: uuid.New(), Token: v}
	}
	return out
}

func expoServer(t *testing.T, status int, tickets []Ticket) (*Expo, *[]Message, *string) {
	var got []Message
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": tickets})
	}))
	t.Cleanup(srv.Close)
	return &Expo{SendURL: srv.URL, ReceiptsURL: srv.URL, AccessToken: "tok", Client: srv.Client()}, &got, &auth
}

func ticket(status, id, errCode string) Ticket {
	var tk Ticket
	tk.Status, tk.ID = status, id
	tk.Details.Error = errCode
	return tk
}

func job() *channels.Job {
	return &channels.Job{DeliveryID: uuid.New(), EventID: uuid.New(), TenantID: uuid.New(), EventType: "job.assigned",
		Recipient: "tom", Message: &channels.Message{Title: "New job", Body: "10:00", URL: "app://j/1"}}
}

func TestSendMixedTickets(t *testing.T) {
	expo, got, auth := expoServer(t, 200, []Ticket{ticket("ok", "t1", ""), ticket("error", "", "DeviceNotRegistered")})
	st := &memStore{tokens: tokens("ExponentPushToken[a]", "ExponentPushToken[b]")}
	pruned := 0
	s := &Sender{Store: st, Expo: expo, ReceiptDelay: time.Minute, OnPruned: func(n int) { pruned += n }}
	res := s.Send(context.Background(), job())
	if res.Outcome != channels.Delivered || res.ResponseBody != "1 of 2 devices accepted" {
		t.Fatalf("result %+v", res)
	}
	if len(*got) != 2 || (*got)[0].Title != "New job" || (*got)[0].Data["url"] != "app://j/1" || *auth != "Bearer tok" {
		t.Fatalf("messages %+v auth %q", *got, *auth)
	}
	if len(st.deleted) != 1 || st.deleted[0] != "ExponentPushToken[b]" || pruned != 1 || len(st.tickets) != 1 || st.touched != 1 {
		t.Fatalf("store %+v", st)
	}
}

func TestSendOutcomes(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		tickets []Ticket
		want    channels.Outcome
	}{
		{"all gone", 200, []Ticket{ticket("error", "", "DeviceNotRegistered")}, channels.Skipped},
		{"rate", 200, []Ticket{ticket("error", "", "MessageRateExceeded")}, channels.Retry},
		{"too big", 200, []Ticket{ticket("error", "", "MessageTooBig")}, channels.Dead},
		{"bad request", 400, nil, channels.Dead},
		{"server error", 503, nil, channels.Retry},
		{"wrong ticket count", 200, []Ticket{}, channels.Retry},
	}
	for _, c := range cases {
		expo, _, _ := expoServer(t, c.status, c.tickets)
		s := &Sender{Store: &memStore{tokens: tokens("ExponentPushToken[a]")}, Expo: expo}
		if res := s.Send(context.Background(), job()); res.Outcome != c.want {
			t.Errorf("%s: %+v", c.name, res)
		}
	}
	s := &Sender{Store: &memStore{}, Expo: &Expo{}}
	if res := s.Send(context.Background(), job()); res.Outcome != channels.Skipped || res.Reason != "no_devices" {
		t.Fatalf("no devices: %+v", res)
	}
	j := job()
	j.Message = nil
	if res := s.Send(context.Background(), j); res.Outcome != channels.Skipped || res.Reason != "no_message" {
		t.Fatalf("no message: %+v", res)
	}
}

func TestReceiptsAndTokenPattern(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"t1":{"status":"ok"},"t2":{"status":"error","details":{"error":"DeviceNotRegistered"}}}}`))
	}))
	defer srv.Close()
	e := &Expo{ReceiptsURL: srv.URL, Client: srv.Client()}
	got, err := e.Receipts(context.Background(), []string{"t1", "t2", "t3"})
	if err != nil || len(got) != 2 || got["t2"].Details.Error != "DeviceNotRegistered" {
		t.Fatalf("receipts %+v %v", got, err)
	}
	for tok, ok := range map[string]bool{
		"ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]": true, "ExpoPushToken[abc-DEF_1]": true,
		"ExponentPushToken[]": false, "fcm:abc": false, "ExponentPushToken[a b]": false,
	} {
		if TokenPattern.MatchString(tok) != ok {
			t.Errorf("%q", tok)
		}
	}
}
