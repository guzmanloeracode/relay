package sink

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/guzmanloeracode/relay/pkg/webhook"
)

func TestSink(t *testing.T) {
	s := New()
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	secret := webhook.NewSecret()
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/config", strings.NewReader(`{"secret":"`+secret+`"}`))
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != http.StatusNoContent {
		t.Fatal("config", err)
	}
	post := func(id, sec string) int {
		body := `{"id":"` + id + `","created_at":"` + time.Now().Add(-50*time.Millisecond).Format(time.RFC3339Nano) + `"}`
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/hook", strings.NewReader(body))
		_ = webhook.SetHeaders(req.Header, []string{sec}, id, time.Now(), []byte(body))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	for i, c := range []struct {
		id, secret string
		want       int
	}{{"evt_1", secret, http.StatusNoContent}, {"evt_1", secret, http.StatusNoContent}, {"evt_2", secret, http.StatusNoContent}, {"evt_3", webhook.NewSecret(), http.StatusUnauthorized}} {
		if got := post(c.id, c.secret); got != c.want {
			t.Fatalf("post %d: %d", i, got)
		}
	}
	st := s.Stats()
	if st.Received != 4 || st.Unique != 3 || st.Duplicates != 1 || st.BadSignatures != 1 || st.LagP50Ms < 50 || st.LagMaxMs > 5000 {
		t.Fatalf("stats %+v", st)
	}
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/reset", nil)
	_, _ = http.DefaultClient.Do(req)
	if s.Stats().Received != 0 {
		t.Fatal("reset")
	}
	s.failRate = 1
	if post("evt_4", secret) != http.StatusServiceUnavailable {
		t.Fatal("fail rate")
	}
}
