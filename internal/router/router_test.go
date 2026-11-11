package router

import (
	"testing"

	"github.com/google/uuid"
)

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, typ string
		want         bool
	}{
		{"*", "job.completed", true},
		{"job.completed", "job.completed", true},
		{"job.completed", "job.created", false},
		{"job.*", "job.completed", true},
		{"job.*", "job.part.requested", true},
		{"job.*", "job", false},
		{"job.*", "jobs.completed", false},
		{"invoice.paid", "invoice.paid.late", false},
	}
	for _, c := range cases {
		if got := Match(c.pattern, c.typ); got != c.want {
			t.Errorf("Match(%q, %q) = %v", c.pattern, c.typ, got)
		}
	}
}

func TestPlan(t *testing.T) {
	hook, hook2, mail, push := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	routes := Merge([]Route{
		{EndpointID: hook, Channel: "webhook", Patterns: []string{"job.*"}},
		{EndpointID: hook, Channel: "webhook", Patterns: []string{"invoice.paid"}}, // second subscription, same endpoint
		{EndpointID: hook2, Channel: "webhook", Patterns: []string{"invoice.*"}},
		{EndpointID: mail, Channel: "email", EmailTo: []string{"ops@example.com", "OPS@example.com"}, Patterns: []string{"*"}},
		{EndpointID: push, Channel: "push", Patterns: []string{"job.completed"}},
	})
	if len(routes) != 4 {
		t.Fatalf("Merge: %d routes", len(routes))
	}

	t.Run("fan out to every channel", func(t *testing.T) {
		plan, skips := Plan("job.completed", true, Targets{Users: []string{"u1", "u2", "u1"}}, nil, routes)
		want := map[string]int{"webhook": 1, "email": 1, "push": 2}
		got := map[string]int{}
		for _, p := range plan {
			got[p.Channel]++
		}
		for ch, n := range want {
			if got[ch] != n {
				t.Errorf("%s: %d deliveries, want %d (%+v)", ch, got[ch], n, plan)
			}
		}
		if len(skips) != 0 {
			t.Errorf("skips: %+v", skips)
		}
	})

	t.Run("invoice.paid reaches both webhooks once each", func(t *testing.T) {
		plan, _ := Plan("invoice.paid", false, Targets{}, nil, routes)
		n := 0
		for _, p := range plan {
			if p.Channel == "webhook" {
				n++
			}
		}
		if n != 2 {
			t.Fatalf("want 2 webhooks, got %+v", plan)
		}
	})

	t.Run("no message: email and push are skipped", func(t *testing.T) {
		plan, skips := Plan("job.completed", false, Targets{Users: []string{"u1"}}, nil, routes)
		if len(plan) != 1 || plan[0].EndpointID != hook {
			t.Fatalf("plan: %+v", plan)
		}
		if len(skips) != 2 {
			t.Fatalf("skips: %+v", skips)
		}
	})

	t.Run("explicit emails replace the default list", func(t *testing.T) {
		plan, _ := Plan("job.created", true, Targets{Emails: []string{"A@x.com", "a@x.com", "b@x.com"}}, nil, routes)
		var to []string
		for _, p := range plan {
			if p.Channel == "email" {
				to = append(to, p.Recipient)
			}
		}
		if len(to) != 2 || to[0] != "a@x.com" || to[1] != "b@x.com" {
			t.Fatalf("emails: %v", to)
		}
	})

	t.Run("targets.endpoints narrows the fan-out", func(t *testing.T) {
		plan, skips := Plan("invoice.paid", true, Targets{}, map[uuid.UUID]bool{hook2: true}, routes)
		if len(plan) != 1 || plan[0].EndpointID != hook2 {
			t.Fatalf("plan: %+v", plan)
		}
		if len(skips) != 2 { // the other webhook and the email route
			t.Fatalf("skips: %+v", skips)
		}
	})

	t.Run("push without users", func(t *testing.T) {
		_, skips := Plan("job.completed", true, Targets{}, nil, routes)
		found := false
		for _, s := range skips {
			found = found || (s.Channel == "push" && s.Reason == "no_users")
		}
		if !found {
			t.Fatalf("skips: %+v", skips)
		}
	})

	t.Run("caps", func(t *testing.T) {
		many := make([]string, 300)
		for i := range many {
			many[i] = uuid.NewString()
		}
		plan, _ := Plan("job.completed", true, Targets{Users: many}, map[uuid.UUID]bool{push: true}, routes)
		if len(plan) != MaxUsers {
			t.Fatalf("want %d, got %d", MaxUsers, len(plan))
		}
	})
}
