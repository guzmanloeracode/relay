// Package router decides which deliveries an event becomes, from the tenant's subscriptions and the event's
// targets. It is pure: the API loads the routes and stores the plan with the event.
package router

import (
	"strings"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/channels"
	"github.com/guzmanloeracode/relay/internal/ids"
	"github.com/guzmanloeracode/relay/internal/store/db"
)

// Limits on the fan-out of one event.
const (
	MaxUsers     = 100
	MaxEmails    = 50
	MaxEndpoints = 20
)

// Route is one active endpoint with every event-type pattern subscribed to it.
type Route struct {
	EndpointID uuid.UUID
	Channel    string
	EmailTo    []string
	Patterns   []string
}

// Targets narrow or address an event's deliveries.
type Targets struct {
	Users     []string `json:"users,omitempty"`     // push: the users whose devices get it
	Emails    []string `json:"emails,omitempty"`    // email: recipients (instead of the endpoint's default list)
	Endpoints []string `json:"endpoints,omitempty"` // only these endpoints (public IDs), among the subscribed ones
}

// FromRows turns the tenant's subscriptions (as ListRoutes returns them) into one route per endpoint.
func FromRows(rows []db.ListRoutesRow) []Route {
	routes := make([]Route, len(rows))
	for i, r := range rows {
		routes[i] = Route{EndpointID: r.EndpointID, Channel: r.Channel, EmailTo: r.EmailTo, Patterns: r.EventTypes}
	}
	return Merge(routes)
}

// EndpointSet decodes targets.endpoints; nil when the event names none.
func (t Targets) EndpointSet() map[uuid.UUID]bool {
	if len(t.Endpoints) == 0 {
		return nil
	}
	set := make(map[uuid.UUID]bool, len(t.Endpoints))
	for _, s := range t.Endpoints {
		if id, err := ids.Decode(ids.Endpoint, s); err == nil {
			set[id] = true
		}
	}
	return set
}

// Planned is one delivery to create.
type Planned struct {
	EndpointID uuid.UUID
	Channel    string
	Recipient  string
}

// Skip is a subscribed endpoint that gets nothing, and why.
type Skip struct {
	Channel, Reason string
}

// Match reports whether an event type matches a subscription pattern: "*", an exact type, or a prefix
// ending in ".*" ("job.*" matches "job.completed" and "job.part.requested", not "jobs.x").
func Match(pattern, eventType string) bool {
	if pattern == "*" || pattern == eventType {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, ".*"); ok {
		return strings.HasPrefix(eventType, prefix+".")
	}
	return false
}

// Merge folds several subscriptions to the same endpoint into one route each.
func Merge(rows []Route) []Route {
	byEndpoint := map[uuid.UUID]int{}
	var out []Route
	for _, r := range rows {
		if i, ok := byEndpoint[r.EndpointID]; ok {
			out[i].Patterns = append(out[i].Patterns, r.Patterns...)
			continue
		}
		byEndpoint[r.EndpointID] = len(out)
		out = append(out, Route{EndpointID: r.EndpointID, Channel: r.Channel, EmailTo: r.EmailTo, Patterns: append([]string(nil), r.Patterns...)})
	}
	return out
}

// Plan returns the deliveries for one event. targetEndpoints holds the decoded IDs of targets.endpoints.
func Plan(eventType string, hasMessage bool, t Targets, targetEndpoints map[uuid.UUID]bool, routes []Route) ([]Planned, []Skip) {
	var plan []Planned
	var skips []Skip
	for _, r := range routes {
		if !matchesAny(r.Patterns, eventType) {
			continue
		}
		if len(targetEndpoints) > 0 && !targetEndpoints[r.EndpointID] {
			skips = append(skips, Skip{r.Channel, "not_targeted"})
			continue
		}
		switch r.Channel {
		case channels.Webhook:
			plan = append(plan, Planned{EndpointID: r.EndpointID, Channel: r.Channel})
		case channels.Email:
			if !hasMessage {
				skips = append(skips, Skip{r.Channel, "no_message"})
				continue
			}
			to := t.Emails
			if len(to) == 0 {
				to = r.EmailTo
			}
			to = dedupe(to, MaxEmails, strings.ToLower)
			if len(to) == 0 {
				skips = append(skips, Skip{r.Channel, "no_recipients"})
			}
			for _, addr := range to {
				plan = append(plan, Planned{EndpointID: r.EndpointID, Channel: r.Channel, Recipient: addr})
			}
		case channels.Push:
			if !hasMessage {
				skips = append(skips, Skip{r.Channel, "no_message"})
				continue
			}
			users := dedupe(t.Users, MaxUsers, nil)
			if len(users) == 0 {
				skips = append(skips, Skip{r.Channel, "no_users"})
			}
			for _, u := range users {
				plan = append(plan, Planned{EndpointID: r.EndpointID, Channel: r.Channel, Recipient: u})
			}
		}
	}
	return plan, skips
}

func matchesAny(patterns []string, eventType string) bool {
	for _, p := range patterns {
		if Match(p, eventType) {
			return true
		}
	}
	return false
}

func dedupe(in []string, max int, norm func(string) string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if norm != nil {
			v = norm(v)
		}
		if v == "" || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
		if len(out) == max {
			break
		}
	}
	return out
}
