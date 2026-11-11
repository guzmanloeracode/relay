package integration

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

// The contract checks in env.call only mean something if the validator rejects responses that break the spec.
func TestContractValidatorCatchesMismatches(t *testing.T) {
	check := func(method, path string, status int, body string) bool {
		req, _ := http.NewRequest(method, "https://relay.tinggalpake.com"+path, nil)
		res := &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(bytes.NewReader([]byte(body))), Request: req}
		ok, _ := contract.ValidateHttpResponse(req, res)
		return ok
	}
	if !check("GET", "/v1/tenants/acme/endpoints", 200, `{"data":[],"next_cursor":null}`) {
		t.Fatal("a valid response was rejected")
	}
	bad := []struct{ path, body string }{
		{"/v1/tenants/acme/endpoints", `{"data":"nope","next_cursor":null}`},
		{"/v1/tenants/acme/endpoints", `{"data":[]}`},
		{"/v1/tenants/acme/deliveries", `{"data":[{"id":"dlv_1","status":"lost"}],"next_cursor":null}`},
		{"/healthz", `{"status":"ok"}`},
	}
	for _, b := range bad {
		if check("GET", b.path, 200, b.body) {
			t.Errorf("%s accepted %s", b.path, b.body)
		}
	}
}
