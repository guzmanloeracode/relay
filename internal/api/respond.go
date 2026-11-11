package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/guzmanloeracode/relay/internal/ids"
)

const errorsDoc = "https://github.com/guzmanloeracode/relay/blob/main/docs/errors.md#"

// FieldError is one invalid field in a 422 response.
type FieldError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

// Problem is an RFC 9457 problem details body.
type Problem struct {
	Type   string       `json:"type"`
	Title  string       `json:"title"`
	Status int          `json:"status"`
	Code   string       `json:"code"`
	Detail string       `json:"detail,omitempty"`
	Errors []FieldError `json:"errors,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func problem(w http.ResponseWriter, status int, code, detail string) {
	writeProblem(w, Problem{Status: status, Code: code, Detail: detail})
}

func writeProblem(w http.ResponseWriter, p Problem) {
	p.Type = errorsDoc + p.Code
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func invalid(w http.ResponseWriter, errs []FieldError) {
	writeProblem(w, Problem{Status: http.StatusUnprocessableEntity, Code: "validation_failed",
		Title: "Validation failed", Detail: "One or more fields are invalid.", Errors: errs})
}

func notFound(w http.ResponseWriter, what string) {
	problem(w, http.StatusNotFound, "not_found", what+" not found.")
}

func internal(w http.ResponseWriter) {
	problem(w, http.StatusInternalServerError, "internal", "Something went wrong on our side. Try again.")
}

const maxBody = 512 << 10

// decode reads a JSON object strictly: unknown fields and trailing data are errors.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			problem(w, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("The body must be at most %d bytes.", maxBody))
			return false
		}
		problem(w, http.StatusBadRequest, "invalid_body", "The body could not be read.")
		return false
	}
	return decodeBytes(w, body, dst)
}

func decodeBytes(w http.ResponseWriter, body []byte, dst any) bool {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &typeErr):
			invalid(w, []FieldError{{Path: typeErr.Field, Message: "must be " + jsonKind(typeErr.Type.Kind().String())}})
		case strings.HasPrefix(err.Error(), "json: unknown field "):
			field := strings.Trim(strings.TrimPrefix(err.Error(), "json: unknown field "), `"`)
			invalid(w, []FieldError{{Path: field, Message: "is not a known field"}})
		default:
			problem(w, http.StatusBadRequest, "invalid_json", "The body is not valid JSON: "+err.Error())
		}
		return false
	}
	if dec.More() {
		problem(w, http.StatusBadRequest, "invalid_json", "The body has data after the JSON object.")
		return false
	}
	return true
}

func jsonKind(k string) string {
	switch k {
	case "string":
		return "a string"
	case "bool":
		return "true or false"
	case "slice", "array":
		return "an array"
	case "map", "struct":
		return "an object"
	default:
		return "a number"
	}
}

// page holds the parsed ?limit and ?cursor of a list request.
type page struct {
	limit  int32
	before *uuid.UUID
}

func parsePage(w http.ResponseWriter, r *http.Request, prefix string) (page, bool) {
	p := page{limit: 50}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 1 || n > 100 {
			invalid(w, []FieldError{{Path: "limit", Message: "must be between 1 and 100"}})
			return p, false
		}
		p.limit = int32(n)
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		id, err := ids.Decode(prefix, v)
		if err != nil {
			invalid(w, []FieldError{{Path: "cursor", Message: "is not a cursor from this list"}})
			return p, false
		}
		p.before = &id
	}
	return p, true
}

// List is the shape of every list response.
type List[T any] struct {
	Data       []T     `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

func listOf[R any, T any](rows []R, limit int32, view func(R) T, id func(R) string) List[T] {
	out := List[T]{Data: make([]T, len(rows))}
	for i, r := range rows {
		out.Data[i] = view(r)
	}
	if len(rows) == int(limit) && len(rows) > 0 {
		c := id(rows[len(rows)-1])
		out.NextCursor = &c
	}
	return out
}

// pathID decodes a public ID from the URL; a malformed ID is reported as not found.
func pathID(w http.ResponseWriter, raw, prefix, what string) (uuid.UUID, bool) {
	id, err := ids.Decode(prefix, raw)
	if err != nil {
		notFound(w, what)
		return uuid.Nil, false
	}
	return id, true
}
