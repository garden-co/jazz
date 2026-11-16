package jazz

import (
	"encoding/json"
	"errors"
	"strings"
)

// The kinds of error Jazz returns, for errors.Is. Each is wrapped in a
// *Error with the details (errors.As). Context errors stay ctx.Err().
var (
	// ErrClosed: the DB (or the subscription's DB) is closed.
	ErrClosed = errors.New("jazz: closed")
	// ErrRefused: the input is invalid (a value, a query, a config).
	ErrRefused = errors.New("jazz: refused")
	// ErrPermission: the app's permissions refuse it.
	ErrPermission = errors.New("jazz: not permitted")
	// ErrNotSupported: Jazz can't do it (yet).
	ErrNotSupported = errors.New("jazz: not supported yet")
	// ErrConflict: a serializable transaction conflicted; retry it.
	ErrConflict = errors.New("jazz: conflict")
	// ErrIO: storage or the network failed.
	ErrIO = errors.New("jazz: i/o")
)

// Kinds of Error.
const (
	KindClosed       = "closed"
	KindRefused      = "refused"
	KindPermission   = "permission"
	KindNotSupported = "notSupported"
	KindConflict     = "conflict"
	KindIO           = "io"
)

// Error is what Jazz refused or failed at.
type Error struct {
	// Kind is one of the Kind constants.
	Kind    string
	Message string
	// Table and Column name what the error is about, where known.
	Table, Column string
}

func (e *Error) Error() string { return "jazz: " + e.Message }

// Unwrap is the sentinel of e's Kind, so errors.Is(err, ErrPermission)
// and the like hold.
func (e *Error) Unwrap() error {
	switch e.Kind {
	case KindClosed:
		return ErrClosed
	case KindPermission:
		return ErrPermission
	case KindNotSupported:
		return ErrNotSupported
	case KindConflict:
		return ErrConflict
	case KindIO:
		return ErrIO
	default:
		return ErrRefused
	}
}

// parseError reads jazz-ffi's error JSON ({"kind","message",…}), or
// plain text.
func parseError(text string) error {
	var e struct {
		Kind    string `json:"kind"`
		Message string `json:"message"`
		Table   string `json:"table"`
		Column  string `json:"column"`
	}
	if json.Unmarshal([]byte(text), &e) != nil || e.Message == "" {
		if strings.EqualFold(text, "closed") {
			return &Error{Kind: KindClosed, Message: "closed"}
		}
		e.Kind, e.Message = KindRefused, text
	}
	kind := normalizeKind(e.Kind, e.Message)
	return &Error{Kind: kind, Message: e.Message, Table: e.Table, Column: e.Column}
}

// normalizeKind maps jazz-ffi's kinds onto Go's. jazz-ffi v1 reports
// "not supported yet" and permission refusals as "refused"; their
// messages tell them apart until it reports them as kinds of their own.
func normalizeKind(kind, message string) string {
	switch strings.ToLower(kind) {
	case "closed":
		return KindClosed
	case "io":
		return KindIO
	case "permission", "permissiondenied", "forbidden":
		return KindPermission
	case "notsupported", "unsupported":
		return KindNotSupported
	case "conflict":
		return KindConflict
	}
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "not supported yet"):
		return KindNotSupported
	case strings.Contains(lower, "permission") || strings.Contains(lower, "not allowed") || strings.Contains(lower, "denied"):
		return KindPermission
	}
	return KindRefused
}
