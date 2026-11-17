package jazz

import (
	"errors"
	"testing"
)

func TestErrorsMatchTheirSentinels(t *testing.T) {
	for _, c := range []struct {
		json string
		is   error
	}{
		{`{"kind":"closed","message":"closed"}`, ErrClosed},
		{`{"kind":"io","message":"disk full"}`, ErrIO},
		{`{"kind":"refused","message":"config.appId must be a UUID"}`, ErrRefused},
		{`{"kind":"refused","message":"not supported yet: includes"}`, ErrNotSupported},
		{`{"kind":"permission","message":"insert on tasks","table":"tasks"}`, ErrPermission},
		{`{"kind":"conflict","message":"serializable conflict"}`, ErrConflict},
		{`plain text`, ErrRefused},
	} {
		err := parseError(c.json)
		if !errors.Is(err, c.is) {
			t.Errorf("%s: %v is not %v", c.json, err, c.is)
		}
		var jerr *Error
		if !errors.As(err, &jerr) {
			t.Errorf("%s: not a *Error", c.json)
		}
	}
	if err := parseError(`{"kind":"permission","message":"x","table":"tasks"}`); err.(*Error).Table != "tasks" {
		t.Errorf("table: %+v", err)
	}
	if errors.Is(parseError(`{"kind":"io","message":"x"}`), ErrClosed) {
		t.Error("io is not closed")
	}
}
