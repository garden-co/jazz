package jazz

import (
	"context"
	"errors"
	"testing"
)

func TestCredentialAuthJSON(t *testing.T) {
	auth, err := Credential(func(context.Context, TokenReason) (string, error) { return "t", nil }).authJSON()
	if err != nil || auth["credential"] != true || auth["bearer"] != nil {
		t.Fatalf("credential auth = %v, %v", auth, err)
	}
}

func TestBackendClientErrors(t *testing.T) {
	err := parseError(`{"kind":"noAccount","message":"no account","userId":"u1"}`)
	var e *Error
	if !errors.Is(err, ErrNoAccount) || !errors.As(err, &e) || e.UserID != "u1" {
		t.Fatalf("noAccount = %#v", err)
	}
	err = parseError(`{"kind":"unsynced","message":"2 writes unsynced","pending":2}`)
	if !errors.Is(err, ErrUnsynced) || !errors.As(err, &e) || e.Pending != 2 {
		t.Fatalf("unsynced = %#v", err)
	}
	err = parseError(`{"kind":"permission","message":"the main backend refused to act for u1"}`)
	if !errors.Is(err, ErrPermission) {
		t.Fatalf("403 = %#v", err)
	}
}
