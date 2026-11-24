package jazz

// End-to-end encryption for an app that declares it. A device keeps its keys
// in a private store the app supplies (EncryptionConfig.Store): the Keychain
// on Apple, an OS key store elsewhere. jazz-go ships none.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Stored is one document of a private store, with its revision.
type Stored struct {
	Bytes    []byte
	Revision uint64
}

// ErrStoreConflict is what Save returns when the document's revision is not
// the one expected; ErrStoreWiped when the scope is not of the incarnation
// given (it was deleted since). Nothing is written in either case.
var (
	ErrStoreConflict = errors.New("jazz: the private store's document changed")
	ErrStoreWiped    = errors.New("jazz: the private store's scope was wiped")
)

// PrivateStore is where a device keeps its encryption keys. It is called on
// the client's thread, one request at a time, so answer quickly. Keep the
// documents in files or rows you protect with the platform's key store (see
// Protect and Unprotect).
type PrivateStore interface {
	// OpenScope is the incarnation of scope, creating the scope with a new
	// random, non-zero incarnation if there is none.
	OpenScope(scope string) (uint64, error)
	// Load is the document, or nil if there is none.
	Load(scope, doc string) (*Stored, error)
	// Save replaces the document if the scope is still of incarnation and the
	// document's revision is still expected (nil: it must not exist),
	// atomically, and returns the new revision. Otherwise it returns
	// ErrStoreWiped or ErrStoreConflict and writes nothing.
	Save(scope string, incarnation uint64, doc string, bytes []byte, expected *uint64) (uint64, error)
	// List is the names of the scope's documents.
	List(scope string) ([]string, error)
	// Delete removes the scope and every document in it: the scope's
	// incarnation first, then its documents.
	Delete(scope string) error
	// Protect protects a secret at rest (encrypt it with a key the platform
	// keeps); Unprotect recovers it. Jazz clears the secret it passes to
	// Protect once Protect returns, and the slice Unprotect returns once it
	// has read it: keep neither (return a copy from Unprotect if you keep the
	// secret yourself).
	Protect(secret []byte) ([]byte, error)
	Unprotect(blob []byte) ([]byte, error)
}

// EncryptionConfig is what an app that declares end-to-end encryption opens
// with: where this device keeps its keys. Set Config.Encryption.
type EncryptionConfig struct {
	// Store is the device's private store.
	Store PrivateStore
	// Environment is the deployment's (default "dev"): the same for every
	// client of one deployment.
	Environment string
}

// private stores, by the id jazz-ffi's store callback gets.
var (
	stores    sync.Map
	nextStore atomic.Uint64
)

// onStore is jazz-ffi's store callback: 0 with *payload the result as JSON,
// or non-zero with *payload the error.
func onStore(ctx, request, payload uintptr) (status int32) {
	out := (*uintptr)(unsafe.Add(unsafe.Pointer(nil), payload))
	*out = 0
	defer func() {
		if r := recover(); r != nil {
			*out = mallocString(fmt.Sprintf("the private store panicked: %v", r))
			status = 1
		}
	}()
	value, ok := stores.Load(uint64(ctx))
	if !ok {
		*out = mallocString("the app is closed")
		return 1
	}
	done, text := answerStore(value.(PrivateStore), goString(request))
	*out = mallocString(text) // jazz-ffi frees it
	if done {
		return 0
	}
	return 1
}

// storeRequest is one request of the private store contract: bytes are
// base64 and numbers that may exceed 2^53 are decimal text.
type storeRequest struct {
	Op          string  `json:"op"`
	Scope       string  `json:"scope"`
	Doc         string  `json:"doc"`
	Incarnation string  `json:"incarnation"`
	Bytes       string  `json:"bytes"`
	Expected    *string `json:"expected"`
	Secret      string  `json:"secret"`
	Blob        string  `json:"blob"`
}

// answerStore runs a request against store: ok with the result as JSON, or
// not ok with "conflict", "wiped" or the failure's message.
//
// Requests and answers carry key bytes as base64. The byte slices this
// wrapper owns are cleared once used; Go strings (the request, the answer and
// the base64 texts in them) are immutable and cannot be, so they stay in
// memory until the garbage collector reuses it. jazz-ffi wipes its own
// copies, including the C string handed back.
func answerStore(store PrivateStore, request string) (bool, string) {
	var r storeRequest
	raw := []byte(request)
	err := json.Unmarshal(raw, &r)
	clear(raw)
	if err != nil {
		return false, "the private store request is not JSON: " + err.Error()
	}
	fail := func(err error) (bool, string) {
		switch {
		case errors.Is(err, ErrStoreConflict):
			return false, "conflict"
		case errors.Is(err, ErrStoreWiped):
			return false, "wiped"
		}
		return false, err.Error()
	}
	text := func(v any) (bool, string) {
		b, err := json.Marshal(v)
		if err != nil {
			return false, err.Error()
		}
		answer := string(b)
		clear(b)
		return true, answer
	}
	b64 := base64.StdEncoding
	switch r.Op {
	case "openScope":
		n, err := store.OpenScope(r.Scope)
		if err != nil {
			return fail(err)
		}
		return text(strconv.FormatUint(n, 10))
	case "load":
		stored, err := store.Load(r.Scope, r.Doc)
		if err != nil {
			return fail(err)
		}
		if stored == nil {
			return true, "null"
		}
		return text(map[string]string{
			"bytes":    b64.EncodeToString(stored.Bytes),
			"revision": strconv.FormatUint(stored.Revision, 10),
		})
	case "save":
		incarnation, err := strconv.ParseUint(r.Incarnation, 10, 64)
		if err != nil {
			return fail(err)
		}
		data, err := b64.DecodeString(r.Bytes)
		if err != nil {
			return fail(err)
		}
		var expected *uint64
		if r.Expected != nil {
			n, err := strconv.ParseUint(*r.Expected, 10, 64)
			if err != nil {
				return fail(err)
			}
			expected = &n
		}
		revision, err := store.Save(r.Scope, incarnation, r.Doc, data, expected)
		if err != nil {
			return fail(err)
		}
		return text(strconv.FormatUint(revision, 10))
	case "list":
		names, err := store.List(r.Scope)
		if err != nil {
			return fail(err)
		}
		if names == nil {
			names = []string{}
		}
		return text(names)
	case "delete":
		if err := store.Delete(r.Scope); err != nil {
			return fail(err)
		}
		return true, "null"
	case "protect":
		secret, err := b64.DecodeString(r.Secret)
		if err != nil {
			return fail(err)
		}
		blob, err := store.Protect(secret)
		if err != nil {
			clear(secret)
			return fail(err)
		}
		answer, answerText := text(b64.EncodeToString(blob))
		clear(secret)
		return answer, answerText
	case "unprotect":
		blob, err := b64.DecodeString(r.Blob)
		if err != nil {
			return fail(err)
		}
		secret, err := store.Unprotect(blob)
		if err != nil {
			return fail(err)
		}
		answer, answerText := text(b64.EncodeToString(secret))
		clear(secret)
		return answer, answerText
	}
	return false, "unknown private store operation " + r.Op
}

// Encryption is the app's end-to-end encryption: this device's account, its
// other devices, its recovery and its keys. Every call refuses for an app that
// declares none; a backend holds no keys and refuses too.
type Encryption struct{ db *DB }

// Encryption is the handle to the app's encryption.
func (db *DB) Encryption() *Encryption { return &Encryption{db: db} }

// EncryptionCode is the code of an encryption failure
// ("approval-code-mismatch", "no-challenge", "invalid-checksum", ...), or ""
// when err is not one.
func EncryptionCode(err error) string {
	var e *Error
	if !errors.As(err, &e) {
		return ""
	}
	return e.Code
}

// Call runs one of the runtime's operations by name (see the README) with
// args, decoding its result into out (nil to ignore it).
func (e *Encryption) Call(ctx context.Context, op string, args, out any) error {
	return e.db.call(ctx, "encryptionCall", map[string]any{"op": op, "args": args}, out)
}

// Ready verifies the account and enrols or recovers this device, waiting up
// to timeout: "active", "awaiting-approval" or "reenrolled".
func (e *Encryption) Ready(ctx context.Context, timeout time.Duration) (string, error) {
	var status string
	err := e.Call(ctx, "waitUntilReady", map[string]any{"timeoutMs": timeout.Milliseconds()}, &status)
	return status, err
}

// EncryptionState is what the runtime's loop is doing.
type EncryptionState struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Stage  string `json:"stage"`
}

// State is what the runtime's loop is doing now.
func (e *Encryption) State(ctx context.Context) (EncryptionState, error) {
	var s EncryptionState
	err := e.Call(ctx, "state", map[string]any{}, &s)
	return s, err
}

// Reports are the reports waiting (type "unreadable", "missing-key",
// "challenged", "ready-failed", ...), each as JSON.
func (e *Encryption) Reports(ctx context.Context) ([]json.RawMessage, error) {
	var reports []json.RawMessage
	err := e.Call(ctx, "reports", nil, &reports)
	return reports, err
}

// DeviceID is this installation's device ID.
func (e *Encryption) DeviceID(ctx context.Context) (string, error) {
	var id string
	err := e.Call(ctx, "deviceId", map[string]any{}, &id)
	return id, err
}

// Device is a device of this account: active, or waiting for approval.
type Device struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	// RemovalRequested says whether this device has published a request to be removed.
	RemovalRequested bool `json:"removalRequested"`
}

// Devices lists the account's devices.
func (e *Encryption) Devices(ctx context.Context) ([]Device, error) {
	var devices []Device
	err := e.Call(ctx, "devices", map[string]any{}, &devices)
	return devices, err
}

// Challenge starts approving a waiting device; Approve finishes it once the
// user confirmed both devices show the same code (ApprovalCode here,
// Answer on the new device).
func (e *Encryption) Challenge(ctx context.Context, deviceID string) error {
	return e.Call(ctx, "challenge", map[string]any{"deviceId": deviceID}, nil)
}

// Answer is, on the new device, the code to show once it was challenged.
func (e *Encryption) Answer(ctx context.Context) (string, error) {
	var code string
	err := e.Call(ctx, "answer", map[string]any{}, &code)
	return code, err
}

// ApprovalCode is, on the approving device, the code to compare ("" until
// the new device answered).
func (e *Encryption) ApprovalCode(ctx context.Context, deviceID string) (string, error) {
	var code *string
	if err := e.Call(ctx, "approvalCode", map[string]any{"deviceId": deviceID}, &code); err != nil || code == nil {
		return "", err
	}
	return *code, nil
}

// Approve approves the device if code is the one ApprovalCode gave.
func (e *Encryption) Approve(ctx context.Context, deviceID, code string) error {
	return e.Call(ctx, "approve", map[string]any{"deviceId": deviceID, "code": code}, nil)
}

// RemoveDevice removes a device and rotates the account.
func (e *Encryption) RemoveDevice(ctx context.Context, deviceID string) error {
	return e.Call(ctx, "remove", map[string]any{"deviceId": deviceID}, nil)
}

// KeyDeliveryResult counts the deliveries written and the spaces whose
// delivery failed in one pass.
type KeyDeliveryResult struct {
	Delivered uint64 `json:"delivered"`
	Failed    uint64 `json:"failed"`
}

// KeysDeliver runs a delivery pass for new members, newly approved devices
// and rotated spaces. Check result.Failed for failures within the pass.
func (e *Encryption) KeysDeliver(ctx context.Context) (KeyDeliveryResult, error) {
	var result KeyDeliveryResult
	err := e.Call(ctx, "keysDeliver", map[string]any{}, &result)
	return result, err
}

// KeysRotate rotates the key of the space row id of table.
func (e *Encryption) KeysRotate(ctx context.Context, table, id string) error {
	return e.Call(ctx, "keysRotate", map[string]any{"table": table, "id": id}, nil)
}

// RecoveryCreate makes a recovery phrase (24 words) to keep. It registers
// nothing: RecoveryRegister does. Only for an account the app signs in itself
// (a device account's recovery is its account key).
func (e *Encryption) RecoveryCreate(ctx context.Context) (string, error) {
	var r struct {
		Phrase string `json:"phrase"`
	}
	err := e.Call(ctx, "recoveryCreate", map[string]any{}, &r)
	return r.Phrase, err
}

// RecoveryRegister registers the recovery root phrase gives, from a device
// that is active.
func (e *Encryption) RecoveryRegister(ctx context.Context, phrase string) error {
	return e.Call(ctx, "recoveryRegister", map[string]any{"phrase": phrase}, nil)
}

// RecoveryRestore approves this new device with the phrase: its status.
func (e *Encryption) RecoveryRestore(ctx context.Context, phrase string) (string, error) {
	var status string
	err := e.Call(ctx, "recoveryRestore", map[string]any{"phrase": phrase}, &status)
	return status, err
}
