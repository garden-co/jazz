package jazz

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// memoryStore is a private store in memory, as a host's would be over a file.
type memoryStore struct {
	scopes map[string]*memoryScope
	next   uint64
}

type memoryScope struct {
	incarnation uint64
	docs        map[string]Stored
}

func newMemoryStore() *memoryStore { return &memoryStore{scopes: map[string]*memoryScope{}} }

func (m *memoryStore) OpenScope(scope string) (uint64, error) {
	if s, ok := m.scopes[scope]; ok {
		return s.incarnation, nil
	}
	m.next++
	m.scopes[scope] = &memoryScope{incarnation: m.next * 7919, docs: map[string]Stored{}}
	return m.scopes[scope].incarnation, nil
}

func (m *memoryStore) Load(scope, doc string) (*Stored, error) {
	s, ok := m.scopes[scope]
	if !ok {
		return nil, nil
	}
	if stored, ok := s.docs[doc]; ok {
		return &stored, nil
	}
	return nil, nil
}

func (m *memoryStore) Save(scope string, incarnation uint64, doc string, bytes []byte, expected *uint64) (uint64, error) {
	s, ok := m.scopes[scope]
	if !ok || s.incarnation != incarnation {
		return 0, ErrStoreWiped
	}
	held, exists := s.docs[doc]
	if (expected == nil) == exists || (exists && held.Revision != *expected) {
		return 0, ErrStoreConflict
	}
	revision := held.Revision + 1
	s.docs[doc] = Stored{Bytes: bytes, Revision: revision}
	return revision, nil
}

func (m *memoryStore) List(scope string) ([]string, error) {
	var names []string
	if s, ok := m.scopes[scope]; ok {
		for name := range s.docs {
			names = append(names, name)
		}
	}
	return names, nil
}

func (m *memoryStore) Delete(scope string) error {
	delete(m.scopes, scope)
	return nil
}

// A real host protects the secret with the platform's key store.
func (m *memoryStore) Protect(secret []byte) ([]byte, error) { return secret, nil }
func (m *memoryStore) Unprotect(blob []byte) ([]byte, error) { return blob, nil }

func ask(t *testing.T, store PrivateStore, request map[string]any) (bool, string) {
	t.Helper()
	text, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return answerStore(store, string(text))
}

// The request contract the Rust runtime speaks: a store written in Go answers
// it with its own typed methods, and conflicts and wiped scopes keep their
// names.
func TestThePrivateStoreAnswersTheRuntimesRequests(t *testing.T) {
	store := newMemoryStore()
	ok, text := ask(t, store, map[string]any{"op": "openScope", "scope": "device"})
	if !ok {
		t.Fatalf("openScope: %s", text)
	}
	var incarnation string
	if err := json.Unmarshal([]byte(text), &incarnation); err != nil {
		t.Fatal(err)
	}
	if n, err := strconv.ParseUint(incarnation, 10, 64); err != nil || n == 0 {
		t.Fatalf("an incarnation is a non-zero decimal: %q", incarnation)
	}
	ok, text = ask(t, store, map[string]any{"op": "load", "scope": "device", "doc": "keys"})
	if !ok || text != "null" {
		t.Fatalf("load of nothing: %v %s", ok, text)
	}
	data := base64.StdEncoding.EncodeToString([]byte("sealed"))
	save := map[string]any{"op": "save", "scope": "device", "incarnation": incarnation, "doc": "keys", "bytes": data, "expected": nil}
	if ok, text = ask(t, store, save); !ok || text != `"1"` {
		t.Fatalf("first save: %v %s", ok, text)
	}
	// The same save again expects no document: a conflict, by name.
	if ok, text = ask(t, store, save); ok || text != "conflict" {
		t.Fatalf("a stale save: %v %s", ok, text)
	}
	save["expected"] = "1"
	if ok, text = ask(t, store, save); !ok || text != `"2"` {
		t.Fatalf("save of the next revision: %v %s", ok, text)
	}
	save["incarnation"] = "1"
	if ok, text = ask(t, store, save); ok || text != "wiped" {
		t.Fatalf("a save into a wiped scope: %v %s", ok, text)
	}
	ok, text = ask(t, store, map[string]any{"op": "load", "scope": "device", "doc": "keys"})
	var loaded map[string]string
	if err := json.Unmarshal([]byte(text), &loaded); err != nil || !ok {
		t.Fatalf("load: %v %s", ok, text)
	}
	if loaded["bytes"] != data || loaded["revision"] != "2" {
		t.Fatalf("load: %v", loaded)
	}
	if ok, text = ask(t, store, map[string]any{"op": "list", "scope": "device"}); !ok || text != `["keys"]` {
		t.Fatalf("list: %v %s", ok, text)
	}
	if ok, text = ask(t, store, map[string]any{"op": "list", "scope": "other"}); !ok || text != `[]` {
		t.Fatalf("list of nothing is an empty list: %v %s", ok, text)
	}
	if ok, _ = ask(t, store, map[string]any{"op": "delete", "scope": "device"}); !ok {
		t.Fatal("delete")
	}
	if ok, text = ask(t, store, map[string]any{"op": "bogus"}); ok {
		t.Fatalf("an unknown operation is refused: %s", text)
	}
	if ok, _ = answerStore(store, "not json"); ok {
		t.Fatal("a request that is not JSON is refused")
	}
}

func TestEncryptionCodeNamesTheFailure(t *testing.T) {
	err := parseError(`{"kind":"refused","message":"the code differs","code":"approval-code-mismatch"}`)
	if got := EncryptionCode(err); got != "approval-code-mismatch" {
		t.Fatalf("code = %q", got)
	}
	if got := EncryptionCode(&Error{Kind: KindRefused, Message: "encryption (approval-code-mismatch): the code differs"}); got != "" {
		t.Fatalf("code = %q", got)
	}
}

// All three helpers reach the C ABI and preserve its typed failure.
func TestMembershipCallsRequireAnEncryptedApp(t *testing.T) {
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	app, err := os.ReadFile("../../examples/docs/todo-client-go/jazz.app.json")
	if err != nil {
		t.Skipf("outside the repository: %v", err)
	}
	ctx := context.Background()
	id := "8f3c2a7e-1b4d-4c9e-9a2f-6d5e4c3b2a10"
	db, err := Open(ctx, Config{App: string(app), AppID: id})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	members := db.Encryption()
	_, addErr := members.AddMember(ctx, "todos", id, id, WaitFor(WriteLocal))
	_, listErr := members.ListMembers(ctx, "todos", id)
	_, removeErr := members.RemoveMember(ctx, "todos", id, id)
	for _, call := range []struct {
		name string
		err  error
	}{{"add", addErr}, {"list", listErr}, {"remove", removeErr}} {
		if got := EncryptionCode(call.err); got != "not-declared" {
			t.Errorf("%s: code %q, error %v", call.name, got, call.err)
		}
	}
}

func TestCancelledEncryptedOpenReleasesCallbacksAfterClose(t *testing.T) {
	originalOpen, originalClose := ffi.openEncrypted, ffi.close
	t.Cleanup(func() {
		ffi.openEncrypted, ffi.close = originalOpen, originalClose
	})
	var provider, store uint64
	allowOpen := make(chan struct{})
	closed := make(chan struct{})
	ffi.openEncrypted = func(_ *byte, _, tokenCtx, _, storeCtx uintptr, _ *uintptr) uintptr {
		provider, store = uint64(tokenCtx), uint64(storeCtx)
		<-allowOpen
		return 1
	}
	ffi.close = func(_ uintptr) {
		if _, ok := providers.Load(provider); !ok {
			t.Error("token provider was released before native close returned")
		}
		if _, ok := stores.Load(store); !ok {
			t.Error("private store was released before native close returned")
		}
		close(closed)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := open(ctx, Config{
		Auth:       Token(func(context.Context, TokenReason) (string, error) { return "", nil }),
		Encryption: &EncryptionConfig{Store: newMemoryStore()},
	})
	close(allowOpen)
	if db != nil || !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled open returned db %v, err %v", db, err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("native close did not run")
	}
	defer providers.Delete(provider)
	defer stores.Delete(store)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, hasProvider := providers.Load(provider)
		_, hasStore := stores.Load(store)
		if !hasProvider && !hasStore {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Error("cancelled open retained its callback registrations")
}

func TestEncryptedShutdownReleasesCallbacksAfterDraining(t *testing.T) {
	originalCloseWithin := ffi.closeWithin
	t.Cleanup(func() { ffi.closeWithin = originalCloseWithin })
	db := &DB{handle: 1, provider: nextProvider.Add(1), store: nextStore.Add(1)}
	providers.Store(db.provider, TokenProvider(func(context.Context, TokenReason) (string, error) { return "", nil }))
	stores.Store(db.store, newMemoryStore())
	defer providers.Delete(db.provider)
	defer stores.Delete(db.store)

	ffi.closeWithin = func(handle uintptr, timeoutMs uint64, _ *uintptr) int32 {
		if handle != 1 || timeoutMs != uint64(DefaultShutdownTimeout.Milliseconds()) {
			t.Errorf("draining close received handle %d, timeout %d ms", handle, timeoutMs)
		}
		if _, ok := providers.Load(db.provider); !ok {
			t.Error("token provider was released before native close returned")
		}
		if _, ok := stores.Load(db.store); !ok {
			t.Error("private store was released before native close returned")
		}
		return 0
	}

	if err := db.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := providers.Load(db.provider); ok {
		t.Error("shutdown retained its token provider")
	}
	if _, ok := stores.Load(db.store); ok {
		t.Error("shutdown retained its private store")
	}
}
func TestDeviceListKeepsRemovalRequests(t *testing.T) {
	var devices []Device
	if err := json.Unmarshal([]byte(`[{"id":"a","status":"active","removalRequested":false},{"id":"b","status":"active","removalRequested":true}]`), &devices); err != nil {
		t.Fatal(err)
	}
	if devices[0].RemovalRequested || !devices[1].RemovalRequested {
		t.Fatalf("unexpected removal requests: %+v", devices)
	}
}
