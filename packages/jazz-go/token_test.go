package jazz

import (
	"context"
	"strings"
	"testing"
	"unsafe"
)

// A panicking TokenProvider fails as a failed fetch (status 1, its
// message the panic) and the panic doesn't cross into jazz-ffi.
func TestTokenProviderPanic(t *testing.T) {
	if err := loadFFI(); err != nil {
		t.Skipf("no libjazz_ffi: %v", err)
	}
	id := nextProvider.Add(1)
	providers.Store(id, TokenProvider(func(context.Context, TokenReason) (string, error) { panic("boom") }))
	defer providers.Delete(id)
	reason := cstr("expired")
	var token uintptr
	status := onToken(uintptr(id), uintptr(unsafe.Pointer(reason)), uintptr(unsafe.Pointer(&token)))
	if status != 1 || token == 0 {
		t.Fatalf("status %d, token %#x", status, token)
	}
	if msg := goString(token); !strings.Contains(msg, "panicked: boom") {
		t.Fatalf("message %q", msg)
	}
}
