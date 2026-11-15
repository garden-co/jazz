// The C side of Subscribe: jazz-ffi calls back into Go through it.
#include <stdint.h>
#include "jazz_ffi.h"
#include "_cgo_export.h"

static void jazz_go_callback(void* ctx, const char* kind, const char* json) {
	jazzGoSubscriptionCallback((uintptr_t)ctx, (char*)kind, (char*)json);
}

// The context is a runtime/cgo.Handle, passed as an integer (not a Go
// pointer) and only turned into the C ABI's void* here.
JazzSubscription* jazz_go_subscribe(JazzHandle* h, const char* q, uintptr_t ctx, char** err) {
	return jazz_subscribe(h, q, jazz_go_callback, (void*)ctx, err);
}
