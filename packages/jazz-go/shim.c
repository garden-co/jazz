// The C side of Subscribe: jazz-ffi calls back into Go through it.
#include <stdint.h>
#include "jazz_ffi.h"
#include "_cgo_export.h"

static void jazz_go_callback(void* ctx, const char* kind, const char* json) {
	jazzGoSubscriptionCallback((uintptr_t)ctx, (char*)kind, (char*)json);
}

// The context is the subscription's integer id (a key of the Go side's
// subscribers map, never a Go pointer), only turned into the C ABI's
// void* here.
JazzSubscription* jazz_go_subscribe(JazzHandle* h, const char* q, uintptr_t ctx, char** err) {
	return jazz_subscribe(h, q, jazz_go_callback, (void*)ctx, err);
}

extern char* jazzGoTokenCallback(uintptr_t ctx, char* reason);

static char* jazz_go_token(void* ctx, const char* reason) {
	return jazzGoTokenCallback((uintptr_t)ctx, (char*)reason);
}

JazzHandle* jazz_go_open_user(const char* config, uintptr_t ctx, char** err) {
	return jazz_open_user(config, jazz_go_token, (void*)ctx, err);
}

JazzWatch* jazz_go_watch(JazzHandle* h, const char* what, uintptr_t ctx, char** err) {
	return jazz_watch(h, what, jazz_go_callback, (void*)ctx, err);
}
