/* crates/jazz/layers/types/src/local_executor.rs */
#include "jazz_types.h"

static void jazz_types__local_executor__noop_wake(void *ctx) { (void)ctx; }
JZ_IMPORT("thread_yield_now") void jazz_types__local_executor__yield_now(void);

/* block_on<F: Future>: poll with a no-op waker until Ready, yielding on Pending. */
void jazz_types__local_executor__block_on(jazz_types_Future fut, void *out) {
    jz_waker waker = { jazz_types__local_executor__noop_wake, NULL };
    for (;;) {
        if (fut.poll(fut.self, &waker, out)) return;
        jazz_types__local_executor__yield_now();
    }
}
