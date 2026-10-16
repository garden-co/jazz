/* task.c: the futures pieces the client uses (wasm_bindgen_futures::spawn_local's task queue,
 * futures::channel::mpsc::unbounded, Waker). The port is synchronous: a spawned task is a
 * closure queued FIFO and run to completion by jz_task_run_ready (the microtask drain). */
#include "rt_internal.h"

typedef struct task { jz_task_fn f; void *ctx; void (*ctx_drop)(void *); } task;

static jz_deque g_tasks;
static bool g_tasks_init, g_running;

void jz_task_spawn(jz_task_fn f, void *ctx, void (*ctx_drop)(void *)) {
    if (!g_tasks_init) {
        jz_deque_init(&g_tasks, sizeof(task));
        g_tasks_init = true;
    }
    task t = {f, ctx, ctx_drop};
    jz_deque_push_back(&g_tasks, &t);
}

void jz_task_run_ready(void) {
    if (g_running || !g_tasks_init) return; /* re-entrant call from inside a task: outer loop drains */
    g_running = true;
    task t;
    while (jz_deque_pop_front(&g_tasks, &t)) {
        t.f(t.ctx);
        if (t.ctx_drop) t.ctx_drop(t.ctx);
    }
    g_running = false;
}

/* unbounded mpsc channel. jz_chan_new returns one allocation shared by the sender and the
 * receiver half: it starts with 2 references and each half calls jz_chan_release once. */
struct jz_chan {
    jz_deque q;
    u32 refs;
    bool closed;
    jz_waker rx_waker;
};

jz_chan *jz_chan_new(u32 elem) {
    jz_chan *c = JZ_NEW(jz_chan);
    jz_deque_init(&c->q, elem);
    c->refs = 2;
    return c;
}

void jz_chan_send(jz_chan *c, const void *item) {
    if (c->closed) return; /* receiver gone: Err(TrySendError) is ignored by callers (`let _ = tx.unbounded_send`) */
    jz_deque_push_back(&c->q, item);
    if (c->rx_waker.wake) {
        jz_waker w = c->rx_waker;
        c->rx_waker.wake = NULL;
        jz_waker_wake(&w);
    }
}

bool jz_chan_try_recv(jz_chan *c, void *out) { return jz_deque_pop_front(&c->q, out); }

void jz_chan_close(jz_chan *c) {
    c->closed = true;
    if (c->rx_waker.wake) {
        jz_waker w = c->rx_waker;
        c->rx_waker.wake = NULL;
        jz_waker_wake(&w);
    }
}

bool jz_chan_is_closed(const jz_chan *c) { return c->closed; }

void jz_chan_release(jz_chan *c) {
    if (!c) return;
    c->closed = true; /* dropping either half disconnects the channel */
    if (--c->refs) return;
    jz_deque_drop(&c->q, NULL);
    jz_free(c);
}

void jz_waker_wake(const jz_waker *w) {
    if (w && w->wake) w->wake(w->ctx);
}
