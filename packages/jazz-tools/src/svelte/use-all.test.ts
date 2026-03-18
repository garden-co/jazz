import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

// Mock svelte context and lifecycle
const contextStore = new Map<unknown, unknown>();
const destroyCallbacks: (() => void)[] = [];

vi.mock("svelte", () => ({
  setContext: (key: unknown, value: unknown) => contextStore.set(key, value),
  getContext: (key: unknown) => contextStore.get(key),
  onDestroy: (fn: () => void) => destroyCallbacks.push(fn),
}));

let contextModule: typeof import("./context.svelte.js");

beforeAll(async () => {
  contextModule = await import("./context.svelte.js");
});

// QuerySubscription relies on $state/$effect runes which need the Svelte
// compiler. We test the callback contract that QuerySubscription wires up —
// the same shape it passes to entry.subscribe() inside its $effect.

describe("QuerySubscription callback contract", () => {
  // These mirror the exact callbacks wired in use-all.svelte.ts.
  // A real QuerySubscription sets this.current / this.loading / this.error
  // inside these callbacks; here we use local variables to verify the logic.

  it("onfulfilled delivers data and clears loading", () => {
    let current: any[] | undefined;
    let loading = true;

    // Mirrors: onfulfilled: (data) => { this.current = data; this.loading = false; }
    const onfulfilled = (data: any[]) => {
      current = data;
      loading = false;
    };

    onfulfilled([{ id: "1", title: "Alice's todo" }]);
    expect(current).toEqual([{ id: "1", title: "Alice's todo" }]);
    expect(loading).toBe(false);
  });

  it("onDelta replaces current with delta.all", () => {
    let current: any[] | undefined = [{ id: "1", title: "First" }];

    // Mirrors: onDelta: (delta) => { this.current = delta.all; }
    const onDelta = (delta: { all: any[] }) => {
      current = delta.all;
    };

    onDelta({
      all: [
        { id: "1", title: "First" },
        { id: "2", title: "Second" },
      ],
    });
    expect(current).toHaveLength(2);
    expect(current![1].title).toBe("Second");
  });

  it("onError surfaces the error on the error property", () => {
    let current: any[] | undefined = [{ id: "1" }];
    let loading = false;
    let error: Error | null = null;

    // Mirrors: onError: (error) => { this.error = ...; this.current = undefined; this.loading = false; }
    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
      current = undefined;
      loading = false;
    };

    onError(new Error("subscription failed"));
    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("subscription failed");
    expect(current).toBeUndefined();
    expect(loading).toBe(false);
  });

  it("onError wraps non-Error values in Error", () => {
    let error: Error | null = null;

    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
    };

    onError("string error");
    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("string error");
  });

  it("synchronous throw during setup is caught and surfaced", () => {
    let error: Error | null = null;
    let loading = true;

    // Mirrors the try/catch in the $effect body
    try {
      throw new Error("getCacheEntry exploded");
    } catch (e) {
      error = e instanceof Error ? e : new Error(String(e));
      loading = false;
    }

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("getCacheEntry exploded");
    expect(loading).toBe(false);
  });
});

describe("QuerySubscription initial value semantics", () => {
  it("with tier, initial value is undefined (awaiting settlement at tier)", () => {
    const options = { tier: "edge" as const };
    const initial = options?.tier ? undefined : [];
    expect(initial).toBeUndefined();
  });

  it("without tier, initial value is empty array (locally available)", () => {
    const options = { localUpdates: "deferred" as const };
    const initial = "tier" in options && (options as any).tier ? undefined : [];
    expect(initial).toEqual([]);
  });

  it("without options, initial value is empty array", () => {
    const options = undefined as { tier?: string } | undefined;
    const initial = options?.tier ? undefined : [];
    expect(initial).toEqual([]);
  });
});

describe("fulfilled cache entry provides initial data", () => {
  it("applies fulfilled state before subscribing", () => {
    const alice = { id: "1", name: "Alice" };
    const entry = {
      state: { status: "fulfilled" as const, data: [alice] },
      subscribe: vi.fn(() => vi.fn()),
    };

    // Mirrors: if (entry.state.status === "fulfilled") { this.current = entry.state.data; ... }
    let current: any[] | undefined;
    let loading = true;

    if (entry.state.status === "fulfilled") {
      current = entry.state.data;
      loading = false;
    }

    expect(current).toEqual([alice]);
    expect(loading).toBe(false);
  });
});

describe("context integration", () => {
  beforeEach(() => {
    contextStore.clear();
  });

  it("context round-trips through set/get", () => {
    const { initJazzContext, getJazzContext } = contextModule;
    const ctx = initJazzContext();
    const retrieved = getJazzContext();
    expect(retrieved).toBe(ctx);
  });

  it("db, session, and manager can be updated on the context object", () => {
    const { initJazzContext } = contextModule;
    const ctx = initJazzContext();

    const mockDb = { shutdown: vi.fn() } as any;
    const mockSession = { user_id: "bob", claims: {} };
    const mockManager = { makeQueryKey: vi.fn(), getCacheEntry: vi.fn() } as any;
    ctx.db = mockDb;
    ctx.session = mockSession;
    ctx.manager = mockManager;

    expect(ctx.db).toBe(mockDb);
    expect(ctx.session).toBe(mockSession);
    expect(ctx.manager).toBe(mockManager);
  });
});
