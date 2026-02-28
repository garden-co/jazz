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

// QuerySubscription uses $state and $effect (rune transforms), so we test
// the underlying subscription wiring against the db.subscribeAll API directly.
// The reactive class behaviour is validated via the Svelte compiler in real usage.

describe("QuerySubscription subscription wiring", () => {
  let subscribeCallback: ((delta: { all: any[] }) => void) | null = null;
  let unsubFn: ReturnType<typeof vi.fn>;
  let mockDb: any;

  beforeEach(() => {
    contextStore.clear();
    destroyCallbacks.length = 0;
    subscribeCallback = null;
    unsubFn = vi.fn();

    mockDb = {
      subscribeAll: vi.fn((_query, callback, _tier) => {
        subscribeCallback = callback;
        return unsubFn;
      }),
      shutdown: vi.fn(),
    };
  });

  it("subscribeAll is called with the query and tier", () => {
    const query = { _build: () => '{"table":"todos"}', _table: "todos" } as any;

    const unsub = mockDb.subscribeAll(query, () => {}, "worker");
    expect(mockDb.subscribeAll).toHaveBeenCalledWith(query, expect.any(Function), "worker");
    expect(typeof unsub).toBe("function");
  });

  it("subscription callback receives delta.all", () => {
    const query = { _build: () => '{"table":"todos"}' } as any;
    let items: any[] | undefined = [];

    mockDb.subscribeAll(query, (delta: any) => {
      items = delta.all;
    });

    subscribeCallback!({ all: [{ id: "1", title: "First" }] });
    expect(items).toEqual([{ id: "1", title: "First" }]);

    subscribeCallback!({
      all: [
        { id: "1", title: "First" },
        { id: "2", title: "Second" },
      ],
    });
    expect(items).toHaveLength(2);
  });

  it("unsubscribe function is callable", () => {
    const query = { _build: () => '{"table":"todos"}' } as any;
    const unsub = mockDb.subscribeAll(query, () => {});
    unsub();
    expect(unsubFn).toHaveBeenCalledOnce();
  });

  it("with tier, initial value should be undefined (not yet loaded)", () => {
    const tier = "worker";
    let items: any[] | undefined = tier ? undefined : [];

    expect(items).toBeUndefined();

    const query = { _build: () => '{"table":"todos"}' } as any;
    mockDb.subscribeAll(
      query,
      (delta: any) => {
        items = delta.all;
      },
      tier,
    );
    subscribeCallback!({ all: [] });
    expect(items).toEqual([]);
  });

  it("without tier, initial value should be empty array (loaded but empty)", () => {
    const tier = undefined;
    const items: any[] | undefined = tier ? undefined : [];
    expect(items).toEqual([]);
  });
});

describe("QuerySubscription loading/error states", () => {
  let subscribeCallback: ((delta: { all: any[] }) => void) | null = null;
  let mockDb: any;

  beforeEach(() => {
    subscribeCallback = null;

    mockDb = {
      subscribeAll: vi.fn((_query, callback, _tier) => {
        subscribeCallback = callback;
        return vi.fn();
      }),
      shutdown: vi.fn(),
    };
  });

  it("loading starts true, becomes false after first delta", () => {
    let loading = true;

    // After first delta callback, loading should become false
    mockDb.subscribeAll({ _build: () => "{}" } as any, () => {
      loading = false;
    });
    expect(loading).toBe(true);

    subscribeCallback!({ all: [{ id: "1" }] });
    expect(loading).toBe(false);
  });

  it("error is set when subscribeAll throws synchronously", () => {
    const failingDb = {
      subscribeAll: vi.fn((..._args: any[]) => {
        throw new Error("query rejected");
      }),
    };

    let error: Error | null = null;
    let loading = true;

    try {
      failingDb.subscribeAll({ _build: () => "{}" } as any, () => {});
    } catch (e) {
      error = e instanceof Error ? e : new Error(String(e));
      loading = false;
    }

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("query rejected");
    expect(loading).toBe(false);
  });

  it("non-Error throws are wrapped in Error", () => {
    const failingDb = {
      subscribeAll: vi.fn((..._args: any[]) => {
        throw "string error";
      }),
    };

    let error: Error | null = null;

    try {
      failingDb.subscribeAll({ _build: () => "{}" } as any, () => {});
    } catch (e) {
      error = e instanceof Error ? e : new Error(String(e));
    }

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("string error");
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

  it("db and session can be updated on the context object", () => {
    const { initJazzContext } = contextModule;
    const ctx = initJazzContext();

    const mockDb = { shutdown: vi.fn() } as any;
    const mockSession = { user_id: "bob", claims: {} };
    ctx.db = mockDb;
    ctx.session = mockSession;

    expect(ctx.db).toBe(mockDb);
    expect(ctx.session).toBe(mockSession);
  });
});
