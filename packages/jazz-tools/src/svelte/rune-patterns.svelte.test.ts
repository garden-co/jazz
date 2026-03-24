import { describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import "./test-helpers.svelte.js";

// ── Rune behaviour smoke tests ─────────────────────────────────────
// These verify that the callback patterns used by QuerySubscription
// behave correctly under real $state proxying and flushSync — not
// the useAll hook itself, but the reactive primitives it relies on.

describe("$state callback patterns", () => {
  it("onfulfilled delivers data and clears loading", () => {
    let current: any[] | undefined = $state();
    let loading: boolean = $state(true);

    const onfulfilled = (data: any[]) => {
      current = data;
      loading = false;
    };

    onfulfilled([{ id: "1", title: "Alice's todo" }]);
    flushSync();

    expect($state.snapshot(current)).toEqual([{ id: "1", title: "Alice's todo" }]);
    expect(loading).toBe(false);
  });

  it("onDelta replaces current with delta.all", () => {
    let current: any[] | undefined = $state([{ id: "1", title: "First" }]);

    const onDelta = (delta: { all: any[] }) => {
      current = delta.all;
    };

    onDelta({
      all: [
        { id: "1", title: "First" },
        { id: "2", title: "Second" },
      ],
    });
    flushSync();

    expect($state.snapshot(current)).toHaveLength(2);
    expect($state.snapshot(current)![1].title).toBe("Second");
  });

  it("onError surfaces error and clears current", () => {
    let current: any[] | undefined = $state([{ id: "1" }]);
    let loading: boolean = $state(false);
    let error: Error | null = $state(null);

    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
      current = undefined;
      loading = false;
    };

    onError(new Error("subscription failed"));
    flushSync();

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("subscription failed");
    expect(current).toBeUndefined();
    expect(loading).toBe(false);
  });

  it("onError wraps non-Error values in Error", () => {
    let error: Error | null = $state(null);

    const onError = (e: unknown) => {
      error = e instanceof Error ? e : new Error(String(e));
    };

    onError("string error");
    flushSync();

    expect(error).toBeInstanceOf(Error);
    expect(error!.message).toBe("string error");
  });

  it("synchronous throw during setup is caught and surfaced", () => {
    let error: Error | null = $state(null);
    let loading: boolean = $state(true);

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

describe("initial value semantics", () => {
  it("with tier → undefined (awaiting settlement)", () => {
    const options = { tier: "edge" as const };
    const initial = options?.tier ? undefined : [];
    expect(initial).toBeUndefined();
  });

  it("without tier → empty array (locally available)", () => {
    const options = {};
    const initial = (options as any)?.tier ? undefined : [];
    expect(initial).toEqual([]);
  });

  it("without options → empty array", () => {
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

    let current: any[] | undefined = $state();
    let loading: boolean = $state(true);

    if (entry.state.status === "fulfilled") {
      current = entry.state.data;
      loading = false;
    }

    expect($state.snapshot(current)).toEqual([alice]);
    expect(loading).toBe(false);
  });
});
