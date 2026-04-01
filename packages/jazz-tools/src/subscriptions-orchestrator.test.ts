import { afterEach, describe, expect, it, vi } from "vitest";
import type { Session } from "./runtime/context.js";
import type { QueryBuilder, QueryOptions } from "./runtime/db.js";
import type { SubscriptionDelta } from "./runtime/subscription-manager.js";
import {
  SubscriptionsOrchestrator,
  makeDeferred,
  trackPromise,
  type CacheEntryHandle,
} from "./subscriptions-orchestrator.js";

type Todo = {
  id: string;
  title: string;
};

type SubscribeCall = {
  callback: (delta: SubscriptionDelta<any>) => void;
  query: QueryBuilder<any>;
  options?: QueryOptions;
  session?: Session;
  unsubscribe: ReturnType<typeof vi.fn>;
};

type UnitHarness = {
  manager: SubscriptionsOrchestrator;
  makeEntry: () => {
    key: string;
    entry: CacheEntryHandle<Todo>;
  };
  calls: SubscribeCall[];
  emit: (index: number, delta: SubscriptionDelta<Todo>) => void;
  setThrowOnSubscribe: (error: Error | null) => void;
};

function makeTodo(id: string, title = `todo-${id}`): Todo {
  return { id, title };
}

function makeQuery(payload?: Record<string, unknown>): QueryBuilder<Todo> {
  const builtPayload = payload ?? {
    table: "todos",
    conditions: [],
    includes: {},
    orderBy: [],
  };

  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify(builtPayload);
    },
  };
}

function makeDelta(all: Todo[]): SubscriptionDelta<Todo> {
  return {
    all,
    delta: [],
  };
}

function createUnitHarness(
  appId = "orchestrator-unit",
  initialSession?: Session | null,
): UnitHarness {
  const calls: SubscribeCall[] = [];
  let throwOnSubscribe: Error | null = null;

  const db: {
    subscribeAll<T extends { id: string }>(
      query: QueryBuilder<T>,
      callback: (delta: SubscriptionDelta<T>) => void,
      options?: QueryOptions,
      session?: Session,
    ): () => void;
  } = {
    subscribeAll<T extends { id: string }>(
      query: QueryBuilder<T>,
      callback: (delta: SubscriptionDelta<T>) => void,
      options?: QueryOptions,
      session?: Session,
    ): () => void {
      if (throwOnSubscribe) {
        throw throwOnSubscribe;
      }
      const unsubscribe = vi.fn();
      calls.push({
        callback: callback as (delta: SubscriptionDelta<any>) => void,
        query: query as QueryBuilder<any>,
        options,
        session,
        unsubscribe,
      });
      return unsubscribe;
    },
  };

  const manager = new SubscriptionsOrchestrator({ appId }, db, initialSession);

  return {
    manager,
    makeEntry() {
      const key = manager.makeQueryKey(makeQuery());
      const entry = manager.getCacheEntry<Todo>(key);
      return { key, entry };
    },
    calls,
    emit(index, delta) {
      const call = calls[index];
      if (!call) {
        throw new Error(`No subscription call at index ${index}`);
      }
      (call.callback as (payload: SubscriptionDelta<Todo>) => void)(delta);
    },
    setThrowOnSubscribe(error) {
      throwOnSubscribe = error;
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("SubscriptionsOrchestrator unit coverage", () => {
  it("SO-U01 trackPromise starts pending and transitions to fulfilled", async () => {
    let resolve!: (value: number) => void;
    const source = new Promise<number>((innerResolve) => {
      resolve = innerResolve;
    });

    const tracked = trackPromise(source);
    expect(tracked.status).toBe("pending");

    resolve(123);

    await expect(tracked).resolves.toBe(123);
    expect(tracked.status).toBe("fulfilled");
    expect(tracked.value).toBe(123);
  });

  it("SO-U02 trackPromise transitions to rejected and records reason", async () => {
    const reason = new Error("expected failure");
    let reject!: (error: Error) => void;
    const source = new Promise<number>((_resolve, innerReject) => {
      reject = innerReject;
    });

    const tracked = trackPromise(source);
    expect(tracked.status).toBe("pending");

    reject(reason);

    await expect(tracked).rejects.toBe(reason);
    expect(tracked.status).toBe("rejected");
    expect(tracked.reason).toBe(reason);
  });

  it("SO-U03 trackPromise is idempotent for previously tracked promises", async () => {
    const tracked = trackPromise(Promise.resolve(77));
    const trackedAgain = trackPromise(tracked);

    expect(trackedAgain).toBe(tracked);
    await expect(trackedAgain).resolves.toBe(77);
  });

  it("SO-U04 makeDeferred without snapshot starts pending", () => {
    const deferred = makeDeferred<number>();
    expect(deferred.status).toBe("pending");
    expect(deferred.value).toBeUndefined();
    expect(deferred.reason).toBeUndefined();
  });

  it("SO-U05 makeDeferred with fulfilled snapshot resolves immediately", async () => {
    const deferred = makeDeferred<number>({ status: "fulfilled", value: 42 });

    expect(deferred.status).toBe("fulfilled");
    expect(deferred.value).toBe(42);
    await expect(deferred).resolves.toBe(42);
  });

  it("SO-U06 makeDeferred with rejected snapshot rejects immediately", async () => {
    const reason = new Error("snapshot error");
    const deferred = makeDeferred<number>({ status: "rejected", reason });

    expect(deferred.status).toBe("rejected");
    expect(deferred.reason).toBe(reason);
    await expect(deferred).rejects.toBe(reason);
  });

  it("SO-U07 makeQueryKey includes appId, normalized options, and query payload", async () => {
    const harness = createUnitHarness("app-so-u07");
    const query = makeQuery({
      table: "todos",
      conditions: [{ column: "done", op: "eq", value: false }],
    });

    try {
      const key = harness.manager.makeQueryKey(query, {
        tier: "edge",
        localUpdates: "deferred",
        propagation: "local-only",
      });
      expect(key).toBe(
        `app-so-u07:${JSON.stringify({
          tier: "edge",
          localUpdates: "deferred",
          propagation: "local-only",
        })}:${query._build()}`,
      );
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U07c makeQueryKey changes when localUpdates or propagation changes", async () => {
    const harness = createUnitHarness("app-so-u07c");
    const query = makeQuery();

    try {
      const defaultKey = harness.manager.makeQueryKey(query);
      const deferredKey = harness.manager.makeQueryKey(query, { localUpdates: "deferred" });
      const localOnlyKey = harness.manager.makeQueryKey(query, { propagation: "local-only" });

      expect(deferredKey).not.toBe(defaultKey);
      expect(localOnlyKey).not.toBe(defaultKey);
      expect(localOnlyKey).not.toBe(deferredKey);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U08 getCacheEntry throws for unknown query key", async () => {
    const harness = createUnitHarness();
    try {
      expect(() => harness.manager.getCacheEntry<Todo>("missing-key")).toThrow(
        'Unknown query key "missing-key". Call makeQueryKey(query, options) first.',
      );
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U09 getCacheEntry returns stable identity for same key", async () => {
    const harness = createUnitHarness();
    try {
      const key = harness.manager.makeQueryKey(makeQuery(), {});
      const first = harness.manager.getCacheEntry<Todo>(key);
      const second = harness.manager.getCacheEntry<Todo>(
        harness.manager.makeQueryKey(makeQuery(), {}),
      );

      expect(second).toBe(first);
      expect(harness.calls).toHaveLength(1);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U09b getCacheEntry forwards full QueryOptions to subscribeAll", async () => {
    const harness = createUnitHarness();
    try {
      const options = {
        tier: "global",
        localUpdates: "deferred",
        propagation: "local-only",
      } satisfies QueryOptions;
      const key = harness.manager.makeQueryKey(makeQuery(), options);

      harness.manager.getCacheEntry<Todo>(key);

      expect(harness.calls).toHaveLength(1);
      expect(harness.calls[0]?.options).toEqual(options);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U10 first delta transitions entry from pending to fulfilled", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      expect(entry.status).toBe("pending");

      harness.emit(0, makeDelta([makeTodo("1")]));

      expect(entry.status).toBe("fulfilled");
      expect(entry.state).toEqual({
        status: "fulfilled",
        data: [makeTodo("1")],
        error: null,
      });
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U11 first delta resolves entry.promise exactly once", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      const firstSnapshot = [makeTodo("1", "first")];
      const secondSnapshot = [makeTodo("1", "first"), makeTodo("2", "second")];

      const firstResolution = entry.promise;
      harness.emit(0, makeDelta(firstSnapshot));
      harness.emit(0, makeDelta(secondSnapshot));

      await expect(firstResolution).resolves.toEqual(firstSnapshot);
      expect(entry.state).toEqual({
        status: "fulfilled",
        data: secondSnapshot,
        error: null,
      });
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U12 first delta emits onfulfilled and not onDelta", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      const onfulfilled = vi.fn();
      const onDelta = vi.fn();
      const unsubscribe = entry.subscribe({ onfulfilled, onDelta });

      const firstSnapshot = [makeTodo("1", "first")];
      harness.emit(0, makeDelta(firstSnapshot));

      expect(onfulfilled).toHaveBeenCalledTimes(1);
      expect(onfulfilled).toHaveBeenCalledWith(firstSnapshot);
      expect(onDelta).not.toHaveBeenCalled();

      unsubscribe();
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U13 subsequent deltas emit onDelta and not onfulfilled", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      const onfulfilled = vi.fn();
      const onDelta = vi.fn();
      const unsubscribe = entry.subscribe({ onfulfilled, onDelta });

      const firstSnapshot = [makeTodo("1", "first")];
      const secondDelta = makeDelta([makeTodo("1", "first"), makeTodo("2", "second")]);

      harness.emit(0, makeDelta(firstSnapshot));
      onfulfilled.mockClear();
      onDelta.mockClear();

      harness.emit(0, secondDelta);

      expect(onfulfilled).not.toHaveBeenCalled();
      expect(onDelta).toHaveBeenCalledTimes(1);
      expect(onDelta).toHaveBeenCalledWith(secondDelta);

      unsubscribe();
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U14 subscribe setup exception marks entry rejected and emits onError", async () => {
    const harness = createUnitHarness();
    const setupError = new Error("subscribeAll failed");
    harness.setThrowOnSubscribe(setupError);

    try {
      const key = harness.manager.makeQueryKey(makeQuery());
      const entry = harness.manager.getCacheEntry<Todo>(key);
      const onError = vi.fn();

      expect(entry.status).toBe("rejected");
      entry.subscribe({ onError });

      expect(onError).toHaveBeenCalledTimes(1);
      expect(onError).toHaveBeenCalledWith(setupError);
      await expect(entry.promise).rejects.toBe(setupError);
      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: setupError,
      });
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U15 unsubscribing last listener schedules cleanup timeout", async () => {
    vi.useFakeTimers();
    const harness = createUnitHarness();
    try {
      const { key, entry } = harness.makeEntry();
      const unsubscribe = entry.subscribe({});

      unsubscribe();

      const internal = (harness.manager as any).entries.get(key);
      expect(internal.cleanupTimeoutId).not.toBeNull();
      expect(harness.calls[0]?.unsubscribe).not.toHaveBeenCalled();
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U16 resubscribe before timeout cancels cleanup", async () => {
    vi.useFakeTimers();
    const harness = createUnitHarness();
    try {
      const { key, entry } = harness.makeEntry();
      const unsubscribeA = entry.subscribe({});
      unsubscribeA();

      const internal = (harness.manager as any).entries.get(key);
      expect(internal.cleanupTimeoutId).not.toBeNull();

      const unsubscribeB = entry.subscribe({});
      expect(internal.cleanupTimeoutId).toBeNull();

      vi.advanceTimersByTime(30_000);
      expect((harness.manager as any).entries.has(key)).toBe(true);

      unsubscribeB();
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U17 cleanup timeout destroys entry, clears listeners, and unsubscribes", async () => {
    vi.useFakeTimers();
    const harness = createUnitHarness();
    try {
      const { key, entry } = harness.makeEntry();
      const unsubscribe = entry.subscribe({});
      const internal = (harness.manager as any).entries.get(key);

      unsubscribe();
      vi.advanceTimersByTime(30_000);

      expect(harness.calls[0]?.unsubscribe).toHaveBeenCalledTimes(1);
      expect((harness.manager as any).entries.has(key)).toBe(false);
      expect((harness.manager as any).queryDefinitions.has(key)).toBe(false);
      expect(internal.listeners.size).toBe(0);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U18 shutdown tears down entries and clears definition maps", async () => {
    const harness = createUnitHarness();
    const firstKey = harness.manager.makeQueryKey(makeQuery({ table: "todos", marker: "first" }));
    const secondKey = harness.manager.makeQueryKey(makeQuery({ table: "todos", marker: "second" }));
    const firstEntry = harness.manager.getCacheEntry<Todo>(firstKey);
    const secondEntry = harness.manager.getCacheEntry<Todo>(secondKey);
    const offA = firstEntry.subscribe({});
    const offB = secondEntry.subscribe({});

    await harness.manager.shutdown();

    expect(harness.calls[0]?.unsubscribe).toHaveBeenCalledTimes(1);
    expect(harness.calls[1]?.unsubscribe).toHaveBeenCalledTimes(1);
    expect((harness.manager as any).entries.size).toBe(0);
    expect((harness.manager as any).queryDefinitions.size).toBe(0);

    offA();
    offB();
  });

  it("SO-U19 makeQueryKey with snapshot hydrates existing pending entry", async () => {
    const harness = createUnitHarness();
    try {
      const query = makeQuery();
      const key = harness.manager.makeQueryKey(query);
      const entry = harness.manager.getCacheEntry<Todo>(key);
      expect(entry.status).toBe("pending");

      const snapshot = [makeTodo("1", "from-snapshot")];
      const hydratedKey = harness.manager.makeQueryKey(query, undefined, snapshot);

      expect(hydratedKey).toBe(key);
      expect(entry.status).toBe("fulfilled");
      expect(entry.state).toEqual({
        status: "fulfilled",
        data: snapshot,
        error: null,
      });
      await expect(entry.promise).resolves.toEqual(snapshot);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U20 listener unsubscribe is idempotent", async () => {
    vi.useFakeTimers();
    const harness = createUnitHarness();
    try {
      const { key, entry } = harness.makeEntry();
      const unsubscribe = entry.subscribe({});

      unsubscribe();
      unsubscribe();

      const internal = (harness.manager as any).entries.get(key);
      expect(internal.listeners.size).toBe(0);
      expect(internal.cleanupTimeoutId).not.toBeNull();

      vi.advanceTimersByTime(30_000);

      expect(harness.calls[0]?.unsubscribe).toHaveBeenCalledTimes(1);
      expect((harness.manager as any).entries.has(key)).toBe(false);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U21 setSession resubscribes active entries with the latest session", async () => {
    const initialSession: Session = {
      user_id: "alice",
      claims: { role: "reader" },
    };
    const nextSession: Session = {
      user_id: "alice",
      claims: { role: "writer" },
    };
    const harness = createUnitHarness("orchestrator-unit-session", initialSession);

    try {
      harness.makeEntry();

      expect(harness.calls).toHaveLength(1);
      expect(harness.calls[0]?.session).toEqual(initialSession);

      harness.manager.setSession(nextSession);

      expect(harness.calls).toHaveLength(2);
      expect(harness.calls[0]?.unsubscribe).toHaveBeenCalledTimes(1);
      expect(harness.calls[1]?.session).toEqual(nextSession);
    } finally {
      await harness.manager.shutdown();
    }
  });
});
