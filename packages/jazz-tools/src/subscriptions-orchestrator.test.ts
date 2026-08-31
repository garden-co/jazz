import { afterEach, describe, expect, it, vi } from "vitest";
import type { Session } from "./runtime/context.js";
import type {
  DbDeltaSubscriptionCallbacks,
  QueryBuilder,
  QueryOptions,
  SubscriptionHandle,
} from "./runtime/db.js";
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
  onDelta: (delta: SubscriptionDelta<any>) => void;
  onError?: (error: Error) => void;
  query: QueryBuilder<any>;
  options?: QueryOptions;
  session?: Session;
  unsubscribe: SubscriptionHandle;
};

type UnitHarness = {
  manager: SubscriptionsOrchestrator;
  makeEntry: () => {
    key: string;
    entry: CacheEntryHandle<Todo>;
  };
  calls: SubscribeCall[];
  emit: (index: number, delta: SubscriptionDelta<Todo>) => void;
  emitError: (index: number, error: Error) => void;
  setThrowOnSubscribe: (error: Error | null) => void;
  setNextReadiness: (readiness: Promise<void> | null) => void;
  setErrorOnSubscribe: (error: Error | null) => void;
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
  let nextReadiness: Promise<void> | null = null;
  let errorOnSubscribe: Error | null = null;

  const db: {
    subscribeDelta<T extends { id: string }>(
      query: QueryBuilder<T>,
      callbacks: DbDeltaSubscriptionCallbacks<T>,
      options?: QueryOptions,
      session?: Session,
    ): SubscriptionHandle;
  } = {
    subscribeDelta<T extends { id: string }>(
      query: QueryBuilder<T>,
      callbacks: DbDeltaSubscriptionCallbacks<T>,
      options?: QueryOptions,
      session?: Session,
    ): SubscriptionHandle {
      if (throwOnSubscribe) {
        throw throwOnSubscribe;
      }
      const unsubscribe = vi.fn() as SubscriptionHandle;
      if (nextReadiness) {
        Object.defineProperty(unsubscribe, "ready", { value: nextReadiness });
        nextReadiness = null;
      }
      calls.push({
        onDelta: callbacks.onDelta as (delta: SubscriptionDelta<any>) => void,
        onError: callbacks.onError,
        query: query as QueryBuilder<any>,
        options,
        session,
        unsubscribe,
      });
      if (errorOnSubscribe) {
        callbacks.onError?.(errorOnSubscribe);
      }
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
      (call.onDelta as (payload: SubscriptionDelta<Todo>) => void)(delta);
    },
    emitError(index, error) {
      const call = calls[index];
      if (!call) {
        throw new Error(`No subscription call at index ${index}`);
      }
      call.onError?.(error);
    },
    setThrowOnSubscribe(error) {
      throwOnSubscribe = error;
    },
    setNextReadiness(readiness) {
      nextReadiness = readiness;
    },
    setErrorOnSubscribe(error) {
      errorOnSubscribe = error;
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

  it("SO-U07 makeQueryKey includes appId, public options, and query payload", async () => {
    const harness = createUnitHarness("app-so-u07");
    const query = makeQuery({
      table: "todos",
      conditions: [{ column: "done", op: "eq", value: false }],
    });

    try {
      const key = harness.manager.makeQueryKey(query, { tier: "edge" });
      expect(key).toBe(
        `app-so-u07:${JSON.stringify({
          tier: "edge",
        })}:${query._build()}`,
      );
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U07c makeQueryKey changes when the public read tier changes", async () => {
    const harness = createUnitHarness("app-so-u07c");
    const query = makeQuery();

    try {
      const defaultKey = harness.manager.makeQueryKey(query);
      const remoteKey = harness.manager.makeQueryKey(query, { tier: "remote" });
      const localFirstKey = harness.manager.makeQueryKey(query, { tier: "local-first" });

      expect(remoteKey).not.toBe(defaultKey);
      expect(localFirstKey).not.toBe(defaultKey);
      expect(localFirstKey).not.toBe(remoteKey);
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

  it("SO-U09b getCacheEntry forwards public QueryOptions to the delta source", async () => {
    const harness = createUnitHarness();
    try {
      const options = {
        tier: "remote",
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
    const setupError = new Error("delta subscription failed");
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

  it("SO-U14a asynchronous subscription admission failure marks entry rejected", async () => {
    const harness = createUnitHarness();
    const admissionError = new Error("browser storage open failed");
    harness.setNextReadiness(Promise.reject(admissionError));

    try {
      const { entry } = harness.makeEntry();
      const onError = vi.fn();
      entry.subscribe({ onError });

      await expect(entry.promise).rejects.toBe(admissionError);
      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: admissionError,
      });
      expect(onError).toHaveBeenCalledWith(admissionError);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U14b preserves a synchronous Db opening failure as the entry outcome", async () => {
    const harness = createUnitHarness();
    const openingError = new Error("native opening failed");
    harness.setErrorOnSubscribe(openingError);

    try {
      const { entry } = harness.makeEntry();
      const onError = vi.fn();
      const onDelta = vi.fn();
      entry.subscribe({ onError, onDelta });

      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: openingError,
      });
      expect(onError).toHaveBeenCalledOnce();
      expect(onError).toHaveBeenCalledWith(openingError);
      await expect(entry.promise).rejects.toBe(openingError);

      harness.emit(0, makeDelta([makeTodo("late", "late opening delta")]));
      harness.emitError(0, new Error("late opening error"));
      expect(onDelta).not.toHaveBeenCalled();
      expect(onError).toHaveBeenCalledOnce();
      expect(entry.error).toBe(openingError);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U14c preserves a deferred-start failure as the pending generation outcome", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      const onError = vi.fn();
      entry.subscribe({ onError });
      expect(entry.status).toBe("pending");

      const readinessError = new Error("deferred subscription readiness failed");
      harness.emitError(0, readinessError);

      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: readinessError,
      });
      expect(onError).toHaveBeenCalledOnce();
      await expect(entry.promise).rejects.toBe(readinessError);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U14d keeps the first terminal error when onError races readiness rejection", async () => {
    const harness = createUnitHarness();
    let rejectReadiness!: (error: Error) => void;
    const readiness = new Promise<void>((_resolve, reject) => {
      rejectReadiness = reject;
    });
    harness.setNextReadiness(readiness);
    try {
      const { entry } = harness.makeEntry();
      const errors: Error[] = [];
      entry.subscribe({ onError: (error) => errors.push(error as Error) });

      const streamError = new Error("stream failed during admission");
      harness.emitError(0, streamError);
      rejectReadiness(new Error("admission rejected after stream failure"));
      await Promise.resolve();

      expect(errors).toEqual([streamError]);
      expect(entry.error).toBe(streamError);
      await expect(entry.promise).rejects.toBe(streamError);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U15 replacement failure rejects the fulfilled entry and remains terminal", async () => {
    const harness = createUnitHarness();
    try {
      const { entry } = harness.makeEntry();
      const onError = vi.fn();
      const onDelta = vi.fn();
      entry.subscribe({ onError, onDelta });
      harness.emit(0, makeDelta([makeTodo("1")]));

      const streamError = new Error("subscription stream failed");
      harness.emitError(0, streamError);

      expect(onError).toHaveBeenCalledOnce();
      expect(onError).toHaveBeenCalledWith(streamError);
      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: streamError,
      });

      harness.emit(0, makeDelta([makeTodo("2", "must stay terminal")]));
      harness.emitError(0, new Error("late duplicate stream failure"));
      expect(onDelta).not.toHaveBeenCalled();
      expect(onError).toHaveBeenCalledOnce();
      expect(entry.state).toEqual({
        status: "rejected",
        data: undefined,
        error: streamError,
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
      issuer: "https://issuer.example",
      authMode: "external",
    };
    const nextSession: Session = {
      user_id: "alice",
      claims: { role: "writer" },
      issuer: "https://issuer.example",
      authMode: "external",
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

  it("SO-U22 setSession skips resubscribe work when the session is unchanged", async () => {
    const session: Session = {
      user_id: "alice",
      claims: { role: "reader" },
      issuer: "https://issuer.example",
      authMode: "external",
    };
    const harness = createUnitHarness("orchestrator-unit-same-session", session);

    try {
      harness.makeEntry();

      expect(harness.calls).toHaveLength(1);

      harness.manager.setSession({
        user_id: "alice",
        claims: { role: "reader" },
        issuer: "https://issuer.example",
        authMode: "external",
      });

      expect(harness.calls).toHaveLength(1);
      expect(harness.calls[0]?.unsubscribe).not.toHaveBeenCalled();
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U25 computeKey is pure and does not register the query definition", async () => {
    const harness = createUnitHarness("app-so-u25");
    try {
      const query = makeQuery();
      const key = harness.manager.computeKey(query);

      expect(key).toBe(harness.manager.makeQueryKey(query));
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U25b computeKey alone leaves the key unregistered", async () => {
    const harness = createUnitHarness("app-so-u25b");
    try {
      const key = harness.manager.computeKey(makeQuery());
      expect(() => harness.manager.getCacheEntry<Todo>(key)).toThrow(/Unknown query key/);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U26 peekState reads state without opening a subscription", async () => {
    const harness = createUnitHarness("app-so-u26");
    try {
      const key = harness.manager.makeQueryKey(makeQuery());

      const first = harness.manager.peekState<Todo>(key);
      const second = harness.manager.peekState<Todo>(key);
      expect(first.status).toBe("pending");
      expect(first).toBe(second);
      expect(harness.calls).toHaveLength(0);

      const entry = harness.manager.getCacheEntry<Todo>(key);
      harness.emit(0, makeDelta([makeTodo("1")]));
      expect(harness.manager.peekState<Todo>(key)).toBe(entry.state);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U27 peekState returns a stable fulfilled snapshot when seeded", async () => {
    const harness = createUnitHarness("app-so-u27");
    try {
      const snapshot = [makeTodo("1", "seed")];
      const key = harness.manager.makeQueryKey(makeQuery(), undefined, snapshot);

      const first = harness.manager.peekState<Todo>(key);
      const second = harness.manager.peekState<Todo>(key);
      expect(first.status).toBe("fulfilled");
      expect(first.status === "fulfilled" ? first.data : undefined).toEqual(snapshot);
      expect(first).toBe(second);
      expect(harness.calls).toHaveLength(0);
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U28 destroying a seeded entry drops its memoised peekState snapshot", async () => {
    vi.useFakeTimers();
    const harness = createUnitHarness("app-so-u28");
    try {
      const snapshot = [makeTodo("1", "seed")];
      const key = harness.manager.makeQueryKey(makeQuery(), undefined, snapshot);
      expect(harness.manager.peekState<Todo>(key).status).toBe("fulfilled");

      const entry = harness.manager.getCacheEntry<Todo>(key);
      const unsubscribe = entry.subscribe({});
      unsubscribe();
      vi.advanceTimersByTime(30_000);

      expect((harness.manager as any).entries.has(key)).toBe(false);
      expect(harness.manager.peekState<Todo>(key).status).toBe("pending");
    } finally {
      await harness.manager.shutdown();
    }
  });

  it("SO-U29 a session change clears cached rows and reloads from the new session", async () => {
    const sessionA: Session = {
      issuer: "https://issuer.example",
      user_id: "a",
      claims: { role: "reader" },
      authMode: "external",
    };
    const sessionB: Session = {
      issuer: "https://issuer.example",
      user_id: "b",
      claims: { role: "reader" },
      authMode: "external",
    };
    const harness = createUnitHarness("app-so-u29", sessionA);

    try {
      const { entry } = harness.makeEntry();
      const onfulfilled = vi.fn();
      const onReset = vi.fn();
      const onError = vi.fn();
      entry.subscribe({ onfulfilled, onReset, onError });

      harness.emit(0, makeDelta([makeTodo("1", "from-A")]));
      expect(entry.status).toBe("fulfilled");
      onfulfilled.mockClear();

      harness.manager.setSession(sessionB);

      expect(entry.status).toBe("pending");
      expect(onReset).toHaveBeenCalledTimes(1);
      expect(harness.calls).toHaveLength(2);
      expect(harness.calls[0]?.unsubscribe).toHaveBeenCalledOnce();

      harness.emit(0, makeDelta([makeTodo("retired", "retired session")]));
      harness.emitError(0, new Error("retired session failed late"));
      expect(entry.status).toBe("pending");
      expect(onfulfilled).not.toHaveBeenCalled();
      expect(onError).not.toHaveBeenCalled();

      harness.emit(1, makeDelta([makeTodo("2", "from-B")]));
      expect(entry.status).toBe("fulfilled");
      expect(onfulfilled).toHaveBeenCalledWith([makeTodo("2", "from-B")]);
    } finally {
      await harness.manager.shutdown();
    }
  });
});
