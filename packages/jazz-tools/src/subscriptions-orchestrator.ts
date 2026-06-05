import type { SubscriptionDelta } from "./runtime/subscription-manager.js";
import type { QueryBuilder, QueryOptions } from "./runtime/db.js";
import type { Session } from "./runtime/context.js";

type UseAllStatePending<T> = {
  status: "pending";
  data: undefined;
  promise: TrackedPromise<T[]>;
  error: null;
};

type UseAllStatefulfilledData<T> = {
  status: "fulfilled";
  data: T[];
  error: null;
};

type UseAllStateError<T> = {
  status: "rejected";
  data: undefined;
  error: unknown;
};

export type UseAllState<T extends { id: string }> =
  | UseAllStatePending<T>
  | UseAllStatefulfilledData<T>
  | UseAllStateError<T>;

export type QueryEntryCallbacks<T extends { id: string }> = {
  onfulfilled?: (data: T[]) => void;
  onDelta?: (delta: SubscriptionDelta<T>) => void;
  onError?: (error: unknown) => void;
  /**
   * Fired when the entry is reset to `pending` underneath an active listener —
   * currently on a session change (see {@link SubscriptionsOrchestrator.setSession}).
   * The previous session's rows are no longer valid, so consumers should drop
   * back to a loading state and clear any cached data until the next delta.
   */
  onReset?: () => void;
};

export type CacheEntryHandle<T extends { id: string }> = {
  readonly state: UseAllState<T>;
  readonly status: UseAllState<T>["status"];
  readonly promise: TrackedPromise<T[]>;
  readonly error: unknown;
  subscribe(callbacks: QueryEntryCallbacks<T>): () => void;
};

export type TrackedStatus = "pending" | "fulfilled" | "rejected";

export type TrackedPromise<T> = Promise<T> & {
  status: TrackedStatus;
  value?: T;
  reason?: unknown;
};

export type TrackedDeferred<T> = TrackedPromise<T> & {
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
};

export function trackPromise<T>(source: Promise<T>): TrackedPromise<T> {
  const tracked = source as TrackedPromise<T>;

  if (
    tracked.status === "pending" ||
    tracked.status === "fulfilled" ||
    tracked.status === "rejected"
  ) {
    return tracked;
  }

  tracked.status = "pending";

  void source.then(
    (value) => {
      if (tracked.status !== "pending") return;
      tracked.status = "fulfilled";
      tracked.value = value;
    },
    (reason) => {
      if (tracked.status !== "pending") return;
      tracked.status = "rejected";
      tracked.reason = reason;
    },
  );

  return tracked;
}

export function makeDeferred<T>(snapshot?: {
  status: TrackedStatus;
  value?: T;
  reason?: unknown;
}): TrackedDeferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;

  const deferred = new Promise<T>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  }) as TrackedDeferred<T>;

  deferred.status = "pending";

  deferred.resolve = (value: T) => {
    if (deferred.status !== "pending") return;
    deferred.status = "fulfilled";
    deferred.value = value;
    resolve(value);
  };

  deferred.reject = (reason: unknown) => {
    if (deferred.status !== "pending") return;
    deferred.status = "rejected";
    deferred.reason = reason;
    reject(reason);
  };

  if (snapshot?.status === "fulfilled") {
    deferred.resolve(snapshot.value as T);
  } else if (snapshot?.status === "rejected") {
    deferred.reject(snapshot.reason);
  }

  return deferred;
}

interface QueryDefinition<T extends { id: string }> {
  query: QueryBuilder<T>;
  options?: QueryOptions;
  snapshot?: T[];
}

interface InternalCacheEntry<T extends { id: string }> {
  key: string;
  query: QueryBuilder<T>;
  options?: QueryOptions;
  state: UseAllState<T>;
  promise: TrackedPromise<T[]>;
  resolvefulfilled: (data: T[]) => void;
  rejectfulfilled: (error: unknown) => void;
  listeners: Set<QueryEntryCallbacks<T>>;
  cleanupTimeoutId: ReturnType<typeof setTimeout> | null;
  unsubscribe?: () => void;
  status: UseAllState<T>["status"];
  error: unknown;
  subscribe(callbacks: QueryEntryCallbacks<T>): () => void;
}

/**
 * Shared, identity-stable `pending` state returned by {@link
 * SubscriptionsOrchestrator.peekState} when a key has no entry and no seeded
 * snapshot. A single instance keeps `useSyncExternalStore` from looping on a
 * fresh object every render. The promise never resolves and is never awaited on
 * this path (the non-suspense reader ignores it).
 */
const SHARED_PENDING: UseAllStatePending<any> = {
  status: "pending",
  data: undefined,
  promise: makeDeferred<any>(),
  error: null,
};

interface DbLike {
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void;
}

export class SubscriptionsOrchestrator {
  private readonly cleanupDelayMs = 30_000;
  private readonly entries = new Map<string, InternalCacheEntry<any>>();
  private readonly queryDefinitions = new Map<string, QueryDefinition<any>>();
  // Memoised fulfilled states for seeded keys read via peekState before their
  // entry exists; keeps the snapshot identity stable for useSyncExternalStore.
  private readonly seededStates = new Map<string, UseAllState<any>>();
  private session?: Session | null;

  constructor(
    private readonly config: { appId: string },
    private readonly db: DbLike,
    session?: Session | null,
  ) {
    this.session = session;
  }

  async init(): Promise<void> {}

  setSession(session: Session | null): void {
    if (sessionsEqual(this.session ?? null, session)) {
      return;
    }

    this.session = session;

    for (const entry of this.entries.values()) {
      this.resubscribeEntry(entry);
    }
  }

  async shutdown(): Promise<void> {
    for (const entry of this.entries.values()) {
      this.destroyEntry(entry);
    }
    this.entries.clear();
    this.queryDefinitions.clear();
    this.seededStates.clear();
  }

  /**
   * Compute the cache key for a query without any side effects. Safe to call
   * during a React render (it neither registers the query nor subscribes). Use
   * {@link makeQueryKey} to register, and {@link getCacheEntry} to subscribe.
   */
  computeKey<T extends { id: string }>(query: QueryBuilder<T>, options?: QueryOptions): string {
    return `${this.config.appId}:${serializeQueryOptions(options)}:${query._build()}`;
  }

  makeQueryKey<T extends { id: string }>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
    snapshot?: T[],
  ): string {
    const key = this.computeKey(query, options);
    this.queryDefinitions.set(key, {
      query,
      options,
      snapshot: snapshot ? [...snapshot] : undefined,
    });
    // A re-seed invalidates any memoised pre-entry snapshot state.
    this.seededStates.delete(key);

    const existing = this.entries.get(key) as InternalCacheEntry<T> | undefined;
    if (existing && existing.state.status === "pending" && snapshot) {
      existing.state = { status: "fulfilled", data: snapshot, error: null };
      existing.resolvefulfilled(snapshot);
    }

    return key;
  }

  getCacheEntry<T extends { id: string }>(key: string): CacheEntryHandle<T> {
    return this.ensureEntryForKey<T>(key);
  }

  /**
   * Read the current state for a key without creating an entry or opening a
   * subscription. Render-safe: returns the live entry state if one exists, the
   * seeded snapshot's fulfilled state if the key was registered with one, or a
   * shared identity-stable `pending` state otherwise. Used by React's
   * `useSyncExternalStore` (`getSnapshot`/`getServerSnapshot`).
   */
  peekState<T extends { id: string }>(key: string): UseAllState<T> {
    const existing = this.entries.get(key);
    if (existing) {
      return existing.state as UseAllState<T>;
    }

    const cachedSeed = this.seededStates.get(key);
    if (cachedSeed) {
      return cachedSeed as UseAllState<T>;
    }

    const queryDef = this.queryDefinitions.get(key);
    if (queryDef?.snapshot !== undefined) {
      const seeded: UseAllState<T> = {
        status: "fulfilled",
        data: queryDef.snapshot as T[],
        error: null,
      };
      this.seededStates.set(key, seeded);
      return seeded;
    }

    return SHARED_PENDING as UseAllState<T>;
  }

  private ensureEntryForKey<T extends { id: string }>(key: string): InternalCacheEntry<T> {
    const existing = this.entries.get(key);
    if (existing) {
      return existing as InternalCacheEntry<T>;
    }

    const queryDef = this.queryDefinitions.get(key);
    if (!queryDef) {
      throw new Error(`Unknown query key "${key}". Call makeQueryKey(query, options) first.`);
    }

    const hasSnapshot = queryDef.snapshot !== undefined;

    const deferred = makeDeferred<T[]>({
      status: hasSnapshot ? "fulfilled" : "pending",
      value: hasSnapshot ? queryDef.snapshot : undefined,
    });
    // Callback-based consumers (non-suspense React, Svelte, Vue) never await
    // this promise, so a subscription failure would surface as an unhandled
    // rejection. Attach a no-op handler; the suspense reader still attaches its
    // own via `use()`.
    deferred.catch(() => {});

    const initialState: UseAllState<T> = hasSnapshot
      ? { status: "fulfilled", data: queryDef.snapshot as T[], error: null }
      : { status: "pending", data: undefined, promise: deferred, error: null };

    const entry = {
      key,
      query: queryDef.query,
      options: queryDef.options,
      state: initialState,
      promise: deferred,
      resolvefulfilled: (data) => {
        deferred.resolve(data);
      },
      rejectfulfilled: (error) => {
        deferred.reject(error);
      },
      listeners: new Set(),
      cleanupTimeoutId: null,
      subscribe: (callbacks) => {
        this.cancelCleanup(entry);
        entry.listeners.add(callbacks);

        if (entry.state.status === "rejected") {
          callbacks.onError?.(entry.state.error);
        }

        if (entry.state.status === "fulfilled") {
          callbacks.onfulfilled?.(entry.state.data);
        }

        return () => {
          if (!entry.listeners.delete(callbacks)) {
            return;
          }
          if (entry.listeners.size === 0) {
            this.scheduleCleanup(entry);
          }
        };
      },
      get status() {
        return entry.state.status;
      },
      get rejected() {
        return entry.state.status === "rejected" ? entry.state.error : null;
      },
      get error() {
        return entry.state.status === "rejected" ? entry.state.error : null;
      },
    } as InternalCacheEntry<T>;

    this.subscribeEntry(entry);

    this.entries.set(key, entry);
    return entry;
  }

  private scheduleCleanup(entry: InternalCacheEntry<any>): void {
    this.cancelCleanup(entry);
    entry.cleanupTimeoutId = setTimeout(() => {
      if (entry.listeners.size === 0) {
        this.destroyEntry(entry);
      }
    }, this.cleanupDelayMs);
  }

  private cancelCleanup(entry: InternalCacheEntry<any>): void {
    if (!entry.cleanupTimeoutId) return;
    clearTimeout(entry.cleanupTimeoutId);
    entry.cleanupTimeoutId = null;
  }

  private destroyEntry(entry: InternalCacheEntry<any>): void {
    if (entry.unsubscribe) {
      entry.unsubscribe();
    }
    entry.unsubscribe = undefined;
    entry.listeners.clear();
    this.cancelCleanup(entry);
    this.entries.delete(entry.key);
    this.queryDefinitions.delete(entry.key);
    // Drop the memoised pre-entry snapshot too: leaving it behind would make a
    // later `peekState` return stale `fulfilled` data for a key whose definition
    // no longer exists (and which `getCacheEntry` would now reject as unknown).
    this.seededStates.delete(entry.key);
  }

  private subscribeEntry<T extends { id: string }>(entry: InternalCacheEntry<T>): void {
    try {
      entry.unsubscribe = this.db.subscribeAll<T>(
        entry.query,
        (delta) => {
          const wasPending = entry.state.status === "pending";
          entry.state = {
            status: "fulfilled",
            data: delta.all,
            error: null,
          };

          if (wasPending) {
            entry.resolvefulfilled(delta.all);
          }

          for (const listener of Array.from(entry.listeners)) {
            if (wasPending) {
              listener.onfulfilled?.(delta.all);
            } else {
              listener.onDelta?.(delta);
            }
          }

          if (entry.listeners.size === 0) {
            this.scheduleCleanup(entry);
          }
        },
        entry.options,
        this.session ?? undefined,
      );
    } catch (error) {
      // Only a synchronous setup (protocol-level) failure from `subscribeAll`
      // lands here and drives the entry to `rejected`. Data-level errors inside
      // an established subscription flow through the subscription's own on-error
      // channel and do not reject the entry; that separation is intentional.
      entry.state = { status: "rejected", data: undefined, error };
      entry.rejectfulfilled(error);
      for (const listener of Array.from(entry.listeners)) {
        listener.onError?.(error);
      }
      this.scheduleCleanup(entry);
    }
  }

  /**
   * Reset an entry to a fresh `pending` state with a new deferred, so a later
   * suspense read awaits the reload rather than the stale resolved promise.
   * Used by session-change resubscription.
   */
  private resetEntryToPending<T extends { id: string }>(entry: InternalCacheEntry<T>): void {
    const next = makeDeferred<T[]>();
    next.catch(() => {});
    entry.promise = next;
    entry.resolvefulfilled = (data) => {
      next.resolve(data);
    };
    entry.rejectfulfilled = (error) => {
      next.reject(error);
    };
    entry.state = { status: "pending", data: undefined, promise: next, error: null };
  }

  private resubscribeEntry<T extends { id: string }>(entry: InternalCacheEntry<T>): void {
    if (entry.unsubscribe) {
      entry.unsubscribe();
      entry.unsubscribe = undefined;
    }

    // The prior session's rows are no longer valid. Drop a settled entry back to
    // `pending` and tell listeners to clear, so stale data is nuked with the
    // session instead of lingering until the new subscription's first delta. A
    // still-`pending` entry is left as-is — its in-flight promise may already be
    // awaited by a suspense reader.
    if (entry.state.status !== "pending") {
      this.resetEntryToPending(entry);
      for (const listener of Array.from(entry.listeners)) {
        listener.onReset?.();
      }
    }

    this.subscribeEntry(entry);
  }
}

function sessionsEqual(a: Session | null, b: Session | null): boolean {
  if (a === b) {
    return true;
  }

  return JSON.stringify(a) === JSON.stringify(b);
}

function serializeQueryOptions(options?: QueryOptions): string {
  if (!options) {
    return "{}";
  }

  return JSON.stringify(options);
}
