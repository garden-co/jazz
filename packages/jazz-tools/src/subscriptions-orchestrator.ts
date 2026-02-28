import { SubscriptionManager, type SubscriptionDelta } from "./runtime/subscription-manager.js";
import type { QueryBuilder } from "./runtime/db.js";
import type { Session } from "./runtime/context.js";
import type { PersistenceTier } from "./runtime/client.js";

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
  tier?: PersistenceTier;
  snapshot?: T[];
}

interface InternalCacheEntry<T extends { id: string }> {
  key: string;
  query: QueryBuilder<T>;
  tier?: PersistenceTier;
  state: UseAllState<T>;
  promise: TrackedPromise<T[]>;
  resolvefulfilled: (data: T[]) => void;
  rejectfulfilled: (error: unknown) => void;
  listeners: Set<QueryEntryCallbacks<T>>;
  cleanupTimeoutId: ReturnType<typeof setTimeout> | null;
  unsubscribe?: () => void;
  subscriptionManager?: SubscriptionManager<T>;
  status: UseAllState<T>["status"];
  error: unknown;
  subscribe(callbacks: QueryEntryCallbacks<T>): () => void;
}

interface DbLike {
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    settledTier?: PersistenceTier,
    session?: Session,
  ): () => void;
}

export class SubscriptionsOrchestrator {
  private readonly cleanupDelayMs = 30_000;
  private readonly entries = new Map<string, InternalCacheEntry<any>>();
  private readonly queryDefinitions = new Map<string, QueryDefinition<any>>();

  constructor(
    private readonly config: { appId: string },
    private readonly db: DbLike,
    private readonly session?: Session | null,
  ) {}

  async init(): Promise<void> {}

  async shutdown(): Promise<void> {
    for (const entry of this.entries.values()) {
      this.destroyEntry(entry);
    }
    this.entries.clear();
    this.queryDefinitions.clear();
  }

  makeQueryKey<T extends { id: string }>(
    query: QueryBuilder<T>,
    tier?: PersistenceTier,
    snapshot?: T[],
  ): string {
    const key = `${this.config.appId}:${tier ?? "none"}:${query._build()}`;
    this.queryDefinitions.set(key, {
      query,
      tier,
      snapshot: snapshot ? [...snapshot] : undefined,
    });

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

  private ensureEntryForKey<T extends { id: string }>(key: string): InternalCacheEntry<T> {
    const existing = this.entries.get(key);
    if (existing) {
      return existing as InternalCacheEntry<T>;
    }

    const queryDef = this.queryDefinitions.get(key);
    if (!queryDef) {
      throw new Error(`Unknown query key "${key}". Call makeQueryKey(query, tier) first.`);
    }

    const hasSnapshot = queryDef.snapshot !== undefined;

    const deferred = makeDeferred<T[]>({
      status: hasSnapshot ? "fulfilled" : "pending",
      value: hasSnapshot ? queryDef.snapshot : undefined,
    });

    const initialState: UseAllState<T> = hasSnapshot
      ? { status: "fulfilled", data: queryDef.snapshot as T[], error: null }
      : { status: "pending", data: undefined, promise: deferred, error: null };

    const entry = {
      key,
      query: queryDef.query,
      tier: queryDef.tier,
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

    try {
      const subscriptionManager = new SubscriptionManager<T>();
      entry.subscriptionManager = subscriptionManager;

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
        entry.tier,
        this.session ?? undefined,
      );
    } catch (error) {
      entry.state = { status: "rejected", data: undefined, error };
      entry.rejectfulfilled(error);
      for (const listener of Array.from(entry.listeners)) {
        listener.onError?.(error);
      }
      this.scheduleCleanup(entry);
    }

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
    entry.subscriptionManager?.clear();
    entry.subscriptionManager = undefined;
    entry.listeners.clear();
    this.cancelCleanup(entry);
    this.entries.delete(entry.key);
    this.queryDefinitions.delete(entry.key);
  }
}
