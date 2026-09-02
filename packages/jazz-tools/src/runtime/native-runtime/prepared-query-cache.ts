const MAX_PREPARED_QUERY_CACHE_ENTRIES = 256;
const MAX_PREPARED_QUERY_CACHE_ENCODED_BYTES = 1_048_576;

type PreparedQueryCacheEntry<T extends object> = {
  readonly key: string | undefined;
  readonly query: T;
  readonly weight: number;
  readonly generation: number;
  leases: number;
};

export type PreparedQueryLease<T extends object> = {
  readonly query: T;
  retain(): PreparedQueryLease<T>;
  release(): void;
  isCurrent(): boolean;
};

/**
 * Runtime-local exact-query retention with active-use pinning and inactive LRU
 * admission. This module intentionally owns only adapter references; it does
 * not own or dispose the native query's underlying Groove shape.
 */
export class PreparedQueryCache<T extends object> {
  private readonly entries = new Map<string, PreparedQueryCacheEntry<T>>();
  private readonly inactive = new Map<string, PreparedQueryCacheEntry<T>>();
  private totalWeight = 0;
  private generation = 0;

  constructor(private readonly onEvict: (query: T) => void = () => {}) {}

  acquire(
    encodedQuery: Uint8Array,
    prepare: (bytes: Uint8Array) => T,
    kind?: "query" | "relation",
    cacheable?: boolean,
  ): PreparedQueryLease<T>;
  acquire(
    encodedQuery: Uint8Array,
    prepare: (bytes: Uint8Array) => T | Promise<T>,
    kind?: "query" | "relation",
    cacheable?: boolean,
  ): PreparedQueryLease<T> | Promise<PreparedQueryLease<T>>;
  acquire(
    encodedQuery: Uint8Array,
    prepare: (bytes: Uint8Array) => T | Promise<T>,
    kind: "query" | "relation" = "query",
    cacheable = true,
  ): PreparedQueryLease<T> | Promise<PreparedQueryLease<T>> {
    // Contextual preparation carries an admitted identity/claims snapshot and
    // must never be reused by another request, even with identical query bytes.
    const key = cacheable ? `${kind}:${bytesKey(encodedQuery)}` : undefined;
    const existing = key === undefined ? undefined : this.entries.get(key);
    if (existing) return this.leaseRetain(existing);

    const generation = this.generation;
    const remember = (query: T): PreparedQueryLease<T> => {
      if (generation !== this.generation) {
        throw new Error("prepared query acquisition was invalidated");
      }
      // Independent pending preparations may complete in either order. Only
      // one reusable entry owns this key; existing active leases stay pinned.
      const current = key === undefined ? undefined : this.entries.get(key);
      if (current) return this.leaseRetain(current);
      const entry: PreparedQueryCacheEntry<T> = {
        key,
        query,
        weight: encodedQuery.byteLength,
        generation,
        leases: 1,
      };
      if (key !== undefined) {
        this.entries.set(key, entry);
        this.totalWeight += entry.weight;
        this.evictToBudget();
      }
      return new CacheLease(this, entry);
    };
    const query = prepare(encodedQuery);
    return query instanceof Promise ? query.then(remember) : remember(query);
  }

  clear(): void {
    this.generation += 1;
    const removed = [...this.entries.values()];
    this.entries.clear();
    this.inactive.clear();
    this.totalWeight = 0;
    for (const entry of removed) this.notifyEviction(entry.query);
  }

  isCurrentEntry(entry: PreparedQueryCacheEntry<T>): boolean {
    return entry.generation === this.generation &&
      (entry.key === undefined || this.entries.get(entry.key) === entry);
  }

  retainEntry(entry: PreparedQueryCacheEntry<T>): void {
    if (!this.isCurrentEntry(entry)) throw new Error("prepared query lease is stale");
    if (entry.key !== undefined) this.inactive.delete(entry.key);
    entry.leases += 1;
  }

  releaseEntry(entry: PreparedQueryCacheEntry<T>): void {
    if (!this.isCurrentEntry(entry)) return;
    if (entry.leases <= 0) return;
    entry.leases -= 1;
    if (entry.leases === 0 && entry.key !== undefined) {
      this.inactive.delete(entry.key);
      this.inactive.set(entry.key, entry);
      this.evictToBudget();
    }
  }

  evictToBudget(): void {
    while (
      (this.entries.size > MAX_PREPARED_QUERY_CACHE_ENTRIES ||
        this.totalWeight > MAX_PREPARED_QUERY_CACHE_ENCODED_BYTES) &&
      this.inactive.size > 0
    ) {
      const oldestKey = this.inactive.keys().next().value as string | undefined;
      if (oldestKey === undefined) return;
      const entry = this.inactive.get(oldestKey);
      this.inactive.delete(oldestKey);
      if (!entry || this.entries.get(oldestKey) !== entry) continue;
      this.entries.delete(oldestKey);
      this.totalWeight -= entry.weight;
      this.notifyEviction(entry.query);
    }
  }

  leaseIsCurrent(entry: PreparedQueryCacheEntry<T>): boolean {
    return this.isCurrentEntry(entry);
  }

  leaseRetain(entry: PreparedQueryCacheEntry<T>): PreparedQueryLease<T> {
    this.retainEntry(entry);
    return new CacheLease(this, entry);
  }

  leaseRelease(entry: PreparedQueryCacheEntry<T>): void {
    this.releaseEntry(entry);
  }

  leaseQuery(entry: PreparedQueryCacheEntry<T>): T {
    return entry.query;
  }
  notifyEviction(query: T): void {
    try {
      this.onEvict(query);
    } catch {
      // Eviction bookkeeping is complete before cleanup is notified.
    }
  }
}

class CacheLease<T extends object> implements PreparedQueryLease<T> {
  private released = false;

  constructor(
    private readonly cache: PreparedQueryCache<T>,
    private readonly entry: PreparedQueryCacheEntry<T>,
  ) {}

  get query(): T {
    return this.cache.leaseQuery(this.entry);
  }

  retain(): PreparedQueryLease<T> {
    if (this.released) throw new Error("prepared query lease is already released");
    return this.cache.leaseRetain(this.entry);
  }

  release(): void {
    if (this.released) return;
    this.released = true;
    this.cache.leaseRelease(this.entry);
  }

  isCurrent(): boolean {
    return !this.released && this.cache.leaseIsCurrent(this.entry);
  }
}

function bytesKey(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
}
