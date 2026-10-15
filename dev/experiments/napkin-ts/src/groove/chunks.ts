// Host-provided immutable chunk retrieval for interruptible evaluation.
import { type Bytes, concatBytes, equalBytes, toHex } from "../support/bytes.js";
import { uuidV4 } from "../support/hash.js";
import {
  type ContentHash,
  type Locator,
  MAX_ENCODED_NODE_BYTES,
  type NodeRef,
  type StagedChunk,
  type StagedLargeValueAccounting,
  objectHash,
} from "./large_values.js";
import { LARGE_VALUE_METADATA_CF, type LayoutStorage } from "../_extern/groove-records.js";

/** Opaque retrieval identity paired with the hash Groove must verify. */
export type ChunkRequest = { objectHash: Bytes; locator: Locator };
export const chunkRequestKey = (r: ChunkRequest) => toHex(r.objectHash) + toHex(r.locator);

export class ChunkStorageError extends Error {
  constructor(
    readonly kind: "Unavailable" | "LocatorConflict" | "Integrity" | "Backend",
    message?: string,
  ) {
    super(
      kind === "Unavailable"
        ? "chunk is unavailable"
        : kind === "LocatorConflict"
          ? "opaque locator already names different content"
          : kind === "Integrity"
            ? "staged chunk failed integrity validation"
            : `chunk storage failed: ${message}`,
    );
  }
}

export class ChunkError extends Error {
  constructor(
    readonly kind: "Unavailable" | "Retryable" | "Backend" | "Integrity" | "Reentrant",
    readonly detail?: string | number,
  ) {
    super(
      kind === "Unavailable"
        ? "chunk is unavailable"
        : kind === "Retryable"
          ? `chunk retrieval is retryable after ${detail}ms`
          : kind === "Backend"
            ? `chunk retrieval failed: ${detail}`
            : kind === "Integrity"
              ? "chunk bytes do not match the requested object hash"
              : "chunk request re-entered while its backing request was being polled",
    );
  }
  static fromStorage(e: unknown): ChunkError {
    if (!(e instanceof ChunkStorageError)) return e as ChunkError;
    if (e.kind === "Unavailable") return new ChunkError("Unavailable");
    if (e.kind === "Integrity") return new ChunkError("Integrity");
    return new ChunkError("Backend", e.message);
  }
}

const tooLargeOrMismatch = (bytes: Bytes, expected: ContentHash) =>
  bytes.length > MAX_ENCODED_NODE_BYTES || !equalBytes(objectHash(bytes), expected);

export interface ChunkKvStorage {
  getExact(locator: Locator): Promise<[ContentHash, Bytes] | null>;
  /** Install one immutable mapping or return the mapping already present. */
  putIfAbsent(
    locator: Locator,
    hash: ContentHash,
    bytes: Bytes,
  ): Promise<[ContentHash, Bytes] | null>;
  deleteExact(locator: Locator, expectedHash: ContentHash): Promise<void>;
}

export interface ChunkStorage {
  get(locator: Locator, expectedHash: ContentHash): Promise<Bytes>;
  stage(chunks: StagedChunk[]): Promise<StagedLargeValueAccounting>;
  delete?(locator: Locator, expectedHash: ContentHash): Promise<void>;
}

export function deleteChunk(s: ChunkStorage, locator: Locator, hash: ContentHash): Promise<void> {
  if (s.delete) return s.delete(locator, hash);
  return Promise.reject(
    new ChunkStorageError("Backend", "chunk deletion is not implemented by this backend"),
  );
}

const emptyAccounting = (): StagedLargeValueAccounting => ({ encodedBytes: 0, nodeCount: 0 });

/** Groove-owned integrity and staging layer over a policy-blind byte KV. */
export class ManagedChunkStorage implements ChunkStorage {
  constructor(private readonly backend: ChunkKvStorage) {}
  async get(locator: Locator, expectedHash: ContentHash): Promise<Bytes> {
    const found = await this.backend.getExact(locator);
    if (!found) throw new ChunkStorageError("Unavailable");
    const [hash, bytes] = found;
    if (!equalBytes(hash, expectedHash) || tooLargeOrMismatch(bytes, expectedHash))
      throw new ChunkStorageError("Integrity");
    return bytes;
  }
  async stage(chunks: StagedChunk[]): Promise<StagedLargeValueAccounting> {
    for (const c of chunks)
      if (tooLargeOrMismatch(c.encoded, c.nodeRef.objectHash))
        throw new ChunkStorageError("Integrity");
    const acc = emptyAccounting();
    for (const c of chunks) {
      const existing = await this.backend.putIfAbsent(
        c.nodeRef.locator,
        c.nodeRef.objectHash,
        c.encoded,
      );
      if (
        existing &&
        (!equalBytes(existing[0], c.nodeRef.objectHash) ||
          tooLargeOrMismatch(existing[1], c.nodeRef.objectHash))
      )
        throw new ChunkStorageError("LocatorConflict");
      if (!existing) {
        acc.encodedBytes += c.encoded.length;
        acc.nodeCount += 1;
      }
    }
    return acc;
  }
  delete(locator: Locator, expectedHash: ContentHash): Promise<void> {
    return this.backend.deleteExact(locator, expectedHash);
  }
}

/** Retargetable local exact-read service for Jazz's auxiliary transport. */
export class LocalChunkReader {
  private constructor(private readonly cell: { storage: ChunkStorage }) {}
  static new(storage: ChunkStorage): LocalChunkReader {
    return new LocalChunkReader({ storage });
  }
  clone(): LocalChunkReader {
    return new LocalChunkReader(this.cell);
  }
  refreshFrom(replacement: LocalChunkReader): void {
    this.cell.storage = replacement.cell.storage;
  }
  get(locator: Locator, expectedHash: ContentHash): Promise<Bytes> {
    return this.cell.storage.get(locator, expectedHash);
  }
}

export interface MissingChunkResolver {
  resolve(request: ChunkRequest): Promise<Bytes>;
}
export interface ChunkInstallObserver {
  installed(nodeRef: NodeRef, encoded: Bytes): Promise<void>;
  completesInstallJournal?(): boolean;
}
export interface ChunkInstallJournal {
  markPending(nodeRef: NodeRef): Promise<void>;
  isPending(nodeRef: NodeRef): Promise<boolean>;
  complete(nodeRef: NodeRef): Promise<void>;
}

export const noopChunkInstallObserver: ChunkInstallObserver = { installed: async () => {} };
export const noopChunkInstallJournal: ChunkInstallJournal = {
  markPending: async () => {},
  isPending: async () => false,
  complete: async () => {},
};
export const unavailableChunkResolver: MissingChunkResolver = {
  resolve: async () => {
    throw new ChunkError("Unavailable");
  },
};

/** In-memory Groove chunk storage used by ephemeral databases and tests. */
export class MemoryChunkStorage implements ChunkStorage, ChunkKvStorage {
  private readonly chunks = new Map<string, [Locator, ContentHash, Bytes]>();
  get len(): number {
    return this.chunks.size;
  }
  isEmpty(): boolean {
    return this.chunks.size === 0;
  }
  async get(locator: Locator, expectedHash: ContentHash): Promise<Bytes> {
    const e = this.chunks.get(toHex(locator));
    if (!e || !equalBytes(e[1], expectedHash)) throw new ChunkStorageError("Unavailable");
    return e[2];
  }
  async stage(chunks: StagedChunk[]): Promise<StagedLargeValueAccounting> {
    for (const c of chunks) {
      if (tooLargeOrMismatch(c.encoded, c.nodeRef.objectHash))
        throw new ChunkStorageError("Integrity");
      const e = this.chunks.get(toHex(c.nodeRef.locator));
      if (e && (!equalBytes(e[1], c.nodeRef.objectHash) || !equalBytes(e[2], c.encoded)))
        throw new ChunkStorageError("LocatorConflict");
    }
    const acc = emptyAccounting();
    for (const c of chunks) {
      const key = toHex(c.nodeRef.locator);
      if (this.chunks.has(key)) continue;
      acc.encodedBytes += c.encoded.length;
      acc.nodeCount += 1;
      this.chunks.set(key, [c.nodeRef.locator, c.nodeRef.objectHash, c.encoded]);
    }
    return acc;
  }
  async delete(locator: Locator, expectedHash: ContentHash): Promise<void> {
    const key = toHex(locator);
    const e = this.chunks.get(key);
    if (!e) return;
    if (!equalBytes(e[1], expectedHash)) throw new ChunkStorageError("Integrity");
    this.chunks.delete(key);
  }
  async getExact(locator: Locator): Promise<[ContentHash, Bytes] | null> {
    const e = this.chunks.get(toHex(locator));
    return e ? [e[1], e[2]] : null;
  }
  async putIfAbsent(locator: Locator, hash: ContentHash, bytes: Bytes) {
    const key = toHex(locator);
    const e = this.chunks.get(key);
    if (e) return [e[1], e[2]] as [ContentHash, Bytes];
    this.chunks.set(key, [locator, hash, bytes]);
    return null;
  }
  deleteExact(locator: Locator, expectedHash: ContentHash): Promise<void> {
    return this.delete(locator, expectedHash);
  }
}

/** Default durable byte plane colocated with the ordered metadata store. */
export class OrderedChunkStorage implements ChunkKvStorage {
  constructor(private readonly storage: WeakRef<LayoutStorage>) {}
  private static key(locator: Bytes): Bytes {
    return concatBytes(new TextEncoder().encode("chunk/"), locator);
  }
  private static decode(value: Bytes): [ContentHash, Bytes] {
    if (value.length < 48) throw new ChunkStorageError("Integrity");
    const hash = value.subarray(16, 48);
    const bytes = value.subarray(48);
    if (tooLargeOrMismatch(bytes, hash)) throw new ChunkStorageError("Integrity");
    return [hash.slice(), bytes.slice()];
  }
  private live(): LayoutStorage {
    const s = this.storage.deref();
    if (!s) throw new ChunkStorageError("Backend", "owning Groove storage is closed");
    return s;
  }
  private async call<T>(f: () => Promise<T>): Promise<T> {
    try {
      return await f();
    } catch (e) {
      throw new ChunkStorageError("Backend", String(e instanceof Error ? e.message : e));
    }
  }
  async getExact(locator: Locator) {
    const s = this.live();
    const v = await this.call(() =>
      s.get(LARGE_VALUE_METADATA_CF, OrderedChunkStorage.key(locator)),
    );
    return v ? OrderedChunkStorage.decode(v) : null;
  }
  async putIfAbsent(locator: Locator, hash: ContentHash, bytes: Bytes) {
    const s = this.live();
    const candidate = concatBytes(uuidV4(), hash, bytes);
    const existing = await this.call(() =>
      s.putIfAbsent(LARGE_VALUE_METADATA_CF, OrderedChunkStorage.key(locator), candidate),
    );
    return existing ? OrderedChunkStorage.decode(existing) : null;
  }
  async deleteExact(locator: Locator, expectedHash: ContentHash) {
    const s = this.live();
    const key = OrderedChunkStorage.key(locator);
    const existing = await this.call(() => s.get(LARGE_VALUE_METADATA_CF, key));
    if (!existing) return;
    const [hash] = OrderedChunkStorage.decode(existing);
    if (!equalBytes(hash, expectedHash)) throw new ChunkStorageError("Integrity");
    await this.call(() => s.compareAndDelete(LARGE_VALUE_METADATA_CF, key, existing));
  }
}

export interface ChunkProvider {
  get(request: ChunkRequest): Promise<Bytes>;
  permitsEagerReadRetry?(): boolean;
  getWithInstallObserver?(request: ChunkRequest, observer: ChunkInstallObserver): Promise<Bytes>;
}

/** Direct retrieval adapter over Groove-owned storage with remote fallback. */
export class StorageChunkProvider implements ChunkProvider {
  private readonly settledInstalls = new Set<string>();
  constructor(
    private readonly storage: ChunkStorage,
    private readonly resolver: MissingChunkResolver = unavailableChunkResolver,
    private readonly observer: ChunkInstallObserver = noopChunkInstallObserver,
    private readonly journal: ChunkInstallJournal = noopChunkInstallJournal,
  ) {}

  get(request: ChunkRequest): Promise<Bytes> {
    return this.getWithInstallObserver(request, this.observer);
  }

  async getWithInstallObserver(
    request: ChunkRequest,
    observer: ChunkInstallObserver,
  ): Promise<Bytes> {
    const nodeRef: NodeRef = { objectHash: request.objectHash, locator: request.locator };
    let bytes: Bytes;
    try {
      bytes = await this.storage.get(request.locator, request.objectHash);
    } catch (e) {
      if (!(e instanceof ChunkStorageError) || e.kind !== "Unavailable")
        throw ChunkError.fromStorage(e);
      const resolved = await this.resolver.resolve(request);
      if (tooLargeOrMismatch(resolved, nodeRef.objectHash)) throw new ChunkError("Integrity");
      await this.journal.markPending(nodeRef);
      try {
        await this.storage.stage([{ nodeRef, encoded: resolved }]);
      } catch (err) {
        throw ChunkError.fromStorage(err);
      }
      await this.reconcilePendingInstall(request, nodeRef, resolved, observer, true);
      return resolved;
    }
    await this.reconcilePendingInstall(request, nodeRef, bytes, observer, false);
    return bytes;
  }

  private async reconcilePendingInstall(
    request: ChunkRequest,
    nodeRef: NodeRef,
    bytes: Bytes,
    observer: ChunkInstallObserver,
    newlyStaged: boolean,
  ): Promise<void> {
    const key = chunkRequestKey(request);
    if (!newlyStaged && this.settledInstalls.has(key)) return;
    if (!newlyStaged && !(await this.journal.isPending(nodeRef))) {
      this.settledInstalls.add(key);
      return;
    }
    await observer.installed(nodeRef, bytes);
    if (!observer.completesInstallJournal?.()) await this.journal.complete(nodeRef);
    if (!(await this.journal.isPending(nodeRef))) this.settledInstalls.add(key);
  }
}

export class PublicationInstallFailures {
  private readonly failures = new Map<string, ChunkError>();
  record(nodeRef: NodeRef, error: ChunkError): void {
    this.failures.set(chunkRequestKey(nodeRef), error);
  }
  take(request: ChunkRequest): ChunkError | undefined {
    const key = chunkRequestKey(request);
    const e = this.failures.get(key);
    this.failures.delete(key);
    return e;
  }
}

export type ChunkCacheStats = {
  entries: number;
  ownedBytes: number;
  budgetBytes: number;
  activeLeases: number;
  leasedBytes: number;
  activeRequests: number;
};

type VerifiedChunkCache = {
  entries: Map<string, [Bytes, number]>;
  bytes: number;
  clock: number;
  budget: number;
};
type LeaseStats = { active: number; bytes: number };
type ActivityState = { activeRequests: number; reclaiming: boolean; waiters: (() => void)[] };
type InFlight = { promise: Promise<Bytes>; consumers: number };

export class OwnedChunkError extends Error {
  constructor(
    readonly error: ChunkError,
    readonly publicationMetadataDurability = false,
  ) {
    super(error.message);
  }
}

/** A verified chunk allocation leased to an evaluator; release() returns its accounting. */
export class ChunkLease {
  private released = false;
  constructor(
    readonly bytes: Bytes,
    private readonly verifiedObjectHash: Bytes,
    private readonly stats: LeaseStats,
  ) {
    stats.active += 1;
    stats.bytes += bytes.length;
  }
  verifiesObjectHash(expected: Bytes): boolean {
    return equalBytes(this.verifiedObjectHash, expected);
  }
  release(): void {
    if (this.released) return;
    this.released = true;
    this.stats.active = Math.max(this.stats.active - 1, 0);
    this.stats.bytes = Math.max(this.stats.bytes - this.bytes.length, 0);
  }
}

export class ChunkReclamationGuard {
  constructor(private readonly activity: ActivityState) {}
  release(): void {
    this.activity.reclaiming = false;
    for (const w of this.activity.waiters.splice(0)) w();
  }
}

export const unavailableChunkProvider: ChunkProvider = {
  get: async () => {
    throw new ChunkError("Unavailable");
  },
};

/** Shared verified cache, in-flight coalescing and reclamation gate over a provider. */
export class OwnedChunkProvider {
  private constructor(
    private readonly provider: ChunkProvider,
    private readonly cache: VerifiedChunkCache,
    private readonly leases: LeaseStats,
    private readonly activity: ActivityState,
    private readonly inFlight: Map<string, InFlight>,
    private readonly installObserver: ChunkInstallObserver | null,
    private readonly installFailures: PublicationInstallFailures | null,
  ) {}
  static new(provider: ChunkProvider = unavailableChunkProvider, budgetBytes = 16 * 1024 * 1024) {
    return new OwnedChunkProvider(
      provider,
      { entries: new Map(), bytes: 0, clock: 0, budget: budgetBytes },
      { active: 0, bytes: 0 },
      { activeRequests: 0, reclaiming: false, waiters: [] },
      new Map(),
      null,
      null,
    );
  }
  withInstallObserver(observer: ChunkInstallObserver, failures: PublicationInstallFailures) {
    return new OwnedChunkProvider(
      this.provider,
      this.cache,
      this.leases,
      this.activity,
      new Map(),
      observer,
      failures,
    );
  }
  cacheStats(): ChunkCacheStats {
    return {
      entries: this.cache.entries.size,
      ownedBytes: this.cache.bytes,
      budgetBytes: this.cache.budget,
      activeLeases: this.leases.active,
      leasedBytes: this.leases.bytes,
      activeRequests: this.activity.activeRequests,
    };
  }
  permitsEagerReadRetry(): boolean {
    return this.provider.permitsEagerReadRetry?.() ?? false;
  }
  tryBeginReclamation(): ChunkReclamationGuard | null {
    if (this.leases.active !== 0) return null;
    if (this.activity.reclaiming || this.activity.activeRequests !== 0) return null;
    this.activity.reclaiming = true;
    return new ChunkReclamationGuard(this.activity);
  }

  async get(request: ChunkRequest): Promise<ChunkLease> {
    try {
      return await this.getTracked(request);
    } catch (e) {
      throw e instanceof OwnedChunkError ? e.error : e;
    }
  }

  private async acquire(): Promise<void> {
    while (this.activity.reclaiming) await new Promise<void>((r) => this.activity.waiters.push(r));
    this.activity.activeRequests += 1;
  }
  private releaseRequest(): void {
    this.activity.activeRequests = Math.max(this.activity.activeRequests - 1, 0);
    if (this.activity.activeRequests === 0) for (const w of this.activity.waiters.splice(0)) w();
  }

  async getTracked(request: ChunkRequest): Promise<ChunkLease> {
    // Admission precedes even a verified-cache hit so reclamation can exclude readers.
    await this.acquire();
    try {
      const key = chunkRequestKey(request);
      this.cache.clock += 1;
      const cached = this.cache.entries.get(key);
      if (cached) {
        cached[1] = this.cache.clock;
        return new ChunkLease(cached[0], request.objectHash, this.leases);
      }
      let entry = this.inFlight.get(key);
      if (entry) entry.consumers += 1;
      else {
        entry = { promise: this.loadAndVerify(request), consumers: 1 };
        this.inFlight.set(key, entry);
      }
      try {
        const bytes = await entry.promise;
        return new ChunkLease(bytes, request.objectHash, this.leases);
      } finally {
        entry.consumers -= 1;
        if (entry.consumers === 0 && this.inFlight.get(key) === entry) this.inFlight.delete(key);
      }
    } finally {
      this.releaseRequest();
    }
  }

  private async loadAndVerify(request: ChunkRequest): Promise<Bytes> {
    let bytes: Bytes;
    if (this.installObserver) {
      let result: Bytes | ChunkError;
      try {
        result = await (this.provider.getWithInstallObserver?.(request, this.installObserver) ??
          this.provider.get(request));
      } catch (e) {
        result = e as ChunkError;
      }
      const failure = this.installFailures?.take(request);
      if (failure) throw new OwnedChunkError(failure, true);
      if (result instanceof Error) throw new OwnedChunkError(result, false);
      bytes = result;
    } else {
      try {
        bytes = await this.provider.get(request);
      } catch (e) {
        throw new OwnedChunkError(e as ChunkError);
      }
    }
    if (tooLargeOrMismatch(bytes, request.objectHash))
      throw new OwnedChunkError(new ChunkError("Integrity"));
    const cache = this.cache;
    if (bytes.length <= cache.budget) {
      while (cache.bytes + bytes.length > cache.budget) {
        let oldest: string | undefined;
        let oldestUse = Infinity;
        for (const [k, [, use]] of cache.entries)
          if (use < oldestUse) {
            oldest = k;
            oldestUse = use;
          }
        if (oldest === undefined) break;
        cache.bytes -= cache.entries.get(oldest)![0].length;
        cache.entries.delete(oldest);
      }
      cache.clock += 1;
      cache.entries.set(chunkRequestKey(request), [bytes, cache.clock]);
      cache.bytes += bytes.length;
    }
    return bytes;
  }
}

/** Deterministic controller for TestChunkProvider. */
export class TestChunkProviderControl {
  paused = false;
  permits = 0;
  failNext: ChunkError | null = null;
  readonly observed: ChunkRequest[] = [];
  waiters: (() => void)[] = [];
  pause(): void {
    this.paused = true;
  }
  releaseOne(): void {
    this.permits += 1;
    for (const w of this.waiters.splice(0)) w();
  }
  resume(): void {
    this.paused = false;
    this.permits = 0;
    for (const w of this.waiters.splice(0)) w();
  }
}

/** In-memory chunk capability with explicit suspension and failure control. */
export class TestChunkProvider implements ChunkProvider {
  private readonly resident = new Set<string>();
  constructor(
    private readonly chunks: Map<string, Bytes>,
    readonly control = new TestChunkProviderControl(),
  ) {}
  static controlled(
    chunks: Iterable<[ChunkRequest, Bytes]>,
  ): [TestChunkProvider, TestChunkProviderControl] {
    const p = new TestChunkProvider(new Map([...chunks].map(([r, b]) => [chunkRequestKey(r), b])));
    return [p, p.control];
  }
  evict(request: ChunkRequest): void {
    this.resident.delete(chunkRequestKey(request));
  }
  permitsEagerReadRetry(): boolean {
    return true;
  }
  async get(request: ChunkRequest): Promise<Bytes> {
    const key = chunkRequestKey(request);
    const c = this.control;
    c.observed.push(request);
    if (!this.resident.has(key)) await Promise.resolve();
    while (c.paused && c.permits === 0) await new Promise<void>((r) => c.waiters.push(r));
    if (c.paused) c.permits -= 1;
    const fail = c.failNext;
    c.failNext = null;
    if (fail) throw fail;
    const bytes = this.chunks.get(key);
    if (!bytes) throw new ChunkError("Unavailable");
    this.resident.add(key);
    return bytes;
  }
}
