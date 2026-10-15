// Owned work and request state for interruptible evaluation.
import { type Bytes, toHex } from "../../../support/bytes.js";
import { SortedMap, cmpStr } from "../../../support/collections.js";
import {
  IvmRuntimeError,
  ScanRequest,
  persistedIndexColumnValue,
  persistedIndexPrimaryKey,
  type ChunkLease,
  type ChunkRequest,
  type DatabaseSchema,
  type IndexSchema,
  type KeyValue,
  type NodeId,
  type OwnedChunkProvider,
  type OwnedStorage,
  type TableSchema,
} from "../../../_extern/groove-ivm-ops.js";

export type StorageRequestKey =
  | { kind: "Get"; family: string; key: Bytes }
  | { kind: "ScanRange"; family: string; start: Bytes; end: Bytes }
  | { kind: "ScanPrefix"; family: string; prefix: Bytes }
  | { kind: "ScanPrefixLimit"; family: string; prefix: Bytes; maxItems: number; reversed: boolean }
  | { kind: "IndexedRowsPrefix"; table: string; index: string; prefix: Bytes }
  | {
      kind: "IndexedRowsPrefixLimit";
      table: string;
      index: string;
      prefix: Bytes;
      maxItems: number;
      reversed: boolean;
    }
  | {
      kind: "IndexedRowsIntersection";
      table: string;
      index: string;
      prefix: Bytes;
      intersections: [string, Bytes][];
    }
  | {
      kind: "IndexedRowsCandidateFilter";
      table: string;
      index: string;
      prefix: Bytes;
      candidateTable: string;
      candidateIndex: string;
      candidatePrefix: Bytes;
      sourceColumn: string;
      candidateColumn: string;
    }
  | { kind: "IndexedRowsRange"; table: string; index: string; start: Bytes; end: Bytes };

export type EvaluationRequestKey =
  | { kind: "Storage"; request: StorageRequestKey }
  | { kind: "Chunk"; request: ChunkRequest };

export type StorageRequestOutput =
  | { kind: "Value"; value?: Bytes }
  | { kind: "Rows"; rows: KeyValue[] };
export type EvaluationRequestOutput =
  | { kind: "Storage"; output: StorageRequestOutput }
  | { kind: "Chunk"; chunk: LoadedChunk };

/** Canonical ordered key for request maps (Rust derives Ord/Hash). */
export function requestKeyString(key: EvaluationRequestKey): string {
  return JSON.stringify(key, (_, v) =>
    v instanceof Uint8Array ? toHex(v) : typeof v === "bigint" ? String(v) : v,
  );
}

export class LoadedChunk {
  constructor(
    private readonly source:
      | { kind: "Direct"; bytes: Bytes }
      | { kind: "Leased"; lease: ChunkLease },
  ) {}

  bytes(): Bytes {
    return this.source.kind === "Direct" ? this.source.bytes : this.source.lease.bytes();
  }

  verifiesObjectHash(expected: Bytes): boolean {
    return this.source.kind === "Leased" && this.source.lease.verifiesObjectHash(expected);
  }
}

export class EvaluationInputs {
  private loaded = new SortedMap<string, [EvaluationRequestKey, EvaluationRequestOutput]>(cmpStr);
  private missing = new SortedMap<string, EvaluationRequestKey>(cmpStr);
  /** A streaming operator may evict only chunks it alone reached. */
  private chunkOwners = new Map<string, Set<NodeId>>();
  private chunkScope?: NodeId;

  setChunkScope(scope: NodeId | undefined): void {
    this.chunkScope = scope;
  }

  /** Release only chunks no other evaluation path has claimed. */
  releaseChunksOwnedBy(owner: NodeId): void {
    this.loaded.retain((k, [key]) => {
      if (key.kind !== "Chunk") return true;
      const owners = this.chunkOwners.get(k);
      if (!owners) return false;
      owners.delete(owner);
      return owners.size > 0;
    });
    for (const [k, owners] of [...this.chunkOwners])
      if (owners.size === 0) this.chunkOwners.delete(k);
  }

  takeMissingChunks(): ChunkRequest[] {
    return this.takeMissing()
      .filter((key) => key.kind === "Chunk")
      .map((key) => key.request);
  }

  installChunk(request: ChunkRequest, bytes: Bytes): void {
    const key: EvaluationRequestKey = { kind: "Chunk", request };
    this.loaded.set(requestKeyString(key), [
      key,
      { kind: "Chunk", chunk: new LoadedChunk({ kind: "Direct", bytes }) },
    ]);
  }

  installChunkFromProvider(request: ChunkRequest, lease: ChunkLease): void {
    const key: EvaluationRequestKey = { kind: "Chunk", request };
    this.loaded.set(requestKeyString(key), [
      key,
      { kind: "Chunk", chunk: new LoadedChunk({ kind: "Leased", lease }) },
    ]);
  }

  chunk(request: ChunkRequest): Bytes {
    return this.loadedChunk(request).bytes();
  }

  loadedChunk(request: ChunkRequest): LoadedChunk {
    const key: EvaluationRequestKey = { kind: "Chunk", request };
    const k = requestKeyString(key);
    if (this.chunkScope !== undefined) {
      let owners = this.chunkOwners.get(k);
      if (!owners) this.chunkOwners.set(k, (owners = new Set()));
      owners.add(this.chunkScope);
    }
    const loaded = this.loaded.get(k);
    if (!loaded) {
      this.missing.set(k, key);
      throw new IvmRuntimeError("EvaluationBlocked");
    }
    if (loaded[1].kind !== "Chunk") throw new IvmRuntimeError("UnsupportedOperator");
    return loaded[1].chunk;
  }

  rows(request: StorageRequestKey): KeyValue[] {
    const key: EvaluationRequestKey = { kind: "Storage", request };
    const k = requestKeyString(key);
    const loaded = this.loaded.get(k);
    if (!loaded) {
      this.missing.set(k, key);
      throw new IvmRuntimeError("EvaluationBlocked");
    }
    const output = loaded[1];
    if (output.kind !== "Storage" || output.output.kind !== "Rows")
      throw new IvmRuntimeError("UnsupportedOperator");
    return output.output.rows;
  }

  install(ready: Iterable<[EvaluationRequestKey, EvaluationRequestOutput]>): void {
    for (const [key, output] of ready) this.loaded.set(requestKeyString(key), [key, output]);
  }

  takeMissing(): EvaluationRequestKey[] {
    const missing = [...this.missing.valuesArray()];
    this.missing.clear();
    return missing;
  }

  /** Move every loaded large-value chunk into a replacing session (#3901). */
  handOffChunks(successor: EvaluationInputs): void {
    this.loaded.retain((k, entry) => {
      if (entry[0].kind !== "Chunk") return true;
      if (!successor.loaded.has(k)) successor.loaded.set(k, entry);
      return false;
    });
  }
}

export interface EvaluationRequestFailure {
  error: Error;
  publicationMetadataDurability: boolean;
}

type Settled =
  | { ok: true; value: EvaluationRequestOutput }
  | { ok: false; error: EvaluationRequestFailure };

interface PendingRequest {
  key: EvaluationRequestKey;
  settled?: Settled;
  eagerRetrySafe: boolean;
}

const failure = (error: any): EvaluationRequestFailure =>
  error && "publicationMetadataDurability" in error
    ? error
    : { error, publicationMetadataDurability: false };

/** One request registry per evaluation session: equal requests share one future. */
export class EvaluationRequests {
  private pending = new SortedMap<string, PendingRequest>(cmpStr);
  private ready = new SortedMap<string, [EvaluationRequestKey, Settled]>(cmpStr);

  /** Register a request unless its future or result exists; true for the creator. */
  request(
    key: EvaluationRequestKey,
    storage: OwnedStorage,
    chunks: OwnedChunkProvider | undefined,
    schema: DatabaseSchema,
  ): boolean {
    const k = requestKeyString(key);
    if (this.pending.has(k) || this.ready.has(k)) return false;
    const eagerRetrySafe =
      key.kind === "Storage"
        ? storage.permitsEagerReadRetry()
        : !!chunks && chunks.permitsEagerReadRetry();
    let future: Promise<EvaluationRequestOutput>;
    if (key.kind === "Storage") {
      future = storageRequest(key.request, storage, schema).then((output) => ({
        kind: "Storage",
        output,
      }));
    } else if (chunks) {
      future = chunks.getTracked(key.request).then(
        (lease: ChunkLease) => ({
          kind: "Chunk",
          chunk: new LoadedChunk({ kind: "Leased", lease }),
        }),
        (error: any) => {
          const [inner, publicationMetadataDurability] = error.intoParts();
          throw { error: new IvmRuntimeError("Chunk", inner), publicationMetadataDurability };
        },
      );
    } else {
      future = Promise.reject(failure(new IvmRuntimeError("Chunk", "Unavailable")));
    }
    const pending: PendingRequest = { key, eagerRetrySafe };
    future.then(
      (value) => (pending.settled = { ok: true, value }),
      (error) => (pending.settled = { ok: false, error: failure(error) }),
    );
    this.pending.set(k, pending);
    return true;
  }

  /** Advance every in-flight request once. */
  poll(): number {
    return this.pollWhere(false);
  }

  /** Re-poll only requests whose backend guarantees no external I/O wait. */
  pollEagerRetry(): number {
    return this.pollWhere(true);
  }

  private pollWhere(onlyEagerRetrySafe: boolean): number {
    const completed: string[] = [];
    for (const [k, request] of this.pending) {
      if (onlyEagerRetrySafe && !request.eagerRetrySafe) continue;
      if (request.settled) completed.push(k);
    }
    for (const k of completed) {
      const request = this.pending.delete(k)!;
      this.ready.set(k, [request.key, request.settled!]);
    }
    return completed.length;
  }

  hasPending(): boolean {
    return this.pending.size > 0;
  }

  /** Move every chunk fetch, in flight or finished, into a replacing session (#3901). */
  handOffChunks(successor: EvaluationRequests): void {
    this.pending.retain((k, request) => {
      if (request.key.kind !== "Chunk") return true;
      if (!successor.pending.has(k)) successor.pending.set(k, request);
      return false;
    });
    this.ready.retain((k, entry) => {
      if (entry[0].kind !== "Chunk") return true;
      if (!successor.ready.has(k)) successor.ready.set(k, entry);
      return false;
    });
  }

  hasPendingChunk(): boolean {
    for (const [, request] of this.pending) if (request.key.kind === "Chunk") return true;
    return false;
  }

  drainReady(): [EvaluationRequestKey, EvaluationRequestOutput][] {
    const ready = [...this.ready.valuesArray()];
    this.ready.clear();
    return ready.map(([key, settled]) => {
      if (!settled.ok)
        throw Object.assign(new Error("evaluation request failed"), {
          key,
          failure: settled.error,
        });
      return [key, settled.value];
    });
  }
}

function indexedSource(
  schema: DatabaseSchema,
  table: string,
  index: string,
): [TableSchema, IndexSchema] {
  const tableSchema = schema.table(table);
  if (!tableSchema) throw new Error("compiled indexed-row source table exists");
  const indexSchema = tableSchema.indices.find((c: IndexSchema) => c.name === index);
  if (!indexSchema) throw new Error("compiled indexed-row source index exists");
  return [tableSchema, indexSchema];
}

const indexPrefix = (prefix: Bytes) => ScanRequest.prefix("indices", prefix);

function limited(request: any, maxItems: number, reversed: boolean): any {
  const out = request.withMaxItems(maxItems);
  return reversed ? out.reversed() : out;
}

async function indexedRows(
  storage: OwnedStorage,
  schema: DatabaseSchema,
  table: string,
  index: string,
  request: any,
): Promise<StorageRequestOutput> {
  const [tableSchema, indexSchema] = indexedSource(schema, table, index);
  const entries = await storage.scan(request);
  return loadIndexedRows(storage, tableSchema, indexSchema, index, entries);
}

/** Below this many source entries, a candidate-filtered source hydrates directly. */
const CANDIDATE_FILTER_MIN_SOURCE_ENTRIES = 512;

async function storageRequest(
  key: StorageRequestKey,
  storage: OwnedStorage,
  schema: DatabaseSchema,
): Promise<StorageRequestOutput> {
  const rows = async (request: any): Promise<StorageRequestOutput> => ({
    kind: "Rows",
    rows: await storage.scan(request),
  });
  switch (key.kind) {
    case "Get":
      return { kind: "Value", value: await storage.get(key.family, key.key) };
    case "ScanRange":
      return rows(ScanRequest.range(key.family, key.start, key.end));
    case "ScanPrefix":
      return rows(ScanRequest.prefix(key.family, key.prefix));
    case "ScanPrefixLimit":
      return rows(limited(ScanRequest.prefix(key.family, key.prefix), key.maxItems, key.reversed));
    case "IndexedRowsPrefix":
      return indexedRows(storage, schema, key.table, key.index, indexPrefix(key.prefix));
    case "IndexedRowsPrefixLimit":
      return indexedRows(
        storage,
        schema,
        key.table,
        key.index,
        limited(indexPrefix(key.prefix), key.maxItems, key.reversed),
      );
    case "IndexedRowsRange":
      return indexedRows(
        storage,
        schema,
        key.table,
        key.index,
        ScanRequest.range("indices", key.start, key.end),
      );
    case "IndexedRowsIntersection": {
      const [tableSchema, primaryIndexSchema] = indexedSource(schema, key.table, key.index);
      const entries: KeyValue[] = await storage.scan(indexPrefix(key.prefix));
      const primaryKeys = indexedPrimaryKeys(tableSchema, key.index, primaryIndexSchema, entries);
      const retained = new Set(primaryKeys.map(toHex));
      for (const [otherIndex, otherPrefix] of key.intersections) {
        const otherSchema = tableSchema.indices.find((c: IndexSchema) => c.name === otherIndex);
        if (!otherSchema) throw new Error("compiled intersected index exists");
        const otherEntries = await storage.scan(indexPrefix(otherPrefix));
        const otherKeys = new Set(
          indexedPrimaryKeys(tableSchema, otherIndex, otherSchema, otherEntries).map(toHex),
        );
        for (const k of [...retained]) if (!otherKeys.has(k)) retained.delete(k);
      }
      const kept = entries.filter((_, i) => retained.has(toHex(primaryKeys[i])));
      return loadIndexedRows(storage, tableSchema, primaryIndexSchema, key.index, kept);
    }
    case "IndexedRowsCandidateFilter": {
      const [tableSchema, indexSchema] = indexedSource(schema, key.table, key.index);
      const [candidateSchema, candidateIndexSchema] = indexedSource(
        schema,
        key.candidateTable,
        key.candidateIndex,
      );
      const entries: KeyValue[] = await storage.scan(indexPrefix(key.prefix));
      // An already-small source is cheaper to hydrate directly; cap the candidate probe.
      if (entries.length < CANDIDATE_FILTER_MIN_SOURCE_ENTRIES) {
        return loadIndexedRows(storage, tableSchema, indexSchema, key.index, entries);
      }
      const candidateScanLimit = Math.ceil(entries.length / 2);
      const candidates: KeyValue[] = await storage.scan(
        indexPrefix(key.candidatePrefix).withMaxItems(candidateScanLimit),
      );
      if (candidates.length === candidateScanLimit) {
        return loadIndexedRows(storage, tableSchema, indexSchema, key.index, entries);
      }
      const candidateUuids = new Set(
        indexedUuidValues(
          candidateSchema,
          key.candidateIndex,
          candidateIndexSchema,
          candidates,
          key.candidateColumn,
        )
          .filter((u): u is Bytes => u !== undefined)
          .map(toHex),
      );
      if (candidateUuids.size === 0) return { kind: "Rows", rows: [] };
      const sourceUuids = indexedUuidValues(
        tableSchema,
        key.index,
        indexSchema,
        entries,
        key.sourceColumn,
      );
      const kept = entries.filter((_, i) => {
        const uuid = sourceUuids[i];
        return uuid !== undefined && candidateUuids.has(toHex(uuid));
      });
      return loadIndexedRows(storage, tableSchema, indexSchema, key.index, kept);
    }
  }
}

async function loadIndexedRows(
  storage: OwnedStorage,
  table: TableSchema,
  indexSchema: IndexSchema,
  indexName: string,
  entries: KeyValue[],
): Promise<StorageRequestOutput> {
  const primaryKeys = indexedPrimaryKeys(table, indexName, indexSchema, entries);
  const rows = await Promise.all(
    primaryKeys.map(async (primaryKey): Promise<KeyValue> => {
      const record = await storage.get(table.name, primaryKey);
      if (!record) throw new IvmRuntimeError("InvalidPersistedIndex", indexName);
      return [primaryKey, record];
    }),
  );
  return { kind: "Rows", rows };
}

function indexedUuidValues(
  table: TableSchema,
  indexName: string,
  indexSchema: IndexSchema,
  entries: KeyValue[],
  column: string,
): (Bytes | undefined)[] {
  return entries.map(([storageKey, storedValue]) => {
    let value;
    try {
      value = persistedIndexColumnValue(
        table,
        indexName,
        indexSchema,
        storageKey,
        storedValue,
        column,
      );
    } catch {
      throw new IvmRuntimeError("InvalidPersistedIndex", indexName);
    }
    while (value.kind === "Nullable" && value.value !== undefined) value = value.value;
    if (value.kind === "Uuid") return value.value;
    if (value.kind === "Nullable") return undefined;
    throw new IvmRuntimeError("InvalidPersistedIndex", indexName);
  });
}

function indexedPrimaryKeys(
  table: TableSchema,
  indexName: string,
  indexSchema: IndexSchema,
  entries: KeyValue[],
): Bytes[] {
  return entries.map(([storageKey, storedValue]) => {
    try {
      return persistedIndexPrimaryKey(table, indexName, indexSchema, storageKey, storedValue);
    } catch {
      throw new IvmRuntimeError("InvalidPersistedIndex", indexName);
    }
  });
}
