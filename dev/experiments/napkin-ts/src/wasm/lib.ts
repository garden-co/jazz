// JS-facing binding surface. wasm-bindgen marshalling (JsValue/Reflect juggling,
// serde_wasm_bindgen, Promise/Closure/waker plumbing) is gone: JS values are the
// representation and async fns are Promises.
import { fromBase64Url } from "../support/bytes.js";
import { uuidToString, uuidV7 } from "../support/hash.js";
import { Reader, Writer } from "../support/postcard.js";
import { deferred } from "../support/runtime.js";
import { remoteLinkHintFromHostStr, type FirstLoad } from "../db/db/first_load.js";
import * as lifecycle from "../db/db/lifecycle.js";
import { WireTransportAdapter } from "../db/db/wire_transport.js";
import type { WireTransport } from "../_extern/db-rest-wasm.js";
import {
  AuthorSubject,
  ClientRelayScope,
  DbConfig,
  FEATURE_AUTHORIZATION_SCOPE_VIEWS,
  IdbStorage,
  InitialSyncFlushCadence,
  JazzDbError,
  MAX_WIRE_FRAME_BYTES,
  MemoryStorage,
  NodeUuid,
  SeededRowIdSource,
  TransactionId,
  WIRE_PROTOCOL_VERSION,
  bindingArgs,
  currentWireFeatures,
  decodeAuthorSubject,
  decodeNamedCells,
  decodeNodeUuid,
  decodePublicSchemaJson,
  decodeWebsocketFrameBatch,
  defaultLargeValueStagingMaxAgeMs,
  defaultReadOpts,
  encodeRelationSnapshot,
  encodeRowUuid,
  encodeRows,
  encodeSubscriptionDelta,
  encodeTxId,
  installHostJsonSchemaValidator,
  parseOpenTransactionId,
  validateFramesForArtifactCorpus,
  verifyClientRuntimeAuthorAt,
} from "../_extern/db-rest-wasm.js";
import {
  ANONYMOUS_ISSUER,
  LOCAL_FIRST_ISSUER,
  deriveUserId as deriveId,
  mintJazzSelfSignedTokenAt,
} from "./identity.js";
import {
  setAsGlobalDefaultWithConfig,
  setTraceEntryCollectionEnabled as setCollect,
  drainTraceEntries as drain,
  subscribeTraceEntries as subscribe,
} from "../wasm_tracing/lib.js";
import { WasmLayerConfig, type Level } from "../wasm_tracing/config.js";
import { IndexedDbPageStore } from "../idb_tree/web.js";

type Bytes = Uint8Array;
type Opt<T> = T | undefined | null;
const present = (v: unknown) => v !== undefined && v !== null;

// ---------------------------------------------------------------- free functions

export const validateSchema = (schema: Bytes): void => void decodePublicSchema(schema);

let wasmTracingInitialized = false;
function initializeWasmTracing(): void {
  if (wasmTracingInitialized) return;
  wasmTracingInitialized = true;
  setAsGlobalDefaultWithConfig(
    new WasmLayerConfig().withMaxLevel(configuredWasmLogLevel()).withConsoleGroupSpans(),
  );
}

function configuredWasmLogLevel(): Level {
  const value = (globalThis as any).__JAZZ_WASM_LOG_LEVEL;
  switch (typeof value === "string" ? value.toLowerCase() : undefined) {
    case "error":
      return "ERROR";
    case "info":
      return "INFO";
    case "debug":
      return "DEBUG";
    case "trace":
      return "TRACE";
    default:
      return "WARN";
  }
}

export const setTraceEntryCollectionEnabled = (enabled: boolean) => setCollect(enabled);
export const drainTraceEntries = () => drain();
export const subscribeTraceEntries = (callback: (...a: any[]) => void) => subscribe(callback);

/** Install the host JSON Schema validator for JSON columns. */
export function setJsonSchemaValidator(compile: (schemaJson: string) => unknown): void {
  installHostJsonSchemaValidator({
    compile(schemaJson: string) {
      let check: unknown;
      try {
        check = compile(schemaJson);
      } catch (error) {
        throw jsErrorMessage(error);
      }
      if (typeof check !== "function") throw "the JSON Schema validator did not return a function";
      return {
        validate(instanceJson: string): string | undefined {
          let outcome: unknown;
          try {
            outcome = check(instanceJson);
          } catch (error) {
            throw jsErrorMessage(error);
          }
          return typeof outcome === "string" ? outcome : undefined;
        },
      };
    },
  });
}

const jsErrorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : typeof error === "string" ? error : String(error);

export const nativeArtifactFingerprint = (): string =>
  (globalThis as any).JAZZ_NATIVE_ARTIFACT_FINGERPRINT ?? "missing-build-fingerprint";

export function __testValidateWireFrameBatchCorpus(frame: Bytes, negotiatedFeatures: string): void {
  if (!/^\d+$/.test(negotiatedFeatures)) throw "test wire corpus features must be a u64 decimal";
  validateFramesForArtifactCorpus(decodeWebsocketFrameBatch(frame), BigInt(negotiatedFeatures));
}
export const __testWireFrameCorpusFeatures = (): string => String(currentWireFeatures());

export const generateId = (): string => uuidToString(uuidV7());
export const currentTimestamp = (): number => Date.now();

function decodeSeed(seedB64: string): Bytes {
  let bytes: Bytes;
  try {
    bytes = fromBase64Url(seedB64);
  } catch (e) {
    throw `seed base64 decode error: ${e}`;
  }
  if (bytes.length !== 32) throw "seed must be exactly 32 bytes";
  return bytes;
}

type PermissionAdvice = "Allowed" | "Denied" | "Unknown";
const adviceString = (advice: PermissionAdvice): string => advice.toLowerCase();

export const mintLocalFirstToken = (seedB64: string, audience: string, ttl: number, now: number) =>
  mintJazzSelfSignedTokenAt(decodeSeed(seedB64), LOCAL_FIRST_ISSUER, audience, ttl, now);
export const deriveUserId = (seedB64: string): string =>
  uuidToString(deriveId(decodeSeed(seedB64)));
export const mintAnonymousToken = (seedB64: string, audience: string, ttl: number, now: number) =>
  mintJazzSelfSignedTokenAt(decodeSeed(seedB64), ANONYMOUS_ISSUER, audience, ttl, now);

interface WasmOpenDbConfig {
  identity: { node: any; author: any };
  rowIdSeed?: bigint;
  historyComplete: boolean;
  initialSyncFlushEvery?: number;
}

function decodeOpenDbConfig(r: Reader): WasmOpenDbConfig {
  const identity = { node: decodeNodeUuid(r), author: decodeAuthorSubject(r) };
  const rowIdSeed = r.bool() ? BigInt(r.varint()) : undefined;
  const historyComplete = r.bool();
  const initialSyncFlushEvery = r.bool() ? r.varint() : undefined;
  return { identity, rowIdSeed, historyComplete, initialSyncFlushEvery };
}

// ---------------------------------------------------------------- streaming mutation

interface StreamingState {
  db: WasmDbInner;
  upload: any;
  mutation: "Insert" | "Update" | "Upsert";
  table: string;
  rowId: any;
  cells: any;
  column: string;
  identity: WriteIdentity;
  updatedAtMs?: number;
  head?: any;
  base?: any;
}

type StreamingLifecycle =
  | { kind: "Open"; state: StreamingState }
  | { kind: "PushInFlight"; completion: Promise<StreamingState> }
  | { kind: "AbortClaimed" }
  | { kind: "Closed" };

const streamingMutationClosed = () => "streaming mutation is closed";

export class StreamingMutation {
  constructor(private lifecycle: StreamingLifecycle) {}

  async push(chunk: Bytes): Promise<void> {
    const current = this.lifecycle;
    if (current.kind !== "Open") throw streamingMutationClosed();
    const state = current.state;
    const completion = deferred<StreamingState>();
    this.lifecycle = { kind: "PushInFlight", completion: completion.promise };
    let error: unknown;
    try {
      await state.db.db.pushStreamingValueUpload(state.upload, chunk);
    } catch (e) {
      error = toJsError(e);
    }
    switch ((this.lifecycle as StreamingLifecycle).kind) {
      case "AbortClaimed":
        completion.resolve(state);
        break;
      case "PushInFlight":
        this.lifecycle = error === undefined ? { kind: "Open", state } : { kind: "Closed" };
        break;
      default:
        throw new Error("streaming mutation push completion lost its lifecycle");
    }
    if (error !== undefined) throw error;
  }

  async finish(): Promise<WasmWrite> {
    const current = this.lifecycle;
    if (current.kind !== "Open") throw streamingMutationClosed();
    this.lifecycle = { kind: "Closed" };
    const s = current.state;
    const db = s.db.db;
    const write = await db
      .finishStreamingValueUpload(
        s.upload,
        s.mutation,
        s.table,
        s.rowId,
        s.cells,
        s.column,
        s.identity,
        s.updatedAtMs,
        s.head,
        s.base,
      )
      .catch(rethrowJs);
    return wasmWrite(db, write);
  }

  async abort(): Promise<boolean> {
    const current = this.lifecycle;
    if (current.kind !== "Open" && current.kind !== "PushInFlight") return false;
    this.lifecycle = { kind: "AbortClaimed" };
    const state = current.kind === "Open" ? current.state : await current.completion;
    try {
      await state.db.db.abortStreamingValueUpload(state.upload);
    } catch (e) {
      throw toJsError(e);
    } finally {
      this.lifecycle = { kind: "Closed" };
    }
    return true;
  }
}

// ---------------------------------------------------------------- write handle

export class WasmWrite {
  constructor(
    readonly payload: Bytes,
    private row: any,
    private tx: any,
    private inner?: { db: any; write: any },
  ) {}

  get txId(): string {
    return this.tx.toString();
  }
  get rowId(): Bytes {
    return this.row.toBytes();
  }

  async writeState(): Promise<any> {
    if (!this.inner) throw "write state is unavailable";
    return this.inner.write.writeState().catch(rethrowJs);
  }

  wait(tier: string, observeOnly?: boolean): Promise<void> {
    const options = { tier: durabilityTierFromStr(tier), observeOnly: observeOnly ?? false };
    if (!this.inner) throw "write state is unavailable";
    const { db, write } = this.inner;
    return new Promise((resolve, reject) =>
      lifecycle.waitForWriteWith(db, write, options, (result: any) =>
        result.ok ? resolve() : reject(toJsError(result.error)),
      ),
    );
  }

  close(): boolean {
    const had = this.inner !== undefined;
    this.inner = undefined;
    return had;
  }
}

function wasmWrite(db: any, write: any): WasmWrite {
  const txId = write.mergeableTxId();
  const rowId = write.rowUuid();
  const w = new Writer();
  encodeRowUuid(w, rowId);
  encodeTxId(w, txId);
  return new WasmWrite(w.finish(), rowId, TransactionId.fromCommittedTx(txId), { db, write });
}

// ---------------------------------------------------------------- db inner

/** A memory-backed handle drives each queued mutation inline; a browser handle
 * leaves that to the host's tick scheduler. */
class WasmDbInner {
  constructor(
    public db: any,
    public drivesMutationsInline: boolean,
  ) {}

  afterEnqueue(): void {
    if (this.drivesMutationsInline) lifecycle.driveQueuedMutationOnce(this.db);
  }

  requestPermissionAdvice(action: any): WasmPermissionAdviceRequest {
    const future = lifecycle.requestPermissionAdvice(this.db, action);
    const requestId = future.requestId();
    return new WasmPermissionAdviceRequest(
      future.then((advice: PermissionAdvice) => adviceString(advice)),
      () => lifecycle.cancelPermissionAdviceRequest(this.db, requestId),
    );
  }

  async registerSchemaView(schema: any): Promise<WasmDbInner> {
    const db = await lifecycle.registerSchemaView(this.db, schema).catch(rethrowJs);
    return new WasmDbInner(db, this.drivesMutationsInline);
  }

  async allSerializedQuery(
    query: Bytes,
    opts: any,
    openTx: any,
    requestScope: any,
    author: any,
    requireCoverage: boolean,
    coverageDeadlineMs: number,
  ): Promise<any> {
    // The coverage budget runs from the first wait on the server (#3839).
    const budget = Number.isFinite(coverageDeadlineMs)
      ? coverageDeadlineMs - Date.now()
      : coverageDeadlineMs;
    const read = () => {
      let started: number | undefined;
      return this.db.allSerializedQuery(
        query,
        opts,
        openTx,
        requestScope,
        author,
        requireCoverage,
        () => {
          started ??= Date.now();
          return Date.now() - started >= budget;
        },
        (attachment: any) => this.db.detachQuery(attachment),
      );
    };
    if (openTx === undefined) return read();
    try {
      return await lifecycle.enqueueTransactionRead(this.db, openTx, read);
    } catch (e) {
      if (e === CANCELLED) throw transactionReadCancelled();
      throw e;
    }
  }

  begin(kind: "mergeable" | "exclusive", id: any, author: any, attribution: any): void {
    if (kind === "mergeable") this.db.enqueueBeginMergeable(id, author, attribution);
    else this.db.enqueueBeginExclusive(id, author, attribution);
    this.afterEnqueue();
  }

  abandonTransaction(txId: any): void {
    this.db.enqueueAbandonTransactionHandle(txId);
    this.afterEnqueue();
  }

  commit(kind: "mergeable" | "exclusive", openTxId: any): WasmWrite {
    let write;
    try {
      write =
        kind === "mergeable"
          ? this.db.enqueueCommitMergeableHandleAtMs(openTxId, currentTimestamp())
          : this.db.enqueueCommitExclusiveHandleAtMs(openTxId, currentTimestamp());
    } catch (e) {
      throw toJsError(e);
    }
    this.afterEnqueue();
    return wasmWrite(this.db, write);
  }

  setTickScheduler(callback: (urgency: string) => void): void {
    let pending = false;
    lifecycle.setTickScheduler(this.db, {
      scheduleTick(urgency: "Immediate" | "Deferred" | "AfterCurrentTurn") {
        callback(
          urgency === "Immediate"
            ? "immediate"
            : urgency === "Deferred"
              ? "deferred"
              : "after-current-turn",
        );
      },
      scheduleTickAfter(delayMs: number) {
        callback(`after:${delayMs}`);
      },
      queryRuntimeWaker() {
        return () => {
          if (pending) return;
          pending = true;
          queueMicrotask(() => {
            pending = false;
            callback("immediate");
          });
        };
      },
    });
  }
}

const CANCELLED = Symbol("cancelled");
const transactionReadCancelled = () =>
  new JazzDbError("Protocol", "transaction read owner operation was cancelled");

export class WasmPermissionAdviceRequest {
  constructor(
    readonly promise: Promise<string>,
    readonly cancel: () => void,
  ) {}
}

// ---------------------------------------------------------------- wire queues

class WasmWireQueues {
  inbound: Bytes[] = [];
  outbound: Bytes[] = [];
  outboundScheduler: (() => void) | undefined;
}

const wasmWireTransport = (queues: WasmWireQueues): WireTransport => ({
  sendFrame(frame: Bytes) {
    queues.outbound.push(frame);
    queues.outboundScheduler?.();
  },
  tryRecvFrame: () => queues.inbound.shift(),
});

// ---------------------------------------------------------------- WasmDb

export class WasmDb {
  private constructor(
    private inner: WasmDbInner | undefined,
    private ownsRuntime: boolean,
    private nonDurableClient: { value: boolean },
    private trustedBackend: boolean,
  ) {}

  private openInner(): WasmDbInner {
    if (!this.inner) throw "WasmDb is closed";
    return this.inner;
  }

  wireFeatures(): number {
    return Number(currentWireFeatures()) >>> 0;
  }

  private requireTrustedBackend(): void {
    if (!this.trustedBackend) throw "backend attribution requires an explicit backend runtime";
  }

  private readAuthor(author: Opt<Bytes>): any {
    if (present(author)) return authorIdFromBytes(author!);
    return this.trustedBackend ? AuthorSubject.SYSTEM : undefined;
  }

  private enqueue<T>(f: (inner: WasmDbInner) => T): T {
    const inner = this.openInner();
    let out: T;
    try {
      out = f(inner);
    } catch (e) {
      throw toJsError(e);
    }
    inner.afterEnqueue();
    return out;
  }

  insert(table: string, cells: Bytes, options: any): WasmWrite {
    const c = decodeCells(cells);
    const o = insertOptionsFromJs(options);
    let db: any;
    const write = this.enqueue((i) => ((db = i.db), i.db.enqueueInsert(table, c, o)));
    return wasmWrite(db, write);
  }

  insertInTransaction(openTransactionId: string, table: string, cells: Bytes, options: any): Bytes {
    const id = parseOpenTx(openTransactionId);
    const c = decodeCells(cells);
    const o = insertOptionsFromJs(options);
    return this.enqueue((i) => i.db.enqueueTransactionInsert(id, table, c, o)).toBytes();
  }

  update(table: string, rowId: Bytes, patch: Bytes, options: any): WasmWrite {
    const row = rowUuidFromBytes(rowId);
    const p = decodeCells(patch);
    const o = updateOptionsFromJs(options);
    let db: any;
    const write = this.enqueue((i) => ((db = i.db), i.db.enqueueUpdate(table, row, p, o)));
    return wasmWrite(db, write);
  }

  updateInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Bytes,
    patch: Bytes,
    options: any,
  ): void {
    const id = parseOpenTx(openTransactionId);
    const row = rowUuidFromBytes(rowId);
    const p = decodeCells(patch);
    const o = updateOptionsFromJs(options);
    this.enqueue((i) => i.db.enqueueTransactionUpdate(id, table, row, p, o));
  }

  updateLargeValuesInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Bytes,
    patch: Bytes,
    mutations: any,
    updatedAtMs?: number,
  ): void {
    const id = parseOpenTx(openTransactionId);
    const row = rowUuidFromBytes(rowId);
    const p = decodeCells(patch);
    const m = largeValueUpdates(mutations);
    const at = present(updatedAtMs) ? checkedJsU64(updatedAtMs!, "updatedAtMs") : undefined;
    this.enqueue((i) => i.db.enqueueTransactionLargeValueUpdate(id, table, row, p, m, at));
  }

  updateLargeValues(
    table: string,
    rowId: Bytes,
    patch: Bytes,
    mutations: any,
    updatedAtMs?: number,
  ): WasmWrite {
    const row = rowUuidFromBytes(rowId);
    const p = decodeCells(patch);
    const m = largeValueUpdates(mutations);
    const at = present(updatedAtMs) ? checkedJsU64(updatedAtMs!, "updatedAtMs") : undefined;
    let db: any;
    const write = this.enqueue(
      (i) => ((db = i.db), i.db.enqueueLargeValueUpdate(table, row, p, m, at)),
    );
    return wasmWrite(db, write);
  }

  upsert(table: string, rowId: Bytes, cells: Bytes, options: any): WasmWrite {
    const row = rowUuidFromBytes(rowId);
    const c = decodeCells(cells);
    const o = upsertOptionsFromJs(options);
    let db: any;
    const write = this.enqueue((i) => ((db = i.db), i.db.enqueueUpsert(table, row, c, o)));
    return wasmWrite(db, write);
  }

  upsertInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Bytes,
    cells: Bytes,
    options: any,
  ): void {
    const id = parseOpenTx(openTransactionId);
    const row = rowUuidFromBytes(rowId);
    const c = decodeCells(cells);
    const o = upsertOptionsFromJs(options);
    this.enqueue((i) => i.db.enqueueTransactionUpsert(id, table, row, c, o));
  }

  delete(table: string, rowId: Bytes, options: any): WasmWrite {
    const row = rowUuidFromBytes(rowId);
    const o = deleteOptionsFromJs(options);
    let db: any;
    const write = this.enqueue((i) => ((db = i.db), i.db.enqueueDelete(table, row, o)));
    return wasmWrite(db, write);
  }

  deleteInTransaction(openTransactionId: string, table: string, rowId: Bytes, options: any): void {
    const id = parseOpenTx(openTransactionId);
    const row = rowUuidFromBytes(rowId);
    const o = deleteOptionsFromJs(options);
    this.enqueue((i) => i.db.enqueueTransactionDelete(id, table, row, o));
  }

  restore(table: string, rowId: Bytes, cells: Bytes, options: any): WasmWrite {
    const row = rowUuidFromBytes(rowId);
    const c = decodeCells(cells);
    const o = restoreOptionsFromJs(options);
    let db: any;
    const write = this.enqueue((i) => ((db = i.db), i.db.enqueueRestore(table, row, c, o)));
    return wasmWrite(db, write);
  }

  restoreInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Bytes,
    cells: Bytes,
    options: any,
  ): void {
    const id = parseOpenTx(openTransactionId);
    const row = rowUuidFromBytes(rowId);
    const c = decodeCells(cells);
    const o = restoreOptionsFromJs(options);
    this.enqueue((i) => i.db.enqueueTransactionRestore(id, table, row, c, o));
  }

  /** Every runtime this binding opens defers local persistence to the host's flush cadence. */
  private static runtimeOwner(inner: WasmDbInner, trustedBackend: boolean): WasmDb {
    lifecycle.setDeferredLocalPersistence(inner.db, true);
    return new WasmDb(inner, true, { value: false }, trustedBackend);
  }

  private static async openBrowserRuntime(
    pageStore: any,
    schema: any,
    config: WasmOpenDbConfig,
    storageOwner: string,
  ): Promise<WasmDb> {
    try {
      const storage = await IdbStorage.open(
        new IndexedDbPageStore(pageStore),
        schema.columnFamilies(),
      );
      const db = await openScopeIsolatedRelayDb(schema, storage, config, storageOwner);
      await lifecycle.restoreBrowserRelayPendingUploads(db);
      return WasmDb.runtimeOwner(new WasmDbInner(db, false), false);
    } catch (e) {
      throw toJsError(e);
    }
  }

  static async openMemory(schemaBytes: Bytes, configBytes: Bytes): Promise<WasmDb> {
    initializeWasmTracing();
    const [schema, config] = decodeOpenArgs(schemaBytes, configBytes);
    validateUntrustedOpenAuthor(config);
    const db = await openDb(schema, new MemoryStorage(schema.columnFamilies()), config).catch(
      rethrowJs,
    );
    return WasmDb.runtimeOwner(new WasmDbInner(db, true), false);
  }

  /** A separate ABI from `openMemory`: raw-open config can never select the system author. */
  static async openMemoryAsBackend(schemaBytes: Bytes, configBytes: Bytes): Promise<WasmDb> {
    initializeWasmTracing();
    const [schema, config] = decodeOpenArgs(schemaBytes, configBytes);
    const identity = backendOpenIdentity(config);
    const db = await openBackendDb(
      schema,
      new MemoryStorage(schema.columnFamilies()),
      config,
      identity,
    ).catch(rethrowJs);
    return WasmDb.runtimeOwner(new WasmDbInner(db, true), true);
  }

  static async openMemoryWithSelfSignedProof(
    schemaBytes: Bytes,
    configBytes: Bytes,
    token: string,
    appId: string,
    claimedAuthor: string,
  ): Promise<WasmDb> {
    initializeWasmTracing();
    const [schema, config] = decodeOpenArgs(schemaBytes, configBytes);
    config.identity.author = verifySelfSignedRuntimeAuthor(token, appId, claimedAuthor);
    const db = await openDb(schema, new MemoryStorage(schema.columnFamilies()), config).catch(
      rethrowJs,
    );
    return WasmDb.runtimeOwner(new WasmDbInner(db, true), false);
  }

  static openBrowser(pageStore: any, schemaBytes: Bytes, configBytes: Bytes, storageOwner: string) {
    initializeWasmTracing();
    const [schema, config] = decodeOpenArgs(schemaBytes, configBytes);
    validateUntrustedOpenAuthor(config);
    return WasmDb.openBrowserRuntime(pageStore, schema, config, storageOwner);
  }

  static openBrowserWithSelfSignedProof(
    pageStore: any,
    schemaBytes: Bytes,
    configBytes: Bytes,
    token: string,
    appId: string,
    claimedAuthor: string,
    storageOwner: string,
  ) {
    initializeWasmTracing();
    const [schema, config] = decodeOpenArgs(schemaBytes, configBytes);
    config.identity.author = verifySelfSignedRuntimeAuthor(token, appId, claimedAuthor);
    return WasmDb.openBrowserRuntime(pageStore, schema, config, storageOwner);
  }

  async registerSchema(schema: Bytes): Promise<WasmDb> {
    const view = await this.openInner().registerSchemaView(decodePublicSchema(schema));
    return new WasmDb(view, false, this.nonDurableClient, this.trustedBackend);
  }

  beginTransaction(
    openTransactionId: string,
    kind: string,
    author?: Bytes,
    attribution?: Bytes,
  ): void {
    const id = parseOpenTx(openTransactionId);
    const a = present(author) ? authorIdFromBytes(author!) : undefined;
    const at = present(attribution) ? authorIdFromBytes(attribution!) : undefined;
    if (a !== undefined && at !== undefined)
      throw "transaction identity cannot contain both author and attribution";
    if (at !== undefined) this.requireTrustedBackend();
    const inner = this.openInner();
    if (kind !== "mergeable" && kind !== "exclusive") throw unknownTransactionKindMessage(kind);
    try {
      inner.begin(kind, id, a, at);
    } catch (e) {
      throw toJsError(e);
    }
  }

  commitTransaction(openTransactionId: string, kind?: string): WasmWrite {
    const id = parseOpenTx(openTransactionId);
    const inner = this.openInner();
    const k = kind ?? "mergeable";
    if (k !== "mergeable" && k !== "exclusive") throw unknownTransactionKindMessage(k);
    return inner.commit(k, id);
  }

  rollbackTransaction(openTransactionId: string): void {
    const id = parseOpenTx(openTransactionId);
    try {
      this.openInner().abandonTransaction(id);
    } catch (e) {
      throw toJsError(e);
    }
  }

  /** Exact local state for the write-merge bridge; not a query. */
  async localCurrentRow(table: string, rowId: Bytes): Promise<Bytes> {
    const row = rowUuidFromBytes(rowId);
    const inner = this.openInner();
    const current = await inner.db.localCurrentRow(table, row).catch(rethrowJs);
    return encodeSynchronousRows(current ? [current] : []);
  }

  async all(
    query: Bytes,
    opts: any,
    openTransactionId?: string,
    author?: Bytes,
    claims?: any,
  ): Promise<Bytes> {
    const inner = this.openInner();
    const tierIsExplicit = present(opts) && optionalStringProp(opts, "tier") !== undefined;
    const readOpts = readOptsFromJs(opts);
    const hasExplicitAuthor = present(author);
    const resolvedAuthor = this.readAuthor(author);
    const admission =
      hasExplicitAuthor && resolvedAuthor !== undefined
        ? [resolvedAuthor, claimsFromJs(resolvedAuthor, claims)]
        : undefined;
    const openTx = present(openTransactionId) ? parseOpenTx(openTransactionId!) : undefined;
    const nonDurableClient = this.nonDurableClient.value;
    if (openTx === undefined) {
      try {
        await lifecycle.queuedMutationBarrier(inner.db);
      } catch (e) {
        if (e === CANCELLED) throw "local write ordering barrier was cancelled";
        throw toJsError(e);
      }
    }
    const requiresCoverage = tierIsExplicit && (nonDurableClient || readOpts.tier === "Remote");
    const result = await inner
      .allSerializedQuery(
        query,
        readOpts,
        openTx,
        admission,
        resolvedAuthor,
        requiresCoverage,
        Date.now() + 15_000,
      )
      .catch(rethrowJs);
    try {
      return result.kind === "Rows"
        ? encodeRows(result.rows)
        : encodeRelationSnapshot(result.snapshot);
    } catch (e) {
      throw toJsError(e);
    }
  }

  /** Bind correlation claims to this already-open client's own identity. */
  async setSessionClaims(claims: any): Promise<void> {
    const inner = this.openInner();
    const identity = lifecycle.identity(inner.db).author;
    await inner.db.setIdentityClaimsAsync(identity, claimsFromJs(identity, claims));
  }

  setIdentityClaims(author: Bytes, claims: any): void {
    const a = authorIdFromBytes(author);
    const c = claimsFromJs(a, claims);
    this.openInner().db.setIdentityClaims(a, c);
  }

  async subscribe(query: Bytes, opts: any, author?: Bytes, claims?: any): Promise<ReadableStream> {
    const readOpts = readOptsFromJs(opts);
    const hasExplicitAuthor = present(author);
    const resolvedAuthor = this.readAuthor(author);
    const admission =
      hasExplicitAuthor && resolvedAuthor !== undefined
        ? [resolvedAuthor, claimsFromJs(resolvedAuthor, claims)]
        : undefined;
    const db = this.openInner();
    const authorization =
      resolvedAuthor !== undefined
        ? { kind: "TrustedServing", author: resolvedAuthor }
        : { kind: "ClientLocal" };
    const stream = await db.db
      .subscribeSerializedQuery(query, readOpts, admission, authorization)
      .catch(rethrowJs);
    return subscriptionStreamToJs(db, stream);
  }

  setTickScheduler(callback: (urgency: string) => void): void {
    this.inner?.setTickScheduler(callback);
  }

  /** Register a callback for rejected writes that no active wait consumed. */
  onMutationError(callback: (event: any) => void): void {
    if (this.inner) lifecycle.onMutationError(this.inner.db, (event: any) => callback(event));
  }

  canInsert(table: string, cells: Bytes): string {
    const c = decodeCells(cells);
    const inner = this.openInner();
    try {
      return adviceString(inner.db.canInsert(table, c));
    } catch (e) {
      throw toJsError(e);
    }
  }

  requestInsertPermissionAdvice(table: string, cells: Bytes) {
    const action = { kind: "Insert", table, cells: decodeCells(cells) };
    return this.openInner().requestPermissionAdvice(action);
  }
  requestReadPermissionAdvice(table: string, rowId: Bytes) {
    const action = { kind: "Read", table, row: rowUuidFromBytes(rowId) };
    return this.openInner().requestPermissionAdvice(action);
  }
  requestUpdatePermissionAdvice(table: string, rowId: Bytes, patch: Bytes) {
    const action = {
      kind: "Update",
      table,
      row: rowUuidFromBytes(rowId),
      patch: decodeCells(patch),
    };
    return this.openInner().requestPermissionAdvice(action);
  }
  requestDeletePermissionAdvice(table: string, rowId: Bytes) {
    const action = { kind: "Delete", table, row: rowUuidFromBytes(rowId) };
    return this.openInner().requestPermissionAdvice(action);
  }

  async waitForPendingWrites(tier: string): Promise<void> {
    const inner = this.openInner();
    await lifecycle.waitForPendingWrites(inner.db, durabilityTierFromStr(tier)).catch(rethrowJs);
  }

  async tick(): Promise<void> {
    const inner = this.openInner();
    await lifecycle.tick(inner.db).catch(rethrowJs);
  }

  /** Configure Jazz-owned upload ingress and unpublished-tree expiry limits. */
  setLargeValueStagingPolicy(
    incomingBytesPerWindow: number,
    windowMs: number,
    maxAgeMs?: number,
  ): void {
    const incoming = checkedJsU64(incomingBytesPerWindow, "incomingBytesPerWindow");
    const window = checkedJsU64(windowMs, "windowMs");
    if (window < 1) throw "windowMs must be at least 1";
    const maxAge = present(maxAgeMs)
      ? checkedJsU64(maxAgeMs!, "maxAgeMs")
      : defaultLargeValueStagingMaxAgeMs;
    lifecycle.setLargeValueStagingPolicy(this.openInner().db, {
      incomingBytesPerWindow: incoming,
      windowMs: window,
      maxAgeMs: maxAge,
    });
  }

  async evictExpiredStagedLargeValues(): Promise<number> {
    const inner = this.openInner();
    return lifecycle.evictExpiredStagedLargeValues(inner.db).catch(rethrowJs);
  }

  beginStreamingMutation(
    table: string,
    rowId: Bytes,
    cells: Bytes,
    column: string,
    mutation?: string,
    author?: Bytes,
    attribution?: Bytes,
    updatedAtMs?: number,
    head?: any,
    base?: any,
  ): StreamingMutation {
    if (present(author) && present(attribution))
      throw "streaming mutation identity cannot contain both author and attribution";
    if (present(attribution) && (present(head) || present(base)))
      throw "backend-attributed streaming mutations do not support branch writes";
    if (present(attribution)) this.requireTrustedBackend();
    const identity: WriteIdentity = present(author)
      ? { kind: "Session", author: authorIdFromBytes(author!) }
      : present(attribution)
        ? { kind: "Attribution", author: authorIdFromBytes(attribution!) }
        : { kind: "Database" };
    const row = rowUuidFromBytes(rowId);
    const c = decodeCells(cells);
    const kinds: Record<string, StreamingState["mutation"]> = {
      insert: "Insert",
      update: "Update",
      upsert: "Upsert",
    };
    const kind = kinds[mutation ?? "insert"];
    if (!kind) throw "unknown streaming mutation kind";
    const at = present(updatedAtMs) ? checkedJsU64(updatedAtMs!, "updatedAtMs") : undefined;
    const h = present(head) ? head : undefined;
    const b = present(base) ? base : undefined;
    if (b !== undefined && h === undefined) throw "streaming mutation base requires a branch head";
    const inner = this.openInner();
    let upload;
    try {
      upload = inner.db.beginStreamingValueUpload(table, c, column);
    } catch (e) {
      throw toJsError(e);
    }
    return new StreamingMutation({
      kind: "Open",
      state: {
        db: inner,
        upload,
        mutation: kind,
        table,
        rowId: row,
        cells: c,
        column,
        identity,
        updatedAtMs: at,
        head: h,
        base: b,
      },
    });
  }

  /** Optimistic in-memory side of a browser client/worker pair. */
  setNonDurableClient(): void {
    lifecycle.setNonDurableClient(this.openInner().db);
    this.nonDurableClient.value = true;
  }

  setRemoteLinkHint(state: string): void {
    const hint = remoteLinkHintFromHostStr(state);
    if (hint === undefined) throw `unknown remote link state ${state}`;
    this.openInner().db.setRemoteLinkHint(hint);
  }

  async foregroundTxTimeHighWater(): Promise<bigint> {
    const inner = this.openInner();
    return BigInt((await lifecycle.foregroundTxTimeHighWater(inner.db)).value);
  }

  async seedForegroundTxTimeHighWater(highWater: bigint): Promise<void> {
    const inner = this.openInner();
    await lifecycle.seedForegroundTxTimeHighWater(inner.db, { value: highWater });
  }

  async connectUpstream(): Promise<WasmTransport> {
    const queues = new WasmWireQueues();
    // Browser carriers negotiate ordinary sync only, without scoped receipt/view frames.
    const transport = new WireTransportAdapter(
      wasmWireTransport(queues),
      WIRE_PROTOCOL_VERSION,
      currentWireFeatures() & ~FEATURE_AUTHORIZATION_SCOPE_VIEWS,
    );
    const dbInner = this.openInner();
    return WasmTransport.create(
      dbInner.db,
      await lifecycle.connectUpstream(dbInner.db, transport),
      queues,
    );
  }

  /** Connect after the browser carrier has accepted the server Hello. */
  async connectUpstreamWithSession(
    protocolVersion: number,
    features: number,
    remoteNode: Bytes,
    remoteEpoch: bigint,
    localNode: Bytes,
    localEpoch: bigint,
  ): Promise<WasmTransport> {
    const dbInner = this.openInner();
    if (remoteNode.length !== 16) throw "server hello authority node must be 16 bytes";
    if (localNode.length !== 16) throw "local peer identity must be 16 bytes";
    const queues = new WasmWireQueues();
    const sessionContext = {
      local: { node: NodeUuid.fromBytes(localNode), epoch: localEpoch },
      remote: { node: NodeUuid.fromBytes(remoteNode), epoch: remoteEpoch },
      linkIdentity: lifecycle.identity(dbInner.db).author,
      negotiatedFeatures: features,
    };
    const transport = new WireTransportAdapter(
      wasmWireTransport(queues),
      protocolVersion,
      features,
      undefined,
      sessionContext,
    );
    return WasmTransport.create(
      dbInner.db,
      await lifecycle.connectUpstream(dbInner.db, transport),
      queues,
    );
  }

  /** Admit a subscriber after pending local writes have been recovered. */
  acceptSubscriber(identity: Bytes, claims: any, localEpoch?: bigint): Promise<WasmTransport> {
    const id = authorIdFromBytes(identity);
    const c = claimsFromJs(id, claims);
    return WasmDb.acceptAdmitted(this.openInner(), id, c, localEpoch);
  }

  /** Attach a browser-local follower for a verified first-party identity. */
  acceptSubscriberWithSelfSignedProof(
    claims: any,
    token: string,
    appId: string,
    claimedAuthor: string,
    localEpoch?: bigint,
  ): Promise<WasmTransport> {
    const id = verifySelfSignedRuntimeAuthor(token, appId, claimedAuthor);
    const c = claimsFromJs(id, claims);
    return WasmDb.acceptAdmitted(this.openInner(), id, c, localEpoch);
  }

  private static async acceptAdmitted(
    dbInner: WasmDbInner,
    identity: any,
    claims: any,
    localEpoch?: bigint,
  ): Promise<WasmTransport> {
    const queues = new WasmWireQueues();
    const owner = lifecycle.identity(dbInner.db);
    const context = present(localEpoch)
      ? {
          local: { node: owner.node, epoch: localEpoch },
          remote: undefined,
          linkIdentity: identity,
          negotiatedFeatures: currentWireFeatures(),
        }
      : undefined;
    const features = context
      ? currentWireFeatures()
      : currentWireFeatures() & ~FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    const transport = new WireTransportAdapter(
      wasmWireTransport(queues),
      WIRE_PROTOCOL_VERSION,
      features,
      undefined,
      context,
    );
    const connection = await dbInner.db
      .acceptSubscriberWithClaimsAsync(transport, identity, claims)
      .catch(rethrowJs);
    return WasmTransport.create(dbInner.db, connection, queues, identity);
  }

  /** A close failure still consumes the binding; physical close happens exactly once. */
  async close(): Promise<boolean> {
    const inner = this.inner;
    if (!inner) return false;
    this.inner = undefined;
    if (this.ownsRuntime) await lifecycle.close(inner.db).catch(rethrowJs);
    return true;
  }
}

// ---------------------------------------------------------------- transport

export class WasmTransport {
  private constructor(
    private db: any,
    private connection: any,
    private queues: WasmWireQueues,
    private auxiliaryPump: any,
    private subscriberIdentity?: any,
  ) {}

  static async create(db: any, connection: any, queues: WasmWireQueues, subscriberIdentity?: any) {
    const pump = (await connection.lock()).ioPump();
    return new WasmTransport(db, connection, queues, pump, subscriberIdentity);
  }

  async updateAuthenticatedClaims(claims: any): Promise<void> {
    if (this.subscriberIdentity === undefined) throw "transport is not a subscriber link";
    const c = claimsFromJs(this.subscriberIdentity, claims);
    if (!this.connection) throw "subscriber transport is closed";
    (await this.connection.lock()).updateAuthenticatedSessionClaims(c);
  }

  /** Route one socket frame through the chunk lane; returns it when it belongs to the Jazz lane. */
  routeAuxiliaryWireFrame(frame: Bytes): Promise<Bytes | undefined> {
    return this.auxiliaryPump.routeIncomingWireFrame(frame);
  }

  recvAuxiliaryWireFrames(maxFrames?: number, maxBytes?: number): Bytes[] {
    return this.auxiliaryPump.takeOutboundWireFrames(
      Math.max(maxFrames ?? 1, 1),
      Math.max(maxBytes ?? MAX_WIRE_FRAME_BYTES, 1),
    );
  }

  auxiliaryReceiveTimeoutMs(): number | undefined {
    const delay = this.auxiliaryPump.incompleteReceiveTimeoutMs();
    return delay === undefined ? undefined : Math.min(delay, 0xffffffff);
  }

  /** Retire only this connection when a partial channel exceeds its deadline. */
  expireAuxiliaryReceive(): void {
    try {
      this.auxiliaryPump.expireIncompleteReceive();
    } catch (error) {
      this.auxiliaryPump.disconnect();
      throw error;
    }
  }

  auxiliaryOutboundReady(): Promise<void> {
    return this.auxiliaryPump.outboundReady();
  }

  /** Redacted chunk-relay flight recorder; ids stay strings to keep the full u64. */
  takeAuxiliaryTrace(): object[] {
    return this.auxiliaryPump.takeTrace().map((t: any) => {
      const entry: Record<string, unknown> = {
        event: t.event,
        role: t.role,
        connection: String(t.connection),
        requestId: String(t.requestId),
        remainingHops: t.remainingHops,
        objectHash: t.objectHash,
        locatorFingerprint: t.locatorFingerprint,
      };
      if (t.response !== undefined) entry.response = t.response;
      if (t.storageError !== undefined) entry.storageError = t.storageError;
      return entry;
    });
  }

  setAuxiliaryTraceEnabled(enabled: boolean): void {
    this.auxiliaryPump.setTraceEnabled(enabled);
  }

  sendWireFrame(frame: Bytes): void {
    this.queues.inbound.push(frame);
  }
  sendWireFrames(frames: Bytes[]): void {
    for (const frame of frames) this.queues.inbound.push(new Uint8Array(frame));
  }
  recvWireFrames(): Bytes[] {
    return this.queues.outbound.splice(0);
  }
  setOutboundScheduler(callback: () => void): void {
    this.queues.outboundScheduler = callback;
  }
  clearOutboundScheduler(): void {
    this.queues.outboundScheduler = undefined;
  }

  async tick(): Promise<number> {
    if (!this.connection) return 0;
    const stats = await (await this.connection.lock()).tick().catch(rethrowJs);
    return stats.subscriptionEvents;
  }

  close(): boolean {
    this.auxiliaryPump.disconnect();
    const connection = this.connection;
    if (!connection) return false;
    this.connection = undefined;
    return this.db.detachConnection(connection);
  }
}

// ---------------------------------------------------------------- argument parsing

type WriteIdentity =
  | { kind: "Session"; author: any }
  | { kind: "Attribution"; author: any }
  | { kind: "Database" };

const parseOpenTx = (id: string) => parseOpenTransactionId(id); // throws the parse string

function decodeCells(bytes: Bytes): any {
  try {
    return decodeNamedCells(bytes);
  } catch (error) {
    throw `decode cells: ${error}`;
  }
}

function largeValueUpdates(mutations: any): any[] {
  if (!Array.isArray(mutations))
    throw `invalid partial-value update descriptor: invalid type: expected a sequence`;
  return mutations;
}

function writeOption(options: any, name: string): any {
  if (!present(options)) return undefined;
  const value = Reflect.get(options, name);
  return present(value) ? value : undefined;
}

/** Removed fields are rejected by presence, so `{ branch: undefined }` cannot slip through. */
const hasWriteOption = (options: any, name: string): boolean =>
  present(options) && Reflect.has(options, name);

function writeIdentityOption(options: any): WriteIdentity {
  const author = writeOption(options, "author");
  const attribution = writeOption(options, "attribution");
  if (author !== undefined && attribution !== undefined)
    throw "write identity cannot contain both author and attribution";
  if (author !== undefined)
    return { kind: "Session", author: authorIdFromBytes(new Uint8Array(author)) };
  if (attribution !== undefined)
    return { kind: "Attribution", author: authorIdFromBytes(new Uint8Array(attribution)) };
  return { kind: "Database" };
}

function writeTimestampOption(options: any): number | undefined {
  const value = writeOption(options, "updatedAtMs");
  if (value === undefined) return undefined;
  if (typeof value !== "number") throw "updatedAtMs must be a number";
  return checkedJsU64(value, "updatedAtMs");
}

function exactWriteTarget(options: any): any {
  const branch = writeOption(options, "branch");
  return branch === undefined ? { kind: "Default" } : { kind: "Branch", branch };
}

function branchViewTarget(options: any): any {
  const head = writeOption(options, "head");
  const base = writeOption(options, "base");
  if (head !== undefined) return { kind: "BranchView", head, base };
  if (base !== undefined) throw "branch view base requires a head selector";
  return { kind: "Default" };
}

const insertOptionsFromJs = (options: any) => ({
  rowId:
    writeOption(options, "rowId") !== undefined
      ? rowUuidFromBytes(new Uint8Array(writeOption(options, "rowId")))
      : undefined,
  identity: writeIdentityOption(options),
  target: exactWriteTarget(options),
  updatedAtMs: writeTimestampOption(options),
});

function updateOptionsFromJs(options: any) {
  const target = branchViewTarget(options);
  return {
    identity: writeIdentityOption(options),
    target,
    updatedAtMs: writeTimestampOption(options),
  };
}

function upsertOptionsFromJs(options: any) {
  if (hasWriteOption(options, "branch"))
    throw "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view";
  return updateOptionsFromJs(options);
}

const deleteOptionsFromJs = updateOptionsFromJs;

const restoreOptionsFromJs = (options: any) => ({
  identity: writeIdentityOption(options),
  target: exactWriteTarget(options),
  updatedAtMs: writeTimestampOption(options),
});

function decodeOpenArgs(schema: Bytes, config: Bytes): [any, WasmOpenDbConfig] {
  const s = decodePublicSchema(schema);
  try {
    const r = new Reader(config);
    const c = decodeOpenDbConfig(r);
    r.finish();
    return [s, c];
  } catch (err) {
    throw toJsError(`decode open config: ${err}`);
  }
}

function decodePublicSchema(schema: Bytes): any {
  try {
    return decodePublicSchemaJson(schema);
  } catch (e) {
    throw toJsError(e);
  }
}

function dbConfig(schema: any, storage: any, config: WasmOpenDbConfig, identity: any): any {
  const c = new DbConfig(schema, storage, identity);
  return config.rowIdSeed !== undefined
    ? c.withIdSource(new SeededRowIdSource(config.rowIdSeed))
    : c;
}

async function openDb(schema: any, storage: any, config: WasmOpenDbConfig): Promise<any> {
  const c = dbConfig(schema, storage, config, config.identity);
  const db = config.historyComplete
    ? await lifecycle.openHistoryComplete(c)
    : await lifecycle.open(c);
  configureInitialSyncFlushCadence(db, config.initialSyncFlushEvery);
  return db;
}

/** Bind the relay capability during construction so app-facing handles never toggle it later. */
async function openScopeIsolatedRelayDb(
  schema: any,
  storage: any,
  config: WasmOpenDbConfig,
  storageOwner: string,
): Promise<any> {
  const c = dbConfig(schema, storage, config, config.identity);
  const scope = ClientRelayScope.fromAdmittedStorageOwner(storageOwner, config.identity.author);
  const db = await lifecycle.openScopeIsolatedClientRelay(c, scope);
  configureInitialSyncFlushCadence(db, config.initialSyncFlushEvery);
  return db;
}

async function openBackendDb(
  schema: any,
  storage: any,
  config: WasmOpenDbConfig,
  identity: any,
): Promise<any> {
  const c = dbConfig(schema, storage, config, identity);
  const db = config.historyComplete
    ? await lifecycle.openHistoryCompleteWithBackendAttribution(c)
    : await lifecycle.openWithBackendAttribution(c);
  configureInitialSyncFlushCadence(db, config.initialSyncFlushEvery);
  return db;
}

function validateUntrustedOpenAuthor(config: WasmOpenDbConfig): void {
  try {
    AuthorSubject.fromUntrustedCanonical(config.identity.author.canonical());
  } catch (error) {
    throw String(error);
  }
}

function backendOpenIdentity(config: WasmOpenDbConfig) {
  validateUntrustedOpenAuthor(config);
  return { node: config.identity.node, author: AuthorSubject.SYSTEM };
}

/** `SystemTime` panics under wasm32, so the browser clock is passed explicitly. */
const verifySelfSignedRuntimeAuthor = (token: string, appId: string, claimedAuthor: string) =>
  verifyClientRuntimeAuthorAt(token, appId, claimedAuthor, Math.floor(Date.now() / 1000));

function configureInitialSyncFlushCadence(db: any, every: number | undefined): void {
  if (!every) return;
  lifecycle.setInitialSyncFlushCadence(db, InitialSyncFlushCadence.every(every));
}

const rowUuidFromBytes = (bytes: Bytes) => bindingArgs.rowUuid(bytes);
const checkedJsU64 = (value: number, name: string): number => bindingArgs.checkedU64(value, name);
const authorIdFromBytes = (bytes: Bytes) => bindingArgs.authorSubject(bytes);
const claimsFromJs = (author: any, claims: any) => bindingArgs.policyClaims(author, claims);
const durabilityTierFromStr = (tier: string) => bindingArgs.durabilityTier(tier);
const readTierFromStr = (tier: string) => bindingArgs.readTier(tier);

function readOptsFromJs(value: any): any {
  const opts = defaultReadOpts();
  if (!present(value)) return opts;
  for (const name of ["read_view", "readView"]) {
    const prop = value[name];
    if (present(prop)) {
      if (typeof prop !== "object") throw `invalid read_view: invalid type: expected a map`;
      opts.readView = prop;
      break;
    }
  }
  const tier = optionalStringProp(value, "tier");
  if (tier !== undefined) opts.tier = readTierFromStr(tier);
  const includeDeleted = optionalBoolProp(value, "include_deleted");
  if (includeDeleted !== undefined) opts.includeDeleted = includeDeleted;
  const timeoutMs = optionalWaitMsProp(value, "first_load_remote_wait_ms");
  if (timeoutMs !== undefined) opts.firstLoad = localFirstServerWait(opts, timeoutMs);
  return opts;
}

/** A local-first read's server-wait timeout. `Remote` reads ignore it. */
const localFirstServerWait = (opts: any, timeoutMs: number): FirstLoad =>
  opts.tier === "LocalFirst" ? { kind: "WaitForRemote", timeoutMs } : opts.firstLoad;

function optionalWaitMsProp(value: any, name: string): number | undefined {
  const prop = value[name];
  if (!present(prop)) return undefined;
  if (typeof prop === "number" && Number.isFinite(prop) && prop >= 0) return Math.floor(prop);
  throw `${name} must be a non-negative number of milliseconds`;
}

function optionalStringProp(value: any, name: string): string | undefined {
  const prop = value[name];
  if (!present(prop)) return undefined;
  if (typeof prop !== "string") throw `${name} must be a string`;
  return prop;
}

function optionalBoolProp(value: any, name: string): boolean | undefined {
  if (!present(value)) return undefined;
  const prop = value[name];
  if (!present(prop)) return undefined;
  if (typeof prop !== "boolean") throw `${name} must be a boolean`;
  return prop;
}

function encodeSynchronousRows(rows: any[]): Bytes {
  for (const row of rows) {
    const [descriptor, raw] = row.encodedRecord();
    let values: any[];
    try {
      values = descriptor.bind(raw).toValues();
    } catch (e) {
      throw toJsError(e);
    }
    if (values.some(valueContainsIndirectScalar))
      throw "synchronous WASM all/transaction reads cannot materialize a large value; use an async relation read or subscription instead";
  }
  try {
    return encodeRows(rows);
  } catch (e) {
    throw toJsError(e);
  }
}

const valueContainsIndirectScalar = (value: any): boolean =>
  value.kind === "Large" ||
  (value.kind === "Nullable" &&
    value.value !== undefined &&
    valueContainsIndirectScalar(value.value));

// ---------------------------------------------------------------- subscriptions

const INITIAL_SUBSCRIPTION_RETRY_MS = 25;
const MAX_SUBSCRIPTION_RETRY_MS = 1_000;
// Hosts clamp setTimeout delays above i32::MAX to near-immediate; segment below it.
const MAX_JS_TIMEOUT_MS = 0x7fffffff;

const localRetryDelayMs = (attempt: number) =>
  Math.min(INITIAL_SUBSCRIPTION_RETRY_MS * 2 ** Math.min(attempt, 6), MAX_SUBSCRIPTION_RETRY_MS);

/** The peer's retry hint is a minimum, not a suggestion to cap. */
const subscriptionRetryDelayMs = (attempt: number, peerRetryAfterMs: number) =>
  Math.max(localRetryDelayMs(attempt), peerRetryAfterMs);

function subscriptionRetryTimerSegments(delayMs: number): number[] {
  const segments: number[] = [];
  while (delayMs > 0) {
    const segment = Math.min(delayMs, MAX_JS_TIMEOUT_MS);
    segments.push(segment);
    delayMs -= segment;
  }
  return segments;
}

/** One host timer per segment; cancelling the pull clears it immediately. */
async function waitForSubscriptionRetry(delayMs: number, signal: AbortSignal): Promise<void> {
  for (const segment of subscriptionRetryTimerSegments(delayMs)) {
    await new Promise<void>((resolve, reject) => {
      const handle = setTimeout(resolve, segment);
      signal.addEventListener(
        "abort",
        () => (clearTimeout(handle), reject("subscription pull was aborted")),
        {
          once: true,
        },
      );
    });
  }
}

function subscriptionStreamToJs(db: WasmDbInner, source: any): ReadableStream {
  const iterator: AsyncIterator<any> = source[Symbol.asyncIterator]();
  const abort = new AbortController();
  let pulling = false;
  let cancelled = false;
  return new ReadableStream(
    {
      async pull(controller) {
        if (cancelled) return;
        if (pulling) throw "subscription stream pull already in progress";
        pulling = true;
        try {
          const next = await iterator.next();
          if (cancelled) return;
          if (next.done) return controller.close();
          const event = next.value;
          for (let attempt = 0; ; attempt++) {
            const outcome = await db.db.hydrateSubscriptionEventForBindingOutcome(event);
            if (outcome.kind === "Ok") break;
            if (outcome.kind === "Error") {
              // Surfacing the error drops the subscription and runs its cleanup.
              const error = toJsError(outcome.error);
              controller.error(error);
              throw error;
            }
            // Keep the event ahead of the source; a fulfilled pull without
            // enqueueing does not re-pull at HWM 0, so wait for a real timer.
            await waitForSubscriptionRetry(
              subscriptionRetryDelayMs(attempt, outcome.retryAfterMs),
              abort.signal,
            );
          }
          controller.enqueue(subscriptionChunkToJs(event));
        } finally {
          pulling = false;
        }
      },
      cancel() {
        cancelled = true;
        abort.abort();
        iterator.return?.();
      },
    },
    { highWaterMark: 0 },
  );
}

function subscriptionChunkToJs(event: any): object {
  switch (event.kind) {
    case "Delta": {
      const delta = encodeSubscriptionDelta(event.added, event.updated, event.removed);
      if (event.terminalOperations.some((op: any) => op.path.length === 0))
        throw "native producer emitted a root terminal operation";
      return {
        type: "delta",
        delta,
        terminalOperations: terminalOperationsToJs(event.terminalOperations),
        reset: event.reset,
        settled: event.settled,
        tier: String(event.tier),
      };
    }
    case "Closed":
      return { type: "closed" };
    case "Rejected": {
      const r = event.reason;
      // ShapeRegistrationPendingCatalogueAdmission is transient, distinct from
      // the permanent UnsupportedShapeCapability.
      const reason =
        r.kind === "UnsupportedShapeCapability"
          ? { type: r.kind, detail: r.detail }
          : r.kind === "ServerFailure"
            ? { type: r.kind, code: String(r.code) }
            : r.kind === "InvalidAuthoritySourceClosure"
              ? { type: r.kind, transition: r.transition }
              : { type: "ShapeRegistrationPendingCatalogueAdmission" };
      return { type: "rejected", reason };
    }
    default:
      throw new Error(`unknown subscription event ${event.kind}`);
  }
}

/** Terminal operations with every key and payload as one Uint8Array (#3369). */
function terminalOperationsToJs(operations: any[]): object[] {
  return operations.map((op) => ({
    root_key: op.rootKey,
    path: op.path.map((s: any) =>
      s.kind === "Collection" ? { Collection: s.name } : { Key: s.key },
    ),
    edit:
      op.edit.kind === "Insert"
        ? { Insert: { index: op.edit.index, key: op.edit.key, value: op.edit.value } }
        : op.edit.kind === "Update"
          ? { Update: { key: op.edit.key, value: op.edit.value } }
          : op.edit.kind === "Remove"
            ? { Remove: { key: op.edit.key } }
            : { Move: { key: op.edit.key, index: op.edit.index } },
  }));
}

// ---------------------------------------------------------------- errors

/** A core error becomes a JS Error with a stable `code`; everything else stays a bare string. */
function toJsError(error: unknown): unknown {
  if (error instanceof JazzDbError) {
    const js = new Error(String((error as any).message));
    (js as any).code = (error as any).codeStr();
    return js;
  }
  return error instanceof Error ? error.message : String(error);
}

const rethrowJs = (e: unknown): never => {
  throw toJsError(e);
};

const unknownTransactionKindMessage = (kind: string) => `unknown transaction kind ${kind}`;
