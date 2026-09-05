declare module "jazz-wasm" {
  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
  export function nativeArtifactFingerprint(): string;
  export function generateId(): string;
  export function currentTimestamp(): bigint;
  export function deriveUserId(seedB64: string): string;
  export function mintLocalFirstToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
  export function mintAnonymousToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;

  export class WasmPreparedQuery {}
  export class QueryAttachment {}
  export class WasmPermissionAdviceRequest {
    readonly promise: Promise<"allowed" | "denied" | "unknown">;
    cancel(): void;
  }

  export class WasmWrite {
    readonly txId: string;
    readonly payload: Uint8Array;
    readonly rowId: Uint8Array;
    writeState(): unknown;
    wait(tier: string): Promise<void>;
    close(): boolean;
  }

  export class StreamingMutation {
    push(chunk: Uint8Array): Promise<void>;
    finish(): Promise<WasmWrite>;
    abort(): Promise<boolean>;
  }

  export class WasmTransport {
    sendWireFrame(frame: Uint8Array): void;
    recvWireFrames(): Uint8Array[];
    routeAuxiliaryWireFrame(frame: Uint8Array): Promise<Uint8Array | undefined>;
    recvAuxiliaryWireFrames(maxFrames?: number, maxBytes?: number): Uint8Array[];
    auxiliaryOutboundReady(): Promise<void>;
    tick(): Promise<number>;
    updateAuthenticatedClaims(claims: Record<string, unknown>): Promise<void>;
    close(): boolean;
  }

  export type WriteOptions = {
    author?: Uint8Array;
    updatedAtMs?: number;
  };

  export type InsertOptions = WriteOptions & {
    rowId?: Uint8Array;
    branch?: unknown;
  };

  export type UpdateOptions = WriteOptions & {
    head?: unknown;
    base?: unknown;
  };

  export type UpsertOptions = UpdateOptions;

  export type DeleteOptions = UpdateOptions;
  export type RestoreOptions = WriteOptions & {
    branch?: unknown;
  };

  export class WasmTx {
    insertEncoded(table: string, cells: Uint8Array, options?: InsertOptions): Uint8Array;
    updateEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      options?: UpdateOptions,
    ): void;
    upsertEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: UpsertOptions,
    ): void;
    deleteEncoded(table: string, rowId: Uint8Array, options?: DeleteOptions): void;
    restoreEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: RestoreOptions,
    ): void;
    commit(): WasmWrite;
    rollback(): void;
  }

  export class WasmDb {
    static openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
    /** Explicit trusted-backend ABI; raw open config cannot select SYSTEM. */
    static openMemoryAsBackend(schema: Uint8Array, config: Uint8Array): WasmDb;
    static openMemoryWithSelfSignedProof(
      schema: Uint8Array,
      config: Uint8Array,
      token: string,
      appId: string,
      claimedAuthor: string,
    ): WasmDb;
    /** Host-only relay open; `storageOwner` is supplied by broker ownership admission. */
    static openBrowser(
      pageStore: unknown,
      schema: Uint8Array,
      config: Uint8Array,
      storageOwner: string,
    ): Promise<WasmDb>;
    static openBrowserWithSelfSignedProof(
      pageStore: unknown,
      schema: Uint8Array,
      config: Uint8Array,
      token: string,
      appId: string,
      claimedAuthor: string,
      storageOwner: string,
    ): Promise<WasmDb>;
    setLargeValueStagingPolicy(
      incomingBytesPerWindow: number,
      windowMs: number,
      maxAgeMs?: number | null,
    ): void;
    evictExpiredStagedLargeValues(): Promise<number>;
    beginStreamingMutationEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      column: string,
      mutation?: "insert" | "update" | "upsert",
      author?: Uint8Array,
      updatedAtMs?: number,
      head?: unknown,
      base?: unknown,
    ): StreamingMutation;
    /** Backend-only provenance-preserving streaming mutation. */
    beginStreamingMutationAttributedEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      column: string,
      mutation: "insert" | "update" | "upsert" | undefined,
      author: Uint8Array | null | undefined,
      attribution: Uint8Array,
      updatedAtMs?: number,
      head?: unknown,
      base?: unknown,
    ): StreamingMutation;
    readValueRange(
      table: string,
      rowId: Uint8Array,
      column: string,
      start: number,
      end: number,
    ): Promise<Uint8Array>;
    readTextUtf16Range(
      table: string,
      rowId: Uint8Array,
      column: string,
      start: number,
      end: number,
    ): Promise<string>;
    readJsonPointer(
      table: string,
      rowId: Uint8Array,
      column: string,
      pointer: string,
    ): Promise<unknown | null>;
    appendValue(
      table: string,
      rowId: Uint8Array,
      column: string,
      bytes: Uint8Array,
    ): Promise<WasmWrite>;
    spliceValue(
      table: string,
      rowId: Uint8Array,
      column: string,
      offset: number,
      deleteLength: number,
      insert: Uint8Array,
    ): Promise<WasmWrite>;
    static destroyBrowserStorage(namespace: string): Promise<void>;

    registerSchema(schema: Uint8Array): WasmDb;
    insertWithIdEncodedAttributed(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    updateEncodedAttributed(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    upsertEncodedAttributed(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    deleteAttributed(table: string, rowId: Uint8Array, author: Uint8Array): WasmWrite;
    restoreEncodedAttributed(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    beginTransaction(openTransactionId: string, kind: string, author?: Uint8Array | null): void;
    beginTransactionAttributed(openTransactionId: string, attribution: Uint8Array): void;
    commitTransaction(openTransactionId: string, kind?: string | null): WasmWrite;
    rollbackTransaction(openTransactionId: string): void;
    attachMergeableTx(openTransactionId: string): WasmTx;
    attachExclusiveTx(openTransactionId: string): WasmTx;

    prepareQuery(query: Uint8Array): WasmPreparedQuery;
    prepareQueryAsync(
      query: Uint8Array,
      author?: Uint8Array,
      claims?: Record<string, unknown>,
    ): {
      poll(): WasmPreparedQuery | undefined;
      cancel(): void;
      setWake(callback: () => void): void;
    };
    subscribeAsync(
      query: WasmPreparedQuery,
      opts?: unknown,
      author?: Uint8Array,
    ): {
      poll(): ReadableStream<unknown> | undefined;
      cancel(): void;
      setWake(callback: () => void): void;
    };
    all(
      query: WasmPreparedQuery,
      opts: unknown,
      openTransactionId?: string,
      author?: Uint8Array,
    ): Uint8Array | Promise<Uint8Array>;
    allAsync(
      query: WasmPreparedQuery,
      opts: unknown,
      openTransactionId?: string,
      author?: Uint8Array,
    ): Promise<Uint8Array>;
    allRelationSnapshot(
      query: WasmPreparedQuery,
      opts: unknown,
      openTransactionId?: string,
      author?: Uint8Array,
    ): Promise<Uint8Array>;
    allRelationQuery(queryJson: string, opts: unknown, author?: Uint8Array): Promise<Uint8Array>;
    subscribeForBackend(query: WasmPreparedQuery, opts: unknown): ReadableStream<unknown>;
    subscribeRelationQueryForBackend(queryJson: string, opts: unknown): ReadableStream<unknown>;
    one(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    /** Attach coverage, optionally at an open transaction snapshot and/or explicit identity. */
    attachQuery(
      query: WasmPreparedQuery,
      opts: unknown,
      openTransactionId?: string,
      author?: Uint8Array,
    ): QueryAttachment;
    queryAttachmentIsCovered(attachment: QueryAttachment): boolean;
    detachQuery(attachment: QueryAttachment): void;
    subscribe(query: WasmPreparedQuery, opts: unknown): ReadableStream<unknown>;
    subscribeRelationQuery(queryJson: string, opts: unknown): ReadableStream<unknown>;
    subscribeRelationQueryForIdentity(
      queryJson: string,
      author: Uint8Array,
      opts: unknown,
    ): ReadableStream<unknown>;

    insertEncoded(table: string, cells: Uint8Array, options?: InsertOptions): WasmWrite;
    canInsertEncoded(table: string, cells: Uint8Array): "allowed" | "denied" | "unknown";
    requestInsertPermissionAdviceEncoded(
      table: string,
      cells: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestReadPermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    updateEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      options?: UpdateOptions,
    ): WasmWrite;
    updateLargeValuesEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      descriptors: unknown,
      updatedAtMs?: number | null,
    ): WasmWrite;
    requestUpdatePermissionAdviceEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestDeletePermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    upsertEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: UpsertOptions,
    ): WasmWrite;
    deleteEncoded(table: string, rowId: Uint8Array, options?: DeleteOptions): WasmWrite;
    restoreEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: RestoreOptions,
    ): WasmWrite;
    setTickScheduler(
      callback: (urgency: "immediate" | "deferred" | `after:${number}`) => void,
    ): void;
    onMutationError(callback: (event: any) => void): void;
    tick(): Promise<void>;
    setNonDurableClient(): void;
    /** @internal Foreground node-lease handoff only. */
    foregroundTxTimeHighWater(): bigint;
    /** @internal Foreground node-lease bootstrap only. */
    seedForegroundTxTimeHighWater(highWater: bigint): void;
    /** Exact wire features compiled into this WASM artifact. */
    wireFeatures(): number;
    close(): Promise<boolean>;
    connectUpstream(): Promise<WasmTransport>;
    connectUpstreamWithSession(
      protocolVersion: number,
      features: number,
      remoteNode: Uint8Array,
      remoteEpoch: bigint,
      localNode: Uint8Array,
      localEpoch: bigint,
    ): Promise<WasmTransport>;
    acceptSubscriber(identity: Uint8Array, claims: Record<string, unknown>): WasmTransport;
    acceptSubscriberWithSelfSignedProof(
      claims: Record<string, unknown>,
      token: string,
      appId: string,
      claimedAuthor: string,
    ): WasmTransport;
    mergeableTx(openTransactionId: string): WasmTx;
    mergeableTxForIdentity(openTransactionId: string, author: Uint8Array): WasmTx;
    exclusiveTx(openTransactionId: string): WasmTx;
  }
}
