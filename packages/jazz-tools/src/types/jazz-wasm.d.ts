declare module "jazz-wasm" {
  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
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
    readonly batchId: string;
    readonly payload: Uint8Array;
    writeState(): unknown;
    wait(tier: string): Promise<void>;
    close(): boolean;
  }

  export class WasmTransport {
    sendWireFrame(frame: Uint8Array): void;
    recvWireFrames(): Uint8Array[];
    tick(): number;
    updateAuthenticatedClaims(claims: Record<string, unknown>): void;
    close(): boolean;
  }

  export class WasmTx {
    insertWithIdEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    updateEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    upsertEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): void;
    restoreEncoded(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      updatedAtMs?: number | null,
    ): void;
    commit(): WasmWrite;
    rollback(): void;
  }

  export class WasmDb {
    static openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
    static openBrowser(namespace: string, schema: Uint8Array, config: Uint8Array): Promise<WasmDb>;
    static destroyBrowserStorage(namespace: string): Promise<void>;

    registerSchema(schema: Uint8Array): WasmDb;
    beginTransaction(openBatchId: string, kind: string, author?: Uint8Array | null): void;
    commitTransaction(openBatchId: string, kind?: string | null): WasmWrite;
    rollbackTransaction(openBatchId: string): void;
    attachMergeableTx(openBatchId: string): WasmTx;
    attachExclusiveTx(openBatchId: string): WasmTx;

    prepareQuery(query: Uint8Array): WasmPreparedQuery;
    all(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    one(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    allForIdentity(query: WasmPreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
    allRelationQuery(queryJson: string, opts: unknown): Uint8Array;
    allRelationQueryForIdentity(queryJson: string, author: Uint8Array, opts: unknown): Uint8Array;
    attachQuery(query: WasmPreparedQuery, opts: unknown): QueryAttachment;
    attachQueryForIdentity(
      query: WasmPreparedQuery,
      author: Uint8Array,
      opts: unknown,
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

    insertEncoded(table: string, cells: Uint8Array): WasmWrite;
    canInsertEncoded(table: string, cells: Uint8Array): "allowed" | "denied" | "unknown";
    requestInsertPermissionAdviceEncoded(
      table: string,
      cells: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestReadPermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    insertWithIdEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): WasmWrite;
    updateEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    requestUpdatePermissionAdviceEncoded(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestDeletePermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    upsertEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): WasmWrite;
    deleteForIdentity(
      table: string,
      rowId: Uint8Array,
      author: Uint8Array,
      updatedAtMs?: number | null,
    ): WasmWrite;
    restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    restoreEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void;
    onMutationError(callback: (event: any) => void): void;
    tick(): void;
    setNonDurableClient(): void;
    close(): boolean;
    connectUpstream(): WasmTransport;
    connectUpstreamWithSession(
      protocolVersion: number,
      features: number,
      remoteNode: Uint8Array,
      remoteEpoch: bigint,
      localNode: Uint8Array,
      localEpoch: bigint,
    ): WasmTransport;
    acceptSubscriber(identity: Uint8Array, claims: Record<string, unknown>): WasmTransport;
    mergeableTx(openBatchId: string): WasmTx;
    mergeableTxForIdentity(openBatchId: string, author: Uint8Array): WasmTx;
    exclusiveTx(openBatchId: string): WasmTx;
  }
}
