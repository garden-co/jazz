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

  export class WasmWrite {
    readonly payload: Uint8Array;
    writeState(): unknown;
    wait(tier: string): void;
  }

  export class WasmTransport {
    sendWireFrame(frame: Uint8Array): void;
    recvWireFrames(): Uint8Array[];
    tick(): number;
    close(): boolean;
  }

  export class WasmTx {
    insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
    updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
    upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
    delete(table: string, rowId: Uint8Array): void;
    restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
    commit(): WasmWrite;
    rollback(): void;
  }

  export class WasmDb {
    static openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
    static openBrowser(namespace: string, schema: Uint8Array, config: Uint8Array): Promise<WasmDb>;
    static destroyBrowserStorage(namespace: string): Promise<void>;

    prepareQuery(query: Uint8Array): WasmPreparedQuery;
    all(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    one(query: WasmPreparedQuery, opts: unknown): Uint8Array;
    allForIdentity(query: WasmPreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
    propagateQuery(query: WasmPreparedQuery, opts: unknown): void;
    queryIsCovered(query: WasmPreparedQuery): boolean;
    subscribe(query: WasmPreparedQuery, opts: unknown): ReadableStream<unknown>;

    insertEncoded(table: string, cells: Uint8Array): WasmWrite;
    canInsertEncoded(table: string, cells: Uint8Array): boolean;
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
    canUpdateEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      author: Uint8Array,
    ): boolean;
    upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    upsertEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    delete(table: string, rowId: Uint8Array): WasmWrite;
    deleteForIdentity(table: string, rowId: Uint8Array, author: Uint8Array): WasmWrite;
    restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): WasmWrite;
    restoreEncodedForIdentity(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      author: Uint8Array,
    ): WasmWrite;
    tick(): void;
    connectUpstream(): WasmTransport;
    acceptSubscriber(identity: Uint8Array): WasmTransport;
    mergeableTx(): WasmTx;
  }
}
