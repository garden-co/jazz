import type { LocalBatchRecord } from "../runtime/client.js";

declare module "jazz-wasm" {
  type InsertValues = Record<string, unknown>;

  export type WasmTraceEntry =
    | {
        kind: "span";
        sequence: number;
        name: string;
        target: string;
        level: string;
        startUnixNano: string;
        endUnixNano: string;
        fields: Record<string, string>;
      }
    | {
        kind: "log";
        sequence: number;
        target: string;
        level: string;
        timestampUnixNano: string;
        message: string;
        fields: Record<string, string>;
      }
    | { kind: "dropped"; count: number };

  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
  export function setTraceEntryCollectionEnabled(enabled: boolean): void;
  export function drainTraceEntries(): WasmTraceEntry[];
  export function subscribeTraceEntries(callback: () => void): () => void;
  /**
   * Worker-side entry point. Called by the JS shim after WASM init.
   * Synchronously installs a Rust closure as `self.onmessage`, then opens
   * the runtime and posts `init-ok` asynchronously.
   */
  export function runAsWorker(initMessage: unknown, pendingMessages: unknown[]): void;

  export class WasmWorkerBridge {
    static attach(worker: Worker, runtime: WasmRuntime, options: unknown): WasmWorkerBridge;
    init(): Promise<{ clientId: string }>;
    updateAuth(jwtToken?: string | null): void;
    sendLifecycleHint(event: string): void;
    openPeer(peerId: string): void;
    sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void;
    closePeer(peerId: string): void;
    setServerPayloadForwarder(
      callback:
        | ((payload: Uint8Array | string, isCatalogue: boolean, sequence: number | null) => void)
        | null,
    ): void;
    applyIncomingServerPayload(payload: Uint8Array): void;
    waitForUpstreamServerConnection(): Promise<void>;
    replayServerConnection(): void;
    disconnectUpstream(): void;
    reconnectUpstream(): void;
    acknowledgeRejectedBatch(batchId: string): void;
    setListeners(listeners: object): void;
    shutdown(): Promise<void>;
    getWorkerClientId(): string | null;
  }

  export class WasmRuntime {
    constructor(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      tier?: string,
      useBinaryEncoding?: boolean,
    );
    schedule?: (task: () => void) => void;

    insert(
      table: string,
      values: InsertValues,
      objectId?: string | null,
    ): { id: string; values: any[]; batchId: string };
    insertWithSession(
      table: string,
      values: InsertValues,
      sessionJson?: string | null,
      objectId?: string | null,
    ): { id: string; values: any[]; batchId: string };
    update(objectId: string, values: unknown): { batchId: string };
    updateWithSession(
      objectId: string,
      values: unknown,
      sessionJson?: string | null,
    ): { batchId: string };
    delete(objectId: string): { batchId: string };
    deleteWithSession(objectId: string, sessionJson?: string | null): { batchId: string };
    loadLocalBatchRecord(batchId: string): LocalBatchRecord | null;
    loadLocalBatchRecords(): LocalBatchRecord[];
    drainRejectedBatchIds(): string[];
    acknowledgeRejectedBatch(batchId: string): boolean;
    sealBatch(batchId: string): void;
    query(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): Promise<unknown>;
    createSubscription(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    executeSubscription(handle: number, onUpdate: Function): void;
    subscribe(
      queryJson: string,
      onUpdate: Function,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    unsubscribe(handle: number): void;
    onSyncMessageReceived(messageJson: string, seq?: number | null): void;
    /** Construct a Rust-owned `WasmWorkerBridge` attached to this runtime. Options
     * are parsed at attach time per spec; `init()` is parameter-less. */
    createWorkerBridge(worker: Worker, options: unknown): WasmWorkerBridge;
    addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
    removeServer(): void;
    batchedTick?(): void;
    addClient(): string;
    getSchema(): unknown;
    getSchemaHash(): string;
    close?(): void;
    setClientRole?(clientId: string, role: string): void;
    onSyncMessageReceivedFromClient?(clientId: string, messageJson: string): void;

    /** Derive a deterministic user ID (UUIDv5) from a base64url-encoded seed. */
    static deriveUserId(seedB64: string): string;
    /** Mint a Jazz self-signed JWT from a base64url-encoded seed. */
    static mintJazzSelfSignedToken(
      seedB64: string,
      issuer: string,
      audience: string,
      ttlSeconds: bigint,
      nowSeconds: bigint,
    ): string;
    /** Get the Ed25519 public key as base64url from a base64url-encoded seed. */
    static getPublicKeyBase64url(seedB64: string): string;
  }
}
