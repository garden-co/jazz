import type { LocalBatchRecord, MutationErrorEvent } from "../runtime/client.js";

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
    loadLocalBatchRecordStorageRow(batchId: string): Uint8Array | null;
    hydrateLocalBatchRecordStorageRow(bytes: Uint8Array): void;
    loadLocalBatchRecords(): LocalBatchRecord[];
    acknowledgeRejectedBatch(batchId: string): boolean;
    onMutationError(callback: (event: MutationErrorEvent) => void): void;
    loadBatchFate(batchId: string): BatchFate | null;
    replayBatchRejection(batchId: string, code: string, reason: string): void;
    discardLocalBatch(batchId: string): boolean;
    sealBatch(batchId: string): void;
    waitForBatch(batchId: string, tier: string): Promise<void>;
    retransmitLocalBatch(batchId: string): void;
    replayLocalBatchPayloads(batchId: string): Uint8Array[];
    reconcileLocalBatchWithServer(batchId: string): void;
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
    createWorkerBridge(worker: Worker, options: unknown): WasmWorkerBridge;
    addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
    removeServer(): void;
    reconcileLocalBatchWithServer?(batchId: string): void;
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

  export interface WasmWorkerBridgeListeners {
    onPeerSync?: (batch: { peerId: string; term: number; payload: Uint8Array[] }) => void;
    onAuthFailure?: (reason: string) => void;
    onLocalBatchRecordsSync?: (batches: LocalBatchRecord[]) => void;
    onMutationErrorReplay?: (event: MutationErrorEvent) => void;
  }

  export class WasmWorkerBridge {
    init(): Promise<{ clientId: string }>;
    updateAuth(jwtToken?: string): void;
    sendLifecycleHint(event: string): void;
    openPeer(peerId: string): void;
    sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void;
    closePeer(peerId: string): void;
    setServerPayloadForwarder(callback: ((payload: Uint8Array) => void) | null): void;
    applyIncomingServerPayload(payload: Uint8Array): void;
    waitForUpstreamServerConnection(): Promise<void>;
    replayServerConnection(): void;
    disconnectUpstream(): void;
    reconnectUpstream(): void;
    simulateCrash(): Promise<void>;
    acknowledgeRejectedBatch(batchId: string): void;
    setListeners(listeners: WasmWorkerBridgeListeners): void;
    getWorkerClientId(): string | null;
    shutdown(): Promise<void>;
  }

  /**
   * Entry point invoked from the worker bootstrap shim. Installs
   * `self.onmessage` and asynchronously brings up the worker runtime.
   */
  export function runAsWorker(initMessage: unknown, pendingMessages: unknown[]): void;

  /**
   * Test helper: decode a postcard-encoded `MainToWorkerWire` payload back into
   * a `{ type, ...fields }` JS object. Used by harnesses that intercept the
   * bridge's outbound Uint8Array traffic to assert against the original wire.
   * Not used in production code paths.
   */
  export function decodeMainToWorkerJs(bytes: Uint8Array): {
    type: string;
    [key: string]: unknown;
  };
}
