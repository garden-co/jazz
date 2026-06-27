import type { MutationErrorEvent } from "../runtime/client.js";

declare module "jazz-wasm" {
  type InsertValues = Record<string, unknown>;
  type HrTime = [number, number];

  export type WasmTraceEntry =
    | {
        kind: "span";
        sequence: number;
        name: string;
        target: string;
        level: string;
        startUnixNano: HrTime;
        endUnixNano: HrTime;
        fields: Record<string, string>;
      }
    | {
        kind: "log";
        sequence: number;
        target: string;
        level: string;
        timestampUnixNano: HrTime;
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
  /**
   * Encode a JS-shaped main→worker control message (`{type: "<kebab-case>", ...}`)
   * to a postcard-binary `Uint8Array`. Test-harness convenience.
   */
  export function encodeMainToWorkerJs(value: unknown): Uint8Array;
  /**
   * Encode a JS-shaped worker→main control message (`{type: "<kebab-case>", ...}`)
   * to a postcard-binary `Uint8Array`. Test-harness convenience.
   */
  export function encodeWorkerToMainJs(value: unknown): Uint8Array;
  /**
   * Decode a postcard-binary main→worker message back into a JS object
   * (`{type: "<kebab-case>", ...}`). Test-harness convenience.
   */
  export function decodeMainToWorkerJs(bytes: Uint8Array): {
    type: string;
    [key: string]: unknown;
  };
  /**
   * Decode a postcard-binary worker→main message back into a JS object
   * (`{type: "<kebab-case>", ...}`). Test-harness convenience.
   */
  export function decodeWorkerToMainJs(bytes: Uint8Array): {
    type: string;
    [key: string]: unknown;
  };

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
    simulateCrash(): Promise<void>;
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
      nonDurableClient?: boolean,
    );

    insert(
      table: string,
      values: InsertValues,
      writeContextJson?: string | null,
      objectId?: string | null,
    ): { id: string; values: any[]; batchId: string };
    restore(
      table: string,
      objectId: string,
      values: InsertValues,
      writeContextJson?: string | null,
    ): { id: string; values: any[]; batchId: string };
    update(
      objectId: string,
      values: unknown,
      writeContextJson?: string | null,
    ): { batchId: string };
    upsert(
      table: string,
      objectId: string,
      values: InsertValues,
      writeContextJson?: string | null,
    ): { batchId: string };
    delete(objectId: string, writeContextJson?: string | null): { batchId: string };
    applyQueryBundle(bytes: Uint8Array): void;
    onMutationError(callback: (event: MutationErrorEvent) => void): void;
    beginBatch(batchMode: "direct" | "transactional"): string;
    rollbackBatch(batchId: string): boolean;
    commitBatch(batchId: string): void;
    waitForBatch(batchId: string, tier: string): Promise<void>;
    /** Connect to a Jazz server over WebSocket. */
    connect(url: string, authJson: string): void;
    /** Disconnect from the Jazz server and drop the transport handle. */
    disconnect(): void;
    /** Push updated auth credentials into the live transport. */
    updateAuth(authJson: string): void;
    /** Register a callback invoked when the Rust transport rejects auth. */
    onAuthFailure(callback: (reason: string) => void): void;
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
    unsubscribe(handle: number): void;
    /** Construct a Rust-owned `WasmWorkerBridge` attached to this runtime. Options
     * are parsed at attach time per spec; `init()` is parameter-less. */
    createWorkerBridge(worker: Worker, options: unknown): WasmWorkerBridge;
    getSchema(): unknown;
    getSchemaHash(): string;
    close?(): void;

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
  }
}
