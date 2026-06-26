/**
 * Dedicated Worker entry point for Jazz.
 *
 * The tab leader election/broker stays in TypeScript. Once this worker is
 * elected leader it opens the durable browser `WasmDb` and exchanges direct
 * sync frames with in-memory tab runtimes.
 */

import type { RuntimeSourcesConfig } from "../runtime/context.js";
import {
  readWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "../runtime/runtime-config.js";
import { installWasmTelemetry } from "../runtime/sync-telemetry.js";
import { isWasmTeardownTrap } from "../runtime/wasm-teardown-trap-suppressor.js";

/**
 * Init message: the only worker-protocol envelope that stays a JS object
 * (everything else rides as binary postcard inside `MainToWorkerWire`).
 * Stays JS because `runtimeSources` carries bundler-resolved JS module/blob
 * refs that don't postcard-serialise, and the shim consumes them locally
 * before handing off to Rust.
 */
interface InitMessage {
  type: "init";
  options?: WorkerInitOptions;
}

interface WorkerInitOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  clientId?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  workerLockName?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
  directOpen?: {
    schema: Uint8Array;
    config: Uint8Array;
    peerIdentity: Uint8Array;
  };
}

declare const self: {
  postMessage(msg: unknown, transfer?: Transferable[]): void;
  onmessage: ((event: MessageEvent) => void) | null;
  close(): void;
  location?: { origin?: string; href?: string };
};

type VitestBrowserRunner = {
  wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T>;
};

function ensureVitestWorkerImportShim(): void {
  const globalRef = globalThis as typeof globalThis & {
    __vitest_browser_runner__?: VitestBrowserRunner;
  };
  if (globalRef.__vitest_browser_runner__) return;
  // Vitest browser mode installs this on the page global, but dedicated workers
  // can miss that setup. Provide the same no-op wrapper so transformed worker
  // imports still resolve through the bundler.
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

ensureVitestWorkerImportShim();

// When the page navigates away, this worker's `ws_stream_wasm`
// transport is abandoned mid-flight and the dying WASM heap traps with
// `RuntimeError: memory access out of bounds` (or an `unreachable` from a
// `send_wrapper` panic in the WebSocket callback). The worker is being
// terminated anyway, so swallow that one inert trap rather than letting it
// reach the console. The Rust runtime sets `__jazzWorkerTearingDown` when it
// receives the "pagehide" lifecycle hint, so this only fires during teardown —
// a genuine fault during normal operation still surfaces.
(globalThis as unknown as EventTarget).addEventListener(
  "error",
  (event) => {
    if (!(globalThis as Record<string, unknown>).__jazzWorkerTearingDown) return;
    const message = (event as ErrorEvent).message || (event as ErrorEvent).error?.message;
    if (!isWasmTeardownTrap(message)) return;
    event.preventDefault();
    event.stopImmediatePropagation();
  },
  true,
);

const DEFAULT_WASM_LOG_LEVEL = "warn";
let initMessage: WorkerInitOptions | null = null;
const pendingMessages: unknown[] = [];
let wasmInitialized = false;
let host: DirectWorkerHost | null = null;

self.onmessage = (event: MessageEvent) => {
  const data = event.data;
  if (
    !initMessage &&
    typeof data === "object" &&
    data !== null &&
    !(data instanceof Uint8Array) &&
    (data as { type?: unknown }).type === "init"
  ) {
    initMessage = normalizeInitMessage(data as InitMessage);
    void bootstrapAndHandoff(initMessage);
    return;
  }
  if (host) host.handle(data);
  else pendingMessages.push(data);
};

function normalizeInitMessage(message: InitMessage | WorkerInitOptions): WorkerInitOptions {
  if ("options" in message && message.options) return message.options;
  return message as WorkerInitOptions;
}

function resolveAbsoluteWasmUrlFromInitError(error: unknown): string | null {
  const origin = self.location?.origin;
  if (!origin) return null;
  const message = error instanceof Error ? error.message : String(error ?? "");
  const match = message.match(/(\/[^"'\s]+\.wasm)/);
  const wasmPath = match?.[1];
  if (!wasmPath) return null;
  return new URL(wasmPath, origin).href;
}

async function runWithRootRelativeFetchSupport<T>(operation: () => Promise<T>): Promise<T> {
  const globalRef = globalThis as typeof globalThis & { fetch?: typeof fetch };
  const originalFetch = globalRef.fetch;
  const origin = self.location?.origin;
  if (typeof originalFetch !== "function" || !origin) return operation();

  const patchedFetch: typeof fetch = (input, init) =>
    originalFetch(
      typeof input === "string" && input.startsWith("/")
        ? new URL(input, origin).toString()
        : input,
      init,
    );
  globalRef.fetch = patchedFetch;
  try {
    return await operation();
  } finally {
    globalRef.fetch = originalFetch;
  }
}

async function ensureWasmInitialized(
  wasmModule: any,
  msg: Pick<WorkerInitOptions, "runtimeSources" | "fallbackWasmUrl"> | undefined,
): Promise<void> {
  if (wasmInitialized) return;

  const syncInitInput = resolveRuntimeConfigSyncInitInput(msg?.runtimeSources);
  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    wasmInitialized = true;
    return;
  }

  if (typeof wasmModule.default !== "function") {
    wasmInitialized = true;
    return;
  }

  const locationHref = self.location?.href;
  const wasmUrl =
    resolveRuntimeConfigWasmUrl(import.meta.url, locationHref, msg?.runtimeSources) ??
    readWorkerRuntimeWasmUrl(locationHref);

  if (wasmUrl) {
    await wasmModule.default({ module_or_path: wasmUrl });
    wasmInitialized = true;
    return;
  }

  try {
    await runWithRootRelativeFetchSupport(() => wasmModule.default());
  } catch (error) {
    const absoluteWasmUrl =
      resolveAbsoluteWasmUrlFromInitError(error) ?? msg?.fallbackWasmUrl ?? null;
    if (!absoluteWasmUrl) throw error;
    await wasmModule.default({ module_or_path: absoluteWasmUrl });
  }

  wasmInitialized = true;
}

async function bootstrapAndHandoff(init: WorkerInitOptions): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    (globalThis as any).__JAZZ_WASM_LOG_LEVEL = init.logLevel ?? DEFAULT_WASM_LOG_LEVEL;
    await ensureWasmInitialized(wasmModule, init);

    installWasmTelemetry({
      wasmModule,
      collectorUrl: init.telemetryCollectorUrl,
      appId: init.appId,
      runtimeThread: "worker",
    });

    await runWorkerHostWithOptionalLock(wasmModule, init);
  } catch (e: any) {
    self.postMessage({ type: "error", message: `Init failed: ${e?.message ?? e}` });
  }
}

async function runWorkerHostWithOptionalLock(
  wasmModule: any,
  init: WorkerInitOptions,
): Promise<void> {
  const handoff = async () => {
    host = await DirectWorkerHost.open(wasmModule, init);
    for (const message of pendingMessages.splice(0)) {
      host.handle(message);
    }
    pendingMessages.length = 0;
  };

  if (!init.workerLockName) {
    await handoff();
    return;
  }

  const locks = (globalThis as { navigator?: { locks?: WorkerLockManager } }).navigator?.locks;
  if (!locks || typeof locks.request !== "function") {
    self.postMessage({
      type: "error",
      message: `Worker lock preflight failed: Web Locks are unavailable for ${init.workerLockName}`,
    });
    return;
  }

  let lockGranted = false;
  let lockLossReported = false;
  try {
    await locks.request(
      init.workerLockName,
      { mode: "exclusive", ifAvailable: true },
      async (lock) => {
        if (!lock) {
          self.postMessage({
            type: "error",
            message: `Worker lock preflight failed: ${init.workerLockName} is already held`,
          });
          return;
        }

        lockGranted = true;
        await handoff();
        await new Promise<void>(() => undefined);
      },
    );
  } catch (error) {
    if (!lockGranted) {
      throw error;
    }
    reportWorkerLockLost(error);
    return;
  }

  if (lockGranted) {
    reportWorkerLockLost(new Error(`Worker lock ${init.workerLockName} was lost`));
    return;
  }

  if (!lockGranted) {
    pendingMessages.length = 0;
  }

  function reportWorkerLockLost(reason: unknown): void {
    if (lockLossReported) return;
    lockLossReported = true;
    const message = reason instanceof Error ? reason.message : String(reason);
    self.onmessage?.(
      new MessageEvent("message", {
        data: {
          type: "worker-lock-lost",
          workerLockName: init.workerLockName,
          reason: message,
        },
      }),
    );
  }
}

type WorkerInbound =
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "query"; id: number; query: Uint8Array; identity?: Uint8Array }
  | { type: "settle"; id: number }
  | { type: "server-in"; frame: Uint8Array }
  | { type: "lifecycle"; event: string }
  | { type: "attach-follower-port"; peerId: string; leadershipId: number; port: MessagePort }
  | { type: "detach-follower-port"; peerId: string; leadershipId: number }
  | { type: "simulate-crash" }
  | { type: "shutdown" };

type WorkerOutbound =
  | { type: "init-ok"; clientId: string }
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "server-out"; frames: Uint8Array[] }
  | { type: "query-result"; id: number; rows: Uint8Array }
  | { type: "settled"; id: number }
  | { type: "follower-port-attached"; peerId: string; leadershipId: number }
  | { type: "follower-port-closed"; peerId: string; leadershipId: number }
  | { type: "shutdown-ok" }
  | { type: "error"; message: string };

type DirectTransport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
};

type DirectDb = {
  connectUpstream(): DirectTransport;
  acceptSubscriber(identity: Uint8Array): DirectTransport;
  prepareQuery(query: Uint8Array): object;
  all(query: object, opts: unknown): Uint8Array;
  allForIdentity?(query: object, identity: Uint8Array, opts: unknown): Uint8Array;
  tick(): void;
};

function isUint8Array(value: unknown): value is Uint8Array {
  return ArrayBuffer.isView(value) && value.constructor.name === "Uint8Array";
}

function normalizeFrames(value: unknown): Uint8Array[] {
  return Array.isArray(value) ? value.filter(isUint8Array) : [];
}

function post(message: WorkerOutbound, transfer: Transferable[] = []): void {
  self.postMessage(message, transfer);
}

function frameTransfers(frames: Uint8Array[]): Transferable[] {
  return frames
    .map((frame) => frame.buffer)
    .filter((buffer): buffer is ArrayBuffer => buffer instanceof ArrayBuffer);
}

class DirectWorkerHost {
  private readonly peers = new Map<string, { port: MessagePort; transport: DirectTransport }>();
  private pumpScheduled = false;
  private pumpAgain = false;
  private pendingDurabilityTick = false;

  private constructor(
    private readonly db: DirectDb,
    private readonly mainTransport: DirectTransport,
    private readonly serverTransport: DirectTransport | null,
    private readonly peerIdentity: Uint8Array,
    private readonly clientId: string,
  ) {}

  static async open(wasmModule: any, init: WorkerInitOptions): Promise<DirectWorkerHost> {
    if (!init.directOpen) {
      throw new Error("worker init is missing direct WasmDb open bytes");
    }
    if (!wasmModule.WasmDb?.openBrowser) {
      throw new Error("jazz-wasm does not expose direct WasmDb.openBrowser");
    }
    const db = (await wasmModule.WasmDb.openBrowser(
      init.dbName,
      init.directOpen.schema,
      init.directOpen.config,
    )) as DirectDb;
    const mainTransport = db.acceptSubscriber(init.directOpen.peerIdentity);
    const serverTransport = init.serverUrl ? db.connectUpstream() : null;
    const host = new DirectWorkerHost(
      db,
      mainTransport,
      serverTransport,
      init.directOpen.peerIdentity,
      init.clientId ?? crypto.randomUUID(),
    );
    self.onmessage = (event: MessageEvent) => host.handle(event.data);
    post({ type: "init-ok", clientId: host.clientId });
    host.schedulePump();
    return host;
  }

  handle(data: unknown): void {
    if (!data || typeof data !== "object") return;
    const message = data as WorkerInbound;
    switch (message.type) {
      case "sync":
        for (const frame of normalizeFrames(message.frames)) {
          this.mainTransport.sendWireFrame(frame);
          this.pendingDurabilityTick = true;
        }
        this.schedulePump();
        return;
      case "query":
        if (message.query instanceof Uint8Array) {
          this.pump();
          const query = this.db.prepareQuery(message.query);
          const identity = message.identity instanceof Uint8Array ? message.identity : null;
          if (identity && typeof this.db.allForIdentity !== "function") {
            post({
              type: "error",
              message: "Worker local query requires session-scoped direct WasmDb reads",
            });
            return;
          }
          const rows = identity
            ? this.db.allForIdentity!(query, identity, { tier: "local" })
            : this.db.all(query, { tier: "local" });
          const transfer = rows.buffer instanceof ArrayBuffer ? [rows.buffer] : [];
          post({ type: "query-result", id: message.id, rows }, transfer);
          this.schedulePump();
        }
        return;
      case "settle":
        this.pendingDurabilityTick = true;
        this.pump();
        post({ type: "settled", id: message.id });
        return;
      case "server-in":
        if (message.frame instanceof Uint8Array) {
          this.serverTransport?.sendWireFrame(message.frame);
          this.pendingDurabilityTick = true;
          this.schedulePump();
        }
        return;
      case "attach-follower-port":
        this.attachFollowerPort(message.peerId, message.leadershipId, message.port);
        return;
      case "detach-follower-port":
        this.detachFollowerPort(message.peerId, message.leadershipId);
        return;
      case "lifecycle":
        if (message.event === "pagehide") {
          (globalThis as Record<string, unknown>).__jazzWorkerTearingDown = true;
        }
        return;
      case "simulate-crash":
        self.close();
        return;
      case "shutdown":
        this.shutdown();
        return;
      default:
        return;
    }
  }

  private attachFollowerPort(peerId: string, leadershipId: number, port: MessagePort): void {
    const existing = this.peers.get(peerId);
    existing?.transport.close();
    const transport = this.db.acceptSubscriber(this.peerIdentity);
    this.peers.set(peerId, { port, transport });
    port.addEventListener("message", (event: MessageEvent) => {
      const msg = event.data as { type?: string; frames?: unknown };
      if (msg.type === "sync") {
        for (const frame of normalizeFrames(msg.frames)) {
          transport.sendWireFrame(frame);
          this.pendingDurabilityTick = true;
        }
        this.schedulePump();
      } else if (msg.type === "close") {
        this.detachFollowerPort(peerId, leadershipId);
      }
    });
    port.start?.();
    post({ type: "follower-port-attached", peerId, leadershipId });
    this.schedulePump();
  }

  private detachFollowerPort(peerId: string, leadershipId: number): void {
    const peer = this.peers.get(peerId);
    if (!peer) return;
    peer.transport.close();
    peer.port.postMessage({ type: "close" });
    this.peers.delete(peerId);
    post({ type: "follower-port-closed", peerId, leadershipId });
  }

  private shutdown(): void {
    this.pendingDurabilityTick = true;
    this.pump();
    this.mainTransport.close();
    this.serverTransport?.close();
    for (const [peerId] of this.peers) {
      this.detachFollowerPort(peerId, 0);
    }
    post({ type: "shutdown-ok" });
    self.close();
  }

  private schedulePump(): void {
    if (this.pumpScheduled) return;
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pump();
      if (this.pumpAgain) {
        this.pumpAgain = false;
        this.schedulePump();
      }
    });
  }

  private pump(): void {
    for (let round = 0; round < 32; round += 1) {
      const hadPendingDurabilityTick = this.pendingDurabilityTick;
      this.pendingDurabilityTick = false;
      this.db.tick();
      let madeProgress = hadPendingDurabilityTick;
      madeProgress =
        this.pumpTransport(this.mainTransport, (frames) =>
          post({ type: "sync", frames }, frameTransfers(frames)),
        ) || madeProgress;
      if (this.serverTransport) {
        madeProgress =
          this.pumpTransport(this.serverTransport, (frames) =>
            post({ type: "server-out", frames }, frameTransfers(frames)),
          ) || madeProgress;
      }
      for (const { port, transport } of this.peers.values()) {
        madeProgress =
          this.pumpTransport(transport, (frames) => {
            port.postMessage({ type: "sync", frames }, frameTransfers(frames));
          }) || madeProgress;
      }
      if (hadPendingDurabilityTick) {
        this.db.tick();
      }
      if (!madeProgress) {
        return;
      }
    }
    this.pumpAgain = true;
  }

  private pumpTransport(transport: DirectTransport, send: (frames: Uint8Array[]) => void): boolean {
    transport.tick();
    const frames = normalizeFrames(transport.recvWireFrames());
    if (frames.length > 0) send(frames);
    return frames.length > 0;
  }
}

interface WorkerLockManager {
  request<T>(
    name: string,
    options: { mode?: "exclusive" | "shared"; ifAvailable?: boolean },
    callback: (lock: unknown | null) => Promise<T> | T,
  ): Promise<T>;
}

async function startup(): Promise<void> {
  try {
    const wasmModule: any = await import("jazz-wasm");
    if (readWorkerRuntimeWasmUrl(self.location?.href)) {
      await ensureWasmInitialized(wasmModule, undefined);
    }
    self.postMessage({ type: "ready" });
  } catch (e: any) {
    self.postMessage({ type: "error", message: `WASM load failed: ${e?.message ?? e}` });
  }
}

startup();
