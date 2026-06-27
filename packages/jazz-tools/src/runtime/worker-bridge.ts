import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import type { WasmRow } from "../drivers/types.js";
import {
  DirectWebSocketCarrier,
  directWireAuthFailureReason,
} from "./core-runtime/direct-websocket.js";
import type { AuthFailureReason } from "./auth-state.js";

/** Page lifecycle hint forwarded to the worker runtime. */
export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  backendSecret?: string;
  cookieSession?: Session;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  workerLockName?: string;
  leadershipId?: number;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
  directOpen?: DirectOpenPayload;
}

export interface PeerSyncBatch {
  peerId: string;
  leadershipId: number;
  payload: Uint8Array[];
}

export interface FollowerPortEvent {
  peerId: string;
  leadershipId: number;
}

interface ListenerSlots {
  onPeerSync?: (batch: PeerSyncBatch) => void;
  onAuthFailure?: (reason: AuthFailureReason) => void;
  onFollowerPortAttached?: (event: FollowerPortEvent) => void;
  onFollowerPortClosed?: (event: FollowerPortEvent) => void;
}

type ServerPayloadForwarder = (payload: Uint8Array) => void;

export interface DirectOpenPayload {
  schema: Uint8Array;
  config: Uint8Array;
  peerIdentity: Uint8Array;
}

interface DirectTransport {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
}

interface RuntimeWithDirectTransport extends Runtime {
  connectUpstreamPeer?(): DirectTransport;
  decodeDirectRows?(payload: Uint8Array, queryJson: string): WasmRow[];
  encodeDirectQuery?(queryJson: string): Uint8Array;
  getDirectOpenPayload?(): DirectOpenPayload;
  onDirectSyncNeeded?(callback: () => void): () => void;
}

type WorkerInbound =
  | { type: "init"; options: WorkerBridgeOptions }
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "update-auth"; jwtToken?: string | null }
  | { type: "query"; id: number; query: Uint8Array; identity?: Uint8Array }
  | { type: "settle"; id: number }
  | { type: "server-in"; frame: Uint8Array }
  | { type: "lifecycle"; event: WorkerLifecycleEvent }
  | { type: "attach-follower-port"; peerId: string; leadershipId: number; port: MessagePort }
  | { type: "detach-follower-port"; peerId: string; leadershipId: number }
  | { type: "simulate-crash" }
  | { type: "shutdown" };

type WorkerOutbound =
  | { type: "ready" }
  | { type: "init-ok"; clientId: string }
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "server-out"; frames: Uint8Array[] }
  | { type: "query-result"; id: number; rows: Uint8Array }
  | { type: "settled"; id: number }
  | { type: "auth-failure"; reason: AuthFailureReason }
  | { type: "follower-port-attached"; peerId: string; leadershipId: number }
  | { type: "follower-port-closed"; peerId: string; leadershipId: number }
  | { type: "shutdown-ok" }
  | { type: "error"; message: string };

type PortInbound = { type: "sync"; frames: Uint8Array[] } | { type: "close" };

type PortOutbound =
  | { type: "sync"; frames: Uint8Array[] }
  | { type: "auth-failure"; reason: AuthFailureReason }
  | { type: "close" };

function isUint8Array(value: unknown): value is Uint8Array {
  return ArrayBuffer.isView(value) && value.constructor.name === "Uint8Array";
}

function normalizeFrames(frames: unknown): Uint8Array[] {
  return Array.isArray(frames) ? frames.filter(isUint8Array) : [];
}

export class WorkerBridge {
  private readonly worker: Worker;
  private readonly runtime: RuntimeWithDirectTransport;
  // Local peer between the page's in-memory runtime and the worker's durable runtime.
  private localSyncTransport: DirectTransport | null = null;
  private readonly listeners: ListenerSlots = {};
  private pendingForwarder: ServerPayloadForwarder | null = null;
  private serverCarrier: DirectWebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<DirectWebSocketCarrier> | null = null;
  private serverCarrierReady = false;
  private serverCarrierOptions: WorkerBridgeOptions | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private clientIdPromise: Promise<string> | null = null;
  private workerClientId: string | null = null;
  private clearPendingInit: (() => void) | null = null;
  private rejectPendingInit: ((error: Error) => void) | null = null;
  private disposed = false;
  private unsubscribeLocalSyncNeeded: (() => void) | null = null;

  private localSyncPumpScheduled = false;
  private localSyncPumpAgain = false;
  private shutdownResolve: (() => void) | null = null;
  private nextSettleId = 1;
  private nextQueryId = 1;
  private readonly pendingSettles = new Map<
    number,
    { resolve: () => void; reject: (error: Error) => void; timeout: ReturnType<typeof setTimeout> }
  >();
  private readonly pendingQueries = new Map<
    number,
    {
      queryJson: string;
      resolve: (rows: WasmRow[]) => void;
      reject: (error: Error) => void;
      timeout: ReturnType<typeof setTimeout>;
    }
  >();

  constructor(worker: Worker, runtime: Runtime) {
    this.worker = worker;
    this.runtime = runtime as RuntimeWithDirectTransport;
    this.worker.addEventListener("message", (event: MessageEvent<WorkerOutbound>) => {
      this.handleWorkerMessage(event.data);
    });
  }

  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.clientIdPromise) return this.clientIdPromise;
    if (this.disposed) {
      this.clientIdPromise = Promise.reject(new Error("WorkerBridge has been disposed"));
      return this.clientIdPromise;
    }

    const connectUpstreamPeer = this.runtime.connectUpstreamPeer;
    const getDirectOpenPayload = this.runtime.getDirectOpenPayload;
    if (typeof connectUpstreamPeer !== "function" || typeof getDirectOpenPayload !== "function") {
      this.clientIdPromise = Promise.reject(
        new Error("WorkerBridge requires a direct WasmDb runtime"),
      );
      return this.clientIdPromise;
    }

    this.localSyncTransport = connectUpstreamPeer.call(this.runtime);
    this.unsubscribeLocalSyncNeeded =
      this.runtime.onDirectSyncNeeded?.(() => this.scheduleLocalSyncPump()) ?? null;
    const directOpen = getDirectOpenPayload.call(this.runtime);
    const initOptions: WorkerBridgeOptions = {
      ...options,
      directOpen,
    };

    this.clientIdPromise = new Promise<string>((resolve, reject) => {
      this.rejectPendingInit = reject;
      const timeout = setTimeout(() => reject(new Error("WorkerBridge init timed out")), 30_000);
      const clearPendingInit = () => {
        clearTimeout(timeout);
        this.worker.removeEventListener("message", onMessage);
        if (this.clearPendingInit === clearPendingInit) {
          this.clearPendingInit = null;
          this.rejectPendingInit = null;
        }
      };
      const onMessage = (event: MessageEvent<WorkerOutbound>) => {
        const msg = event.data;
        if (this.disposed) return;
        if (msg.type === "init-ok") {
          clearPendingInit();
          this.workerClientId = msg.clientId;
          this.scheduleLocalSyncPump();
          resolve(msg.clientId);
        } else if (msg.type === "error") {
          clearPendingInit();
          reject(new Error(msg.message));
        }
      };
      this.clearPendingInit = clearPendingInit;
      this.worker.addEventListener("message", onMessage);
      this.postToWorker({ type: "init", options: initOptions });
      this.openServerCarrier(initOptions);
      this.scheduleLocalSyncPump();
    });
    return this.clientIdPromise;
  }

  updateAuth(auth: WorkerBridgeAuthUpdate): void {
    if (!this.serverCarrierOptions) {
      this.postToWorker({
        type: "update-auth",
        jwtToken: auth.jwtToken ?? null,
      });
      return;
    }
    this.serverCarrierOptions = {
      ...this.serverCarrierOptions,
      jwtToken: auth.jwtToken ?? undefined,
      backendSecret: auth.backendSecret,
      cookieSession: auth.cookieSession,
    };
    void this.reopenServerCarrier();
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    this.postToWorker({ type: "lifecycle", event });
  }

  async shutdown(): Promise<void> {
    if (this.disposed) return;
    this.pumpLocalSyncTransport();
    this.disposed = true;
    const rejectPendingInit = this.rejectPendingInit;
    this.clearPendingInit?.();
    this.clearPendingInit = null;
    rejectPendingInit?.(new Error("WorkerBridge init was shut down"));
    this.rejectPendingInit = null;
    this.unsubscribeLocalSyncNeeded?.();
    this.unsubscribeLocalSyncNeeded = null;
    this.closeServerCarrier();
    await new Promise<void>((resolve) => {
      const timeout = setTimeout(resolve, 1_000);
      this.shutdownResolve = () => {
        clearTimeout(timeout);
        resolve();
      };
      this.postToWorker({ type: "shutdown" });
    });
    this.localSyncTransport?.close();
    this.localSyncTransport = null;
  }

  getWorkerClientId(): string | null {
    return this.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ServerPayloadForwarder | null): void {
    this.pendingForwarder = forwarder;
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    await this.serverCarrierPromise;
    return;
  }

  async waitForDurableSettle(): Promise<void> {
    if (this.disposed) return;
    if (!this.localSyncTransport) {
      await this.clientIdPromise;
    }
    if (!this.localSyncTransport || this.disposed) return;

    this.pumpLocalSyncTransport();
    const id = this.nextSettleId++;
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingSettles.delete(id);
        reject(new Error("WorkerBridge durable settle timed out"));
      }, 30_000);
      this.pendingSettles.set(id, { resolve, reject, timeout });
      this.postToWorker({ type: "settle", id });
      this.scheduleLocalSyncPump();
    });
  }

  async queryLocalRows(queryJson: string, session?: Session): Promise<WasmRow[]> {
    if (this.disposed) return [];
    const encodeDirectQuery = this.runtime.encodeDirectQuery;
    const decodeDirectRows = this.runtime.decodeDirectRows;
    if (typeof encodeDirectQuery !== "function" || typeof decodeDirectRows !== "function") {
      throw new Error("WorkerBridge local query requires a direct WasmDb runtime");
    }
    await this.clientIdPromise;
    if (this.disposed) return [];

    await this.waitForDurableSettle();
    if (this.disposed) return [];

    this.pumpLocalSyncTransport();
    const id = this.nextQueryId++;
    const query = encodeDirectQuery.call(this.runtime, queryJson);
    const identity = session ? parseUuid(session.user_id) : undefined;
    return await new Promise<WasmRow[]>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingQueries.delete(id);
        reject(new Error("WorkerBridge local query timed out"));
      }, 30_000);
      this.pendingQueries.set(id, { queryJson, resolve, reject, timeout });
      this.postToWorker(
        { type: "query", id, query, identity },
        identity ? [query.buffer, identity.buffer] : [query.buffer],
      );
      this.scheduleLocalSyncPump();
    });
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    this.postToWorker({ type: "server-in", frame: payload });
  }

  replayServerConnection(): void {
    void this.reopenServerCarrier();
    this.scheduleLocalSyncPump();
  }

  disconnectUpstream(): void {
    this.closeServerCarrier();
  }

  reconnectUpstream(): void {
    void this.reopenServerCarrier();
    this.scheduleLocalSyncPump();
  }

  replayWorkerUpstreamConnection(): void {
    this.reconnectUpstream();
  }

  /** Test-only: posts `simulate-crash` so the worker releases OPFS handles
   * without a clean snapshot, and resolves on `shutdown-ok` (or after the
   * shutdown-ack timeout). Used to validate WAL replay. */
  async simulateCrash(): Promise<void> {
    this.postToWorker({ type: "simulate-crash" });
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.listeners.onPeerSync = listener;
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.listeners.onAuthFailure = listener;
  }

  onFollowerPortAttached(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortAttached = listener;
  }

  onFollowerPortClosed(listener: (event: FollowerPortEvent) => void): void {
    this.listeners.onFollowerPortClosed = listener;
  }

  openPeer(peerId: string): void {
    void peerId;
  }

  sendPeerSync(peerId: string, leadershipId: number, payload: Uint8Array[]): void {
    this.listeners.onPeerSync?.({ peerId, leadershipId, payload });
  }

  closePeer(peerId: string): void {
    void peerId;
  }

  attachFollowerPort(peerId: string, leadershipId: number, port: MessagePort): void {
    this.postToWorker({ type: "attach-follower-port", peerId, leadershipId, port }, [port]);
  }

  detachFollowerPort(peerId: string, leadershipId: number): void {
    this.postToWorker({ type: "detach-follower-port", peerId, leadershipId });
  }

  private handleWorkerMessage(message: WorkerOutbound): void {
    if (!message || typeof message !== "object") return;
    switch (message.type) {
      case "sync":
        for (const frame of normalizeFrames(message.frames)) {
          this.localSyncTransport?.sendWireFrame(frame);
        }
        this.scheduleLocalSyncPump();
        return;
      case "server-out":
        this.forwardServerFrames(normalizeFrames(message.frames));
        return;
      case "query-result": {
        const pending = this.pendingQueries.get(message.id);
        if (!pending) return;
        clearTimeout(pending.timeout);
        this.pendingQueries.delete(message.id);
        try {
          const decodeDirectRows = this.runtime.decodeDirectRows;
          if (typeof decodeDirectRows !== "function") {
            throw new Error("WorkerBridge local query requires a direct WasmDb runtime");
          }
          pending.resolve(decodeDirectRows.call(this.runtime, message.rows, pending.queryJson));
        } catch (error) {
          pending.reject(error instanceof Error ? error : new Error(String(error)));
        }
        return;
      }
      case "settled": {
        const pending = this.pendingSettles.get(message.id);
        if (!pending) return;
        clearTimeout(pending.timeout);
        this.pendingSettles.delete(message.id);
        pending.resolve();
        return;
      }
      case "auth-failure":
        this.listeners.onAuthFailure?.(message.reason);
        return;
      case "follower-port-attached":
        this.listeners.onFollowerPortAttached?.(message);
        return;
      case "follower-port-closed":
        this.listeners.onFollowerPortClosed?.(message);
        return;
      case "shutdown-ok":
        this.shutdownResolve?.();
        this.shutdownResolve = null;
        return;
      case "error":
        for (const pending of this.pendingSettles.values()) {
          clearTimeout(pending.timeout);
          pending.reject(new Error(message.message));
        }
        this.pendingSettles.clear();
        for (const pending of this.pendingQueries.values()) {
          clearTimeout(pending.timeout);
          pending.reject(new Error(message.message));
        }
        this.pendingQueries.clear();
        console.error("Jazz worker bridge error", message.message);
        return;
      default:
        return;
    }
  }

  private scheduleLocalSyncPump(): void {
    if (!this.localSyncTransport || this.localSyncPumpScheduled || this.disposed) return;
    this.localSyncPumpScheduled = true;
    queueMicrotask(() => {
      this.localSyncPumpScheduled = false;
      this.pumpLocalSyncTransport();
      if (this.localSyncPumpAgain) {
        this.localSyncPumpAgain = false;
        this.scheduleLocalSyncPump();
      }
    });
  }

  private pumpLocalSyncTransport(): void {
    const transport = this.localSyncTransport;
    if (!transport || this.disposed) return;
    for (let round = 0; round < 32; round += 1) {
      transport.tick();
      const frames = normalizeFrames(transport.recvWireFrames());
      if (frames.length > 0) {
        this.postToWorker(
          { type: "sync", frames },
          frames.map((frame) => frame.buffer),
        );
      }
      if (frames.length === 0) {
        return;
      }
    }
    this.localSyncPumpAgain = true;
  }

  private postToWorker(message: WorkerInbound, transfer: Transferable[] = []): void {
    this.worker.postMessage(message, transfer);
  }

  private openServerCarrier(options: WorkerBridgeOptions): void {
    if (!options.serverUrl || !options.directOpen) return;
    this.closeServerCarrier();
    this.serverCarrierOptions = options;
    const carrier = new DirectWebSocketCarrier({
      serverUrl: options.serverUrl,
      appId: options.appId,
      peerIdentity: options.directOpen.peerIdentity,
      authJson: buildWorkerBridgeAuthJson(options),
      onFrame: (frame) => {
        this.applyIncomingServerPayload(frame);
        this.scheduleLocalSyncPump();
      },
      onError: (error) => {
        const reason = directWireAuthFailureReason(error);
        if (reason) this.listeners.onAuthFailure?.(reason);
      },
    });
    this.serverCarrier = carrier;
    this.serverCarrierPromise = carrier.ready().then(() => {
      if (carrier !== this.serverCarrier || this.disposed) return carrier;
      this.serverCarrierReady = true;
      this.flushQueuedServerFrames(carrier);
      return carrier;
    });
    this.serverCarrierPromise.catch((error) => {
      this.rejectPendingServerWork(
        `Direct websocket connection failed: ${stringifyUnknown(error)}`,
      );
    });
  }

  private async reopenServerCarrier(): Promise<void> {
    const options = this.serverCarrierOptions;
    if (!options) return;
    this.closeServerCarrier();
    this.openServerCarrier(options);
    this.scheduleLocalSyncPump();
  }

  private closeServerCarrier(): void {
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverCarrierReady = false;
  }

  private forwardServerFrames(frames: Uint8Array[]): void {
    if (frames.length === 0) return;
    for (const frame of frames) {
      this.pendingForwarder?.(frame);
    }
    const carrier = this.serverCarrier;
    if (!carrier || !this.serverCarrierReady) {
      this.queuedServerFrames.push(...frames);
      return;
    }
    void carrier.sendBatch(frames).catch((error) => {
      this.rejectPendingServerWork(`Direct websocket send failed: ${stringifyUnknown(error)}`);
    });
  }

  private flushQueuedServerFrames(carrier: DirectWebSocketCarrier): void {
    if (this.queuedServerFrames.length === 0 || carrier !== this.serverCarrier) return;
    const frames = this.queuedServerFrames.splice(0);
    void carrier.sendBatch(frames).catch((error) => {
      this.rejectPendingServerWork(`Direct websocket send failed: ${stringifyUnknown(error)}`);
    });
  }

  private rejectPendingServerWork(message: string): void {
    const error = new Error(message);
    for (const pending of this.pendingSettles.values()) {
      clearTimeout(pending.timeout);
      pending.reject(error);
    }
    this.pendingSettles.clear();
    console.error("Jazz worker bridge server error", message);
  }
}

function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function stringifyUnknown(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

interface WorkerBridgeAuthUpdate {
  jwtToken?: string;
  backendSecret?: string;
  cookieSession?: Session;
}

function buildWorkerBridgeAuthJson(options: WorkerBridgeOptions): string {
  const payload: {
    jwt_token: string | null;
    admin_secret?: string;
    backend_secret?: string;
    backend_session?: Session;
  } = {
    jwt_token: options.jwtToken ?? null,
  };
  if (options.adminSecret) {
    payload.admin_secret = options.adminSecret;
  }
  if (options.backendSecret) {
    payload.backend_secret = options.backendSecret;
    if (options.cookieSession) {
      payload.backend_session = options.cookieSession;
    }
  }
  return JSON.stringify(payload);
}

export class MessagePortRuntimeBridge {
  private readonly port: MessagePort;
  private readonly runtime: RuntimeWithDirectTransport;
  private transport: DirectTransport | null = null;
  private authFailureCallback: ((reason: AuthFailureReason) => void) | null = null;
  private unsubscribeSyncNeeded: (() => void) | null = null;
  private pumpScheduled = false;
  private pumpAgain = false;

  constructor(port: MessagePort, runtime: Runtime) {
    this.port = port;
    this.runtime = runtime as RuntimeWithDirectTransport;
  }

  init(): void {
    if (this.transport) return;
    const connectUpstreamPeer = this.runtime.connectUpstreamPeer;
    if (typeof connectUpstreamPeer !== "function") {
      throw new Error("MessagePortRuntimeBridge requires a direct WasmDb runtime");
    }
    this.transport = connectUpstreamPeer.call(this.runtime);
    this.unsubscribeSyncNeeded =
      this.runtime.onDirectSyncNeeded?.(() => this.schedulePump()) ?? null;
    this.port.addEventListener("message", (event: MessageEvent<PortOutbound>) => {
      this.handlePortMessage(event.data);
    });
    this.port.start?.();
    this.schedulePump();
  }

  shutdown(): void {
    this.unsubscribeSyncNeeded?.();
    this.unsubscribeSyncNeeded = null;
    this.transport?.close();
    this.transport = null;
    this.port.postMessage({ type: "close" } satisfies PortInbound);
  }

  detachForReconnect(): void {
    this.unsubscribeSyncNeeded?.();
    this.unsubscribeSyncNeeded = null;
    this.transport?.close();
    this.transport = null;
  }

  onAuthFailure(callback: (reason: AuthFailureReason) => void): void {
    this.authFailureCallback = callback;
  }

  private handlePortMessage(message: PortOutbound): void {
    if (!message || typeof message !== "object") return;
    if (message.type === "sync") {
      for (const frame of normalizeFrames(message.frames)) {
        this.transport?.sendWireFrame(frame);
      }
      this.schedulePump();
    } else if (message.type === "auth-failure") {
      this.authFailureCallback?.(message.reason);
    } else if (message.type === "close") {
      this.detachForReconnect();
    }
  }

  private schedulePump(): void {
    if (!this.transport || this.pumpScheduled) return;
    this.pumpScheduled = true;
    queueMicrotask(() => {
      this.pumpScheduled = false;
      this.pumpTransport();
      if (this.pumpAgain) {
        this.pumpAgain = false;
        this.schedulePump();
      }
    });
  }

  private pumpTransport(): void {
    const transport = this.transport;
    if (!transport) return;
    for (let round = 0; round < 32; round += 1) {
      transport.tick();
      const frames = normalizeFrames(transport.recvWireFrames());
      if (frames.length > 0) {
        this.port.postMessage(
          { type: "sync", frames } satisfies PortInbound,
          frames.map((frame) => frame.buffer),
        );
      }
      if (frames.length === 0) {
        return;
      }
    }
    this.pumpAgain = true;
  }
}
