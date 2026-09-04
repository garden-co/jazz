import type { AuthFailureReason } from "../auth-state.js";
import type {
  BrowserFollowerConnection,
  BrowserFollowerConnectionContext,
} from "../runtime-source.js";
import { BrowserWorkerTransportPump, transferableFrames } from "./browser-worker-transport.js";
import {
  deserializeBrowserRelayError,
  type BrowserFollowerPortEvent,
  type BrowserFollowerPortRequest,
} from "./browser-worker-protocol.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { IndexedDbPageStore } from "../indexeddb-page-store.js";

type PendingRequest = {
  type: BrowserFollowerPortRpcRequest["type"] | "open-inspector-control";
  resolve: () => void;
  reject: (error: Error) => void;
};

type BrowserFollowerPortRpcRequest =
  | { type: "init"; sessionClaims: Record<string, unknown> }
  | { type: "wait-server" }
  | { type: "disconnect" }
  | { type: "flush-local" }
  | { type: "close"; releaseContext?: boolean }
  | { type: "prepare-storage-reset" }
  | { type: "finish-storage-reset" }
  | { type: "abort-storage-reset" }
  | { type: "reconnect"; authJson: string; sessionClaims: Record<string, unknown> };

/** Connects one tab's non-durable in-memory runtime to the elected worker. */
export class MessagePortBrowserFollowerConnection implements BrowserFollowerConnection {
  private pump: BrowserWorkerTransportPump | null = null;
  private readonly pendingFrames: Uint8Array[][] = [];
  private readonly readyPromise: Promise<void>;
  private inspectorAttachmentPhysicalDbName: string | null = null;
  private readonly pending = new Map<number, PendingRequest>();
  private nextRequestId = 1;
  private closed = false;
  private failed: Error | null = null;
  private readonly disposeQueryCoverageTrace: (() => void) | null;

  constructor(
    private readonly runtime: NativeRuntimeAdapter,
    private readonly port: MessagePort,
    sessionClaims: Record<string, unknown>,
    private readonly dbName: string | null,
    private readonly callbacks: Pick<
      BrowserFollowerConnectionContext,
      | "onAuthFailure"
      | "onAuthRestored"
      | "onExplicitOfflineChange"
      | "onFailure"
      | "onStorageReset"
      | "onStorageInvalidated"
    >,
    private readonly traceRelay = false,
  ) {
    port.addEventListener("message", this.onMessage);
    port.addEventListener("messageerror", this.onMessageError);
    port.start();
    this.disposeQueryCoverageTrace = traceRelay
      ? runtime.onQueryCoverageTrace((entry) => {
          if (this.closed) return;
          this.port.postMessage({
            type: "diagnostic-query-coverage",
            ...entry,
          } satisfies BrowserFollowerPortRequest);
        })
      : null;

    // Establish the accepted peer with this tab's claims before any runtime
    // frames can be delivered. MessagePort ordering keeps the handshake ahead
    // of the pump's first outbound frame.
    const initialized = this.request({ type: "init", sessionClaims });
    const connected = (async () => {
      const transport = await runtime.connectUpstreamPeer();
      if (this.closed || this.failed) {
        await runtime.retirePeerTransport(transport);
        throw this.failed ?? new Error("Browser follower connection is closed");
      }
      this.pump = new BrowserWorkerTransportPump(
        runtime,
        transport,
        (frames) => {
          const copies = transferableFrames(frames);
          this.port.postMessage(
            { type: "frames", frames: copies } satisfies BrowserFollowerPortRequest,
            copies.map((frame) => frame.buffer),
          );
        },
        (error) => {
          const failure = asError(error);
          // Retirement can itself fail after the causal transport failure.
          // Preserve the first error and notify remote waiters before teardown.
          if (this.failed || this.closed) return;
          runtime.reportRemoteServerTransportError(failure);
          this.fail(failure);
        },
        traceRelay
          ? (entries) => {
              console.debug(
                "JAZZ_AUX_RELAY",
                entries.map((entry) => ({ ...entry, hop: "tab-worker" })),
              );
            }
          : undefined,
      );
      for (const frames of this.pendingFrames.splice(0)) this.pump.receive(frames);
    })();
    this.readyPromise = Promise.all([initialized, connected]).then(() => undefined);
    void this.readyPromise.catch((error: unknown) => this.fail(asError(error)));
  }

  async ready(): Promise<void> {
    await this.readyPromise;
    if (this.failed) throw this.failed;
    if (this.closed) throw new Error("Browser follower connection is closed");
  }

  /** A worker-issued receipt, not caller-supplied authority. */
  getAuthenticatedInspectorAttachmentPhysicalDbName(): string | null {
    return this.inspectorAttachmentPhysicalDbName;
  }

  async waitForServerConnection(): Promise<void> {
    await this.ready();
    await this.request({ type: "wait-server" });
  }

  async disconnect(): Promise<void> {
    await this.ready();
    await this.request({ type: "disconnect" });
  }

  async deleteStorage(): Promise<void> {
    await this.ready();
    if (!this.dbName) throw new Error("Browser storage reset requires its IndexedDB name");
    await this.request({ type: "prepare-storage-reset" });
    try {
      await IndexedDbPageStore.destroy(this.dbName);
    } catch (error) {
      await this.request({ type: "abort-storage-reset" }).catch(() => undefined);
      throw error;
    }
    await this.request({ type: "finish-storage-reset" });
  }

  async openInspectorControlPort(): Promise<MessagePort> {
    await this.ready();
    const channel = new MessageChannel();
    const id = this.nextRequestId++;
    const promise = new Promise<void>((resolve, reject) => {
      this.pending.set(id, { type: "open-inspector-control", resolve, reject });
    });
    this.port.postMessage(
      {
        type: "open-inspector-control",
        id,
        port: channel.port2,
      } satisfies BrowserFollowerPortRequest,
      [channel.port2],
    );
    await promise;
    return channel.port1;
  }

  async reconnect(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    await this.ready();
    await this.request({ type: "reconnect", authJson, sessionClaims });
    // `reconnect` acknowledges that the worker started a new upstream attempt;
    // it does not establish that attempt. Keep the original remote failure
    // latched until the worker confirms an actual negotiated server connection.
    await this.request({ type: "wait-server" });
    this.runtime.clearRemoteServerTransportError();
  }

  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): void {
    if (this.closed || this.failed) return;
    void this.ready()
      .then(() => {
        this.port.postMessage({
          type: "update-auth",
          authJson,
          sessionClaims,
        } satisfies BrowserFollowerPortRequest);
      })
      .catch((error: unknown) => this.fail(asError(error)));
  }

  async shutdown(releaseContext = false): Promise<void> {
    if (this.closed) return;
    await this.request({ type: "close", releaseContext });
    this.dispose(new Error("Browser follower connection is closed"));
  }

  async flushLocal(): Promise<void> {
    await this.ready();
    await this.pump!.flush();
    const workerBarrier = this.request({ type: "flush-local" });
    await this.runtime.flushLocalSettlements();
    this.port.postMessage({ type: "flush-local-observed" } satisfies BrowserFollowerPortRequest);
    await workerBarrier;
  }

  detachForReconnect(): void {
    this.dispose(new Error("Browser follower connection is reconnecting"));
  }

  private request(request: BrowserFollowerPortRpcRequest): Promise<void> {
    if (this.failed) return Promise.reject(this.failed);
    if (this.closed) return Promise.reject(new Error("Browser follower connection is closed"));
    const id = this.nextRequestId++;
    const message = { ...request, id };
    const promise = new Promise<void>((resolve, reject) => {
      this.pending.set(id, { type: request.type, resolve, reject });
    });
    this.port.postMessage(message satisfies BrowserFollowerPortRequest);
    return promise;
  }

  private readonly onMessage = (event: MessageEvent<BrowserFollowerPortEvent>): void => {
    const message = event.data;
    if (message.type === "frames") {
      if (this.pump) this.pump.receive(message.frames);
      else this.pendingFrames.push(message.frames);
      return;
    }
    if (message.type === "auth-failure") {
      this.callbacks.onAuthFailure(message.reason as AuthFailureReason);
      return;
    }
    if (message.type === "auth-restored") {
      this.callbacks.onAuthRestored();
      return;
    }
    if (message.type === "transport-state") {
      this.callbacks.onExplicitOfflineChange?.(message.explicitlyDisconnected);
      return;
    }
    if (message.type === "mutation-error") {
      this.runtime.reportRemoteMutationError(message.event);
      return;
    }
    if (message.type === "transport-error") {
      // Keep this distinct from a fate rejection. The runtime records the
      // error before any later port teardown so active Edge/Global waits and
      // remote subscriptions wake, while Local durability stays valid.
      this.runtime.reportRemoteServerTransportError(deserializeBrowserRelayError(message.error));
      return;
    }
    if (message.type === "storage-reset") {
      for (const [id, pending] of this.pending) {
        if (pending.type !== "finish-storage-reset") continue;
        this.pending.delete(id);
        pending.resolve();
      }
      this.port.postMessage({
        type: "storage-reset-observed",
        resetId: message.resetId,
      } satisfies BrowserFollowerPortRequest);
      this.callbacks.onStorageReset?.();
      return;
    }
    if (message.type === "storage-invalidated") {
      this.callbacks.onStorageInvalidated?.();
      this.dispose(new Error("IndexedDB storage was externally invalidated"));
      return;
    }
    if (message.type === "relay-trace") {
      if (this.traceRelay) console.debug("JAZZ_AUX_RELAY", message.entries);
      return;
    }
    if (message.type === "error") {
      this.fail(deserializeBrowserRelayError(message.error));
      return;
    }
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    if (message.error) {
      pending.reject(deserializeBrowserRelayError(message.error));
    } else {
      this.inspectorAttachmentPhysicalDbName ??= message.inspectorAttachmentPhysicalDbName ?? null;
      pending.resolve();
    }
  };

  private readonly onMessageError = (): void => {
    this.fail(new Error("Browser follower port message error"));
  };

  private fail(error: Error): void {
    if (this.failed || this.closed) return;
    this.failed = error;
    // Tell the worker that this endpoint is gone so it can notify the broker,
    // which will issue a fresh channel while the leadership is still valid.
    try {
      this.port.postMessage({ type: "close" } satisfies BrowserFollowerPortRequest);
    } catch {
      // The port may already be unusable; disposal below is still required.
    }
    this.callbacks.onFailure(error);
    this.dispose(error);
  }

  private dispose(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.port.removeEventListener("message", this.onMessage);
    this.port.removeEventListener("messageerror", this.onMessageError);
    this.disposeQueryCoverageTrace?.();
    this.pump?.close();
    this.pendingFrames.length = 0;
    this.port.close();
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
