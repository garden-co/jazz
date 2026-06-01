import type { Runtime } from "../client.js";

export interface MessagePortRuntimeTransportOptions {
  port: MessagePort;
  runtime: Runtime;
}

interface RuntimeWithFollowerHooks extends Runtime {
  installFollowerOutboxSender(): void;
  setFollowerOutboxForwarder(cb: ((payload: Uint8Array) => void) | null): void;
  applyIncomingFollowerPayload(payload: Uint8Array): void;
  replayFollowerServerEdge(): void;
}

/**
 * Follower main-runtime transport. Replaces WorkerBridge for follower tabs:
 *   - Server-bound outbox is forwarded as a `follower-sync` JS object to the
 *     leader-minted port.
 *   - Incoming `leader-sync` payloads are applied via applyIncomingFollowerPayload.
 *   - On install, replays the runtime's server edge so the main runtime has
 *     somewhere to route server-bound outbox entries.
 */
export class MessagePortRuntimeTransport {
  private readonly port: MessagePort;
  private readonly runtime: RuntimeWithFollowerHooks;
  private disposed = false;

  constructor(opts: MessagePortRuntimeTransportOptions) {
    this.port = opts.port;
    this.runtime = opts.runtime as RuntimeWithFollowerHooks;
  }

  start(): void {
    this.runtime.installFollowerOutboxSender();
    this.runtime.setFollowerOutboxForwarder((payload) => {
      if (this.disposed) return;
      try {
        this.port.postMessage({
          type: "follower-sync",
          payload: [payload],
        });
      } catch {
        // port closed; SharedWorkerLeaderClient will reissue.
      }
    });
    this.runtime.replayFollowerServerEdge();

    this.port.onmessage = (event: MessageEvent) => {
      const data = event.data as { type?: string; payload?: unknown };
      if (data?.type !== "leader-sync") return;
      const payload = data.payload;
      if (!Array.isArray(payload)) return;
      let applied = false;
      for (const entry of payload) {
        if (entry instanceof Uint8Array) {
          this.runtime.applyIncomingFollowerPayload(entry);
          applied = true;
        }
      }
      // Applied sync messages are parked until a tick drives them — without
      // this, follower queries never settle and subscriptions never fire until
      // some unrelated event ticks the runtime. Mirrors WorkerBridge, which
      // calls `batched_tick()` after each worker→main sync batch
      // (worker_bridge.rs).
      if (applied) {
        this.runtime.batchedTick?.();
      }
    };
    this.port.start();
  }

  stop(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.runtime.setFollowerOutboxForwarder(null);
    try {
      this.port.onmessage = null;
      this.port.close();
    } catch {
      // best-effort
    }
  }
}
