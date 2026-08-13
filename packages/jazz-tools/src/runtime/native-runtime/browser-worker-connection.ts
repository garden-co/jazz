import type { AuthFailureReason } from "../auth-state.js";
import type {
  BrowserWorkerConnection as BrowserWorkerConnectionContract,
  BrowserWorkerConnectionContext,
} from "../runtime-source.js";
import { BrowserWorkerTransportPump, transferableFrames } from "./browser-worker-transport.js";
import type {
  BrowserWorkerEvent,
  BrowserWorkerInitOptions,
  BrowserWorkerMessage,
  BrowserWorkerRequest,
} from "./browser-worker-protocol.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

type PendingRequest = {
  resolve: () => void;
  reject: (error: Error) => void;
};

export class DedicatedBrowserWorkerConnection implements BrowserWorkerConnectionContract {
  private readonly worker: Worker;
  private readonly pump: BrowserWorkerTransportPump;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly readyPromise: Promise<void>;
  private nextRequestId = 1;
  private closed = false;
  private failed: Error | null = null;

  constructor(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    callbacks: Pick<BrowserWorkerConnectionContext, "onAuthFailure" | "onFailure">,
  ) {
    this.worker = new Worker(new URL("./browser-connection-worker.js", import.meta.url), {
      type: "module",
    });
    this.worker.onmessage = (event: MessageEvent<BrowserWorkerEvent>) => {
      this.handleMessage(event.data, callbacks.onAuthFailure);
    };
    this.worker.onerror = (event) => {
      this.fail(
        new Error(
          event.message
            ? `Browser persistence worker failed: ${event.message}`
            : "Browser persistence worker failed",
        ),
      );
    };

    this.readyPromise = this.request({ type: "init", ...options });
    this.readyPromise.catch(callbacks.onFailure);

    const transport = runtime.connectUpstreamPeer();
    this.pump = new BrowserWorkerTransportPump(runtime, transport, (frames) => {
      this.postFrames(frames);
    });
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  async waitForServerConnection(): Promise<void> {
    await this.ready();
    await this.request({ type: "wait-server" });
  }

  updateAuth(authJson: string): void {
    void this.ready()
      .then(() => this.request({ type: "update-auth", authJson }))
      .catch((error: unknown) => this.fail(asError(error)));
  }

  async disconnect(): Promise<void> {
    await this.ready();
    await this.request({ type: "disconnect" });
  }

  async reconnect(authJson: string): Promise<void> {
    await this.ready();
    await this.request({ type: "reconnect", authJson });
  }

  async deleteStorage(): Promise<void> {
    await this.ready();
    await this.request({ type: "delete-storage" });
    this.dispose();
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    try {
      await this.ready();
      await this.request({ type: "close" });
    } finally {
      this.dispose();
    }
  }

  private request(request: BrowserWorkerRequest): Promise<void> {
    if (this.failed) return Promise.reject(this.failed);
    if (this.closed) return Promise.reject(new Error("Browser persistence worker is closed"));
    const id = this.nextRequestId++;
    const promise = new Promise<void>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
    this.worker.postMessage({ id, ...request } satisfies BrowserWorkerMessage);
    return promise;
  }

  private postFrames(frames: readonly Uint8Array[]): void {
    if (this.closed || this.failed || frames.length === 0) return;
    const copies = transferableFrames(frames);
    this.worker.postMessage(
      { type: "frames", frames: copies } satisfies BrowserWorkerMessage,
      copies.map((frame) => frame.buffer),
    );
  }

  private handleMessage(
    message: BrowserWorkerEvent,
    onAuthFailure: (reason: AuthFailureReason) => void,
  ): void {
    if (message.type === "frames") {
      this.pump.receive(message.frames);
      return;
    }
    if (message.type === "auth-failure") {
      onAuthFailure(message.reason as AuthFailureReason);
      return;
    }
    if (message.type === "error") {
      this.fail(new Error(message.message));
      return;
    }
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    if (message.error) pending.reject(new Error(message.error));
    else pending.resolve();
  }

  private fail(error: Error): void {
    if (this.failed || this.closed) return;
    this.failed = error;
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
    this.pump?.close();
    this.worker.terminate();
  }

  private dispose(): void {
    if (this.closed) return;
    this.closed = true;
    this.pump.close();
    this.worker.terminate();
    const error = new Error("Browser persistence worker is closed");
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
