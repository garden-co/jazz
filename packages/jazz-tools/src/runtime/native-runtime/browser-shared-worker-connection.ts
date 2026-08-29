import {
  createBrowserWorkerAssetScope,
  resolveBrowserWorkerRuntimeSources,
  resolveBrowserWorkerUrl,
} from "../browser-worker-config.js";
import type { BrowserWorkerConnection, BrowserWorkerConnectionContext } from "../runtime-source.js";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import type {
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserFollowerPortRequest,
  BrowserWorkerInitOptions,
} from "./browser-worker-protocol.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

export class SharedBrowserWorkerConnection implements BrowserWorkerConnection {
  private worker: SharedWorker | null = null;
  private readonly readyPromise: Promise<void>;
  private connection: MessagePortBrowserFollowerConnection | null = null;
  private closed = false;

  constructor(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    private readonly callbacks: Pick<
      BrowserWorkerConnectionContext,
      | "onAuthFailure"
      | "onAuthRestored"
      | "onExplicitOfflineChange"
      | "onFailure"
      | "onStorageReset"
      | "onStorageInvalidated"
    >,
  ) {
    // A physical database namespace has exactly one broker realm for a given
    // worker asset. Auth scope is deliberately not part of this name: an
    // incompatible account must reach the existing owner and fail clearly
    // rather than silently opening a second page-store owner.
    const runtimeSources = resolveBrowserWorkerRuntimeSources(options.runtimeSources);
    const workerName = [
      "jazz-runtime",
      options.dbName,
      createBrowserWorkerAssetScope(runtimeSources),
    ].join(":");
    const createWorker =
      runtimeSources?.brokerWorkerUrl || runtimeSources?.baseUrl || runtimeSources?.wasmVersion
        ? (name: string) =>
            new SharedWorker(resolveBrowserWorkerUrl(runtimeSources), {
              type: "module",
              name,
            })
        : (name: string) =>
            new SharedWorker(new URL("../../worker/jazz-broker-worker.js", import.meta.url), {
              type: "module",
              name,
            });
    this.readyPromise = this.connect(
      runtime,
      { ...options, runtimeSources },
      fingerprint,
      workerName,
      createWorker,
    );
    void this.readyPromise.catch(callbacks.onFailure);
  }

  private async connect(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    workerName: string,
    createWorker: (name: string) => SharedWorker,
  ): Promise<void> {
    let generation = readWorkerGeneration(workerName);
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const generationName = `${workerName}:generation-${generation}`;
      if (await this.connectOnce(runtime, options, fingerprint, generationName, createWorker)) {
        return;
      }
      generation = advanceWorkerGeneration(workerName, generation);
    }
    throw new Error("Shared browser runtime did not answer after its previous realm closed");
  }

  private connectOnce(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    workerName: string,
    createWorker: (name: string) => SharedWorker,
  ): Promise<boolean> {
    const worker = createWorker(workerName);
    this.worker = worker;
    const port = worker.port;
    port.start();
    return new Promise<boolean>((resolve, reject) => {
      let bootstrapTimer: ReturnType<typeof setTimeout> | null = setTimeout(() => {
        cleanup();
        port.close();
        if (this.worker === worker) this.worker = null;
        resolve(false);
      }, 1000);
      const onMessage = (event: MessageEvent<BrowserSharedWorkerConnectResponse>) => {
        if (event.data?.type === "worker-alive") {
          if (bootstrapTimer) clearTimeout(bootstrapTimer);
          bootstrapTimer = null;
          return;
        }
        if (event.data?.type === "runtime-error") {
          cleanup();
          reject(new Error(event.data.message));
          return;
        }
        if (event.data?.type !== "runtime-ready") return;
        cleanup();
        if (this.closed) {
          port.postMessage({
            type: "close",
            releaseContext: true,
          } satisfies BrowserFollowerPortRequest);
          port.close();
          resolve(true);
          return;
        }
        this.connection = new MessagePortBrowserFollowerConnection(
          runtime,
          port,
          options.sessionClaims,
          options.dbName,
          {
            onAuthFailure: this.callbacks.onAuthFailure,
            onAuthRestored: this.callbacks.onAuthRestored,
            onExplicitOfflineChange: this.callbacks.onExplicitOfflineChange,
            onFailure: this.callbacks.onFailure,
            onStorageReset: this.callbacks.onStorageReset,
            onStorageInvalidated: this.callbacks.onStorageInvalidated,
          },
          options.logLevel === "trace",
        );
        void this.connection.ready().then(() => resolve(true), reject);
      };
      const onMessageError = () => {
        cleanup();
        reject(new Error("Shared browser runtime port message error"));
      };
      const cleanup = () => {
        if (bootstrapTimer) clearTimeout(bootstrapTimer);
        bootstrapTimer = null;
        port.removeEventListener("message", onMessage);
        port.removeEventListener("messageerror", onMessageError);
      };
      port.addEventListener("message", onMessage);
      port.addEventListener("messageerror", onMessageError);
      port.postMessage({
        type: "connect-runtime",
        tabId: crypto.randomUUID(),
        fingerprint,
        options,
      } satisfies BrowserSharedWorkerConnectRequest);
    });
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  async waitForServerConnection(): Promise<void> {
    await this.ready();
    await this.connection?.waitForServerConnection();
  }

  async updateAuth(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    await this.ready();
    this.connection?.updateAuth(authJson, sessionClaims);
  }

  async disconnect(): Promise<void> {
    await this.ready();
    await this.connection?.disconnect();
  }

  async reconnect(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    await this.ready();
    await this.connection?.reconnect(authJson, sessionClaims);
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.readyPromise.catch(() => undefined);
    await this.connection?.shutdown(true);
    this.connection = null;
    this.worker?.port.close();
    this.worker = null;
  }

  async flushLocal(): Promise<void> {
    await this.ready();
    await this.connection?.flushLocal();
  }

  async deleteStorage(): Promise<void> {
    await this.ready();
    await this.connection?.deleteStorage();
  }

  async openInspectorControlPort(): Promise<MessagePort> {
    await this.ready();
    if (!this.connection) throw new Error("Shared browser runtime is not connected");
    return this.connection.openInspectorControlPort();
  }
}

function readWorkerGeneration(workerName: string): number {
  try {
    const generation = Number.parseInt(
      localStorage.getItem(workerGenerationKey(workerName)) ?? "0",
      10,
    );
    return Number.isSafeInteger(generation) && generation >= 0 ? generation : 0;
  } catch {
    return 0;
  }
}

function advanceWorkerGeneration(workerName: string, failedGeneration: number): number {
  try {
    const key = workerGenerationKey(workerName);
    const current = readWorkerGeneration(workerName);
    if (current === failedGeneration) {
      localStorage.setItem(key, String(failedGeneration + 1));
    }
    return readWorkerGeneration(workerName);
  } catch {
    return failedGeneration + 1;
  }
}

function workerGenerationKey(workerName: string): string {
  return `jazz:shared-worker-generation:${workerName}`;
}
