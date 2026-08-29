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
  private readyError: Error | null = null;
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
    // worker asset. The physical namespace already includes the complete
    // app/environment/auth scope, so different accounts intentionally reach
    // separate workers and caches while same-scope tabs share one realm.
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
    // Retain admission failures as state rather than leaving a process-wide
    // bootstrap promise rejected. The manager converts this state back into
    // the caller's `all()`/`wait()` rejection at its operation boundary; a
    // tab which has not yet installed one must never produce an ambient
    // unhandled-rejection event merely because its durable root is owned by
    // another account.
    this.readyPromise = this.connect(
      runtime,
      { ...options, runtimeSources },
      fingerprint,
      workerName,
      createWorker,
    ).then(
      () => undefined,
      (error: unknown) => {
        this.readyError = error instanceof Error ? error : new Error(String(error));
        callbacks.onFailure(this.readyError);
      },
    );
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
      const outcome = await this.connectOnce(
        runtime,
        options,
        fingerprint,
        generationName,
        createWorker,
      );
      if (outcome.error) throw outcome.error;
      if (outcome.connected) {
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
  ): Promise<{ connected: boolean; error?: Error }> {
    const worker = createWorker(workerName);
    this.worker = worker;
    const port = worker.port;
    port.start();
    return new Promise<{ connected: boolean; error?: Error }>((resolve) => {
      let bootstrapTimer: ReturnType<typeof setTimeout> | null = setTimeout(() => {
        cleanup();
        port.close();
        if (this.worker === worker) this.worker = null;
        resolve({ connected: false });
      }, 1000);
      const onMessage = (event: MessageEvent<BrowserSharedWorkerConnectResponse>) => {
        if (event.data?.type === "worker-alive") {
          if (bootstrapTimer) clearTimeout(bootstrapTimer);
          bootstrapTimer = null;
          return;
        }
        if (event.data?.type === "runtime-error") {
          cleanup();
          // Do not reject a bare MessagePort callback promise. A browser can
          // report that rejection before the caller's operation has observed
          // readiness. The outer, constructor-owned state machine turns this
          // into the same explicit error after it has installed containment.
          resolve({ connected: false, error: new Error(event.data.message) });
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
          resolve({ connected: true });
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
        void this.connection.ready().then(
          () => resolve({ connected: true }),
          (error: unknown) =>
            resolve({
              connected: false,
              error: error instanceof Error ? error : new Error(String(error)),
            }),
        );
      };
      const onMessageError = () => {
        cleanup();
        resolve({
          connected: false,
          error: new Error("Shared browser runtime port message error"),
        });
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

  ready(): Promise<void> {
    // The constructor-owned promise always settles successfully; this is the
    // operation boundary that turns its retained admission failure back into
    // a normal caller-visible rejection.
    return this.readyPromise.then(() => {
      if (this.readyError) throw this.readyError;
    });
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
