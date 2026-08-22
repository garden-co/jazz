import { resolveBrokerWorkerUrl } from "../browser-broker-client.js";
import type { BrowserWorkerConnection, BrowserWorkerConnectionContext } from "../runtime-source.js";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import type {
  BrowserSharedWorkerConnectRequest,
  BrowserSharedWorkerConnectResponse,
  BrowserWorkerInitOptions,
} from "./browser-worker-protocol.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

export class SharedBrowserWorkerConnection implements BrowserWorkerConnection {
  private readonly worker: SharedWorker;
  private readonly readyPromise: Promise<void>;
  private connection: MessagePortBrowserFollowerConnection | null = null;
  private closed = false;

  constructor(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    private readonly callbacks: Pick<
      BrowserWorkerConnectionContext,
      "onAuthFailure" | "onAuthRestored" | "onFailure" | "onStorageReset" | "onStorageInvalidated"
    >,
  ) {
    this.worker =
      options.runtimeSources?.brokerWorkerUrl || options.runtimeSources?.baseUrl
        ? new SharedWorker(resolveBrokerWorkerUrl(options.runtimeSources), {
            type: "module",
            name: `jazz-runtime:${options.appId}:${options.dbName}`,
          })
        : new SharedWorker(new URL("../../worker/jazz-broker-worker.js", import.meta.url), {
            type: "module",
            name: "jazz-runtime",
          });
    const port = this.worker.port;
    port.start();
    this.readyPromise = new Promise<void>((resolve, reject) => {
      const onMessage = (event: MessageEvent<BrowserSharedWorkerConnectResponse>) => {
        if (event.data?.type === "runtime-error") {
          cleanup();
          reject(new Error(event.data.message));
          return;
        }
        if (event.data?.type !== "runtime-ready") return;
        cleanup();
        this.connection = new MessagePortBrowserFollowerConnection(
          runtime,
          port,
          options.sessionClaims,
          {
            onAuthFailure: callbacks.onAuthFailure,
            onAuthRestored: callbacks.onAuthRestored,
            onFailure: callbacks.onFailure,
            onStorageReset: callbacks.onStorageReset,
            onStorageInvalidated: callbacks.onStorageInvalidated,
          },
        );
        void this.connection.ready().then(resolve, reject);
      };
      const onMessageError = () => {
        cleanup();
        reject(new Error("Shared browser runtime port message error"));
      };
      const cleanup = () => {
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
    void this.readyPromise.catch(callbacks.onFailure);
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
    await this.connection?.shutdown();
    this.connection = null;
  }

  async attachFollowerPort(): Promise<void> {
    throw new Error("Shared browser runtime attaches tabs directly");
  }

  async detachFollowerPort(): Promise<void> {}

  async deleteStorage(): Promise<void> {
    await this.ready();
    await this.connection?.deleteStorage();
  }

  async simulateCrash(): Promise<void> {
    throw new Error("Shared browser runtime crash simulation is not implemented yet");
  }

  async simulatePendingAuthConfirmation(): Promise<void> {
    throw new Error("Shared browser runtime auth simulation is not implemented yet");
  }
}
