import type { BrowserWorkerConnection, BrowserWorkerConnectionContext } from "../runtime-source.js";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";

/** Browser connection backed by a peer port minted by an existing SharedWorker. */
export class AttachedBrowserWorkerConnection implements BrowserWorkerConnection {
  private readonly connection: MessagePortBrowserFollowerConnection;

  constructor(
    runtime: NativeRuntimeAdapter,
    port: MessagePort,
    sessionClaims: Record<string, unknown>,
    dbName: string,
    callbacks: Pick<
      BrowserWorkerConnectionContext,
      | "onAuthFailure"
      | "onAuthRestored"
      | "onExplicitOfflineChange"
      | "onFailure"
      | "onStorageReset"
      | "onStorageInvalidated"
    >,
  ) {
    this.connection = new MessagePortBrowserFollowerConnection(
      runtime,
      port,
      sessionClaims,
      dbName,
      callbacks,
    );
  }

  ready(): Promise<void> {
    return this.connection.ready();
  }

  waitForServerConnection(): Promise<void> {
    return this.connection.waitForServerConnection();
  }

  async updateAuth(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    this.connection.updateAuth(authJson, sessionClaims);
  }

  disconnect(): Promise<void> {
    return this.connection.disconnect();
  }

  reconnect(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    return this.connection.reconnect(authJson, sessionClaims);
  }

  deleteStorage(): Promise<void> {
    return this.connection.deleteStorage();
  }

  flushLocal(): Promise<void> {
    return this.connection.flushLocal();
  }

  shutdown(): Promise<void> {
    return this.connection.shutdown();
  }

  async openInspectorControlPort(): Promise<MessagePort> {
    throw new Error("Inspector peers cannot open nested inspector sessions");
  }

  getAuthenticatedInspectorAttachmentPhysicalDbName(): string | null {
    return this.connection.getAuthenticatedInspectorAttachmentPhysicalDbName();
  }
}
