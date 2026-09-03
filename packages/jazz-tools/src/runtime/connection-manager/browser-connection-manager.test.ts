import { describe, expect, it, vi } from "vitest";
import type { JazzClient } from "../client.js";
import type { BrowserWorkerConnection } from "../runtime-source.js";
import { BrowserConnectionManager } from "./browser-connection-manager.js";
import type { DbForConnection } from "./types.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

describe("BrowserConnectionManager.shutdown", () => {
  it("continues teardown after flush fails and preserves the flush error", async () => {
    const flushError = new Error("flush failed");
    const workerShutdownError = new Error("worker shutdown failed");
    const connection: BrowserWorkerConnection = {
      ready: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      updateAuth: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      deleteStorage: vi.fn(async () => undefined),
      flushLocal: vi.fn(async () => {
        throw flushError;
      }),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      shutdown: vi.fn(async () => {
        throw workerShutdownError;
      }),
    };
    const client = {
      discard: vi.fn(),
    } as unknown as JazzClient;
    const disposeRuntimeTelemetry = vi.fn();
    const manager = new BrowserConnectionManager({} as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        client: JazzClient;
        disposeRuntimeTelemetry: () => void;
      },
      { connection, client, disposeRuntimeTelemetry },
    );

    await expect(manager.shutdown()).rejects.toBe(flushError);

    expect(client.discard).toHaveBeenCalledOnce();
    expect(disposeRuntimeTelemetry).toHaveBeenCalledOnce();
    expect(connection.shutdown).toHaveBeenCalledOnce();
  });
});

describe("BrowserConnectionManager explicit transport transitions", () => {
  it("enables Inspector-local reads only from the worker attachment receipt", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(
        () => "jazz-inspector-authenticated-root",
      ),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: { createBrowserWorkerConnection: vi.fn(() => connection) },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledWith(
        "jazz-inspector-authenticated-root",
      ),
    );
  });

  it("revokes an Inspector receipt when a failed follower is replaced, and requires a fresh receipt", async () => {
    const firstReady = deferred();
    const secondReady = deferred();
    const first = {
      ready: vi.fn(() => firstReady.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "same-coordinate"),
    } as unknown as BrowserWorkerConnection;
    const second = {
      ready: vi.fn(() => secondReady.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "same-coordinate"),
    } as unknown as BrowserWorkerConnection;
    const callbacks: Array<{ onFailure(error: unknown): void }> = [];
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks.push(input);
          return callbacks.length === 1 ? first : second;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    // The original connection fails before its init receipt resolves. Its late
    // receipt must not authorize the replacement merely because the physical
    // coordinate is identical.
    callbacks[0]?.onFailure(new Error("closed"));
    const reconnect = manager.reconnect();
    firstReady.resolve();
    await vi.waitFor(() =>
      expect(host.runtimeSource.createBrowserWorkerConnection).toHaveBeenCalledTimes(2),
    );
    expect(host.enableAuthenticatedInspectorLocalReads).not.toHaveBeenCalled();
    // Initial open, follower retirement, then the replacement opening each
    // revoke authority. In particular, removing the retirement clear makes
    // this assertion fail even though the replacement uses the same root.
    expect(host.clearAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(3);

    secondReady.resolve();
    await reconnect;
    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledWith("same-coordinate"),
    );
    expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(1);
  });

  it("revokes an Inspector receipt before storage reset", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      shutdown: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
      getAuthenticatedInspectorAttachmentPhysicalDbName: vi.fn(() => "authenticated-root"),
    } as unknown as BrowserWorkerConnection;
    let onStorageReset: (() => void) | undefined;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          onStorageReset = input.onStorageReset;
          return connection;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      enableAuthenticatedInspectorLocalReads: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    await vi.waitFor(() =>
      expect(host.enableAuthenticatedInspectorLocalReads).toHaveBeenCalledOnce(),
    );

    onStorageReset?.();
    await vi.waitFor(() =>
      expect(host.clearAuthenticatedInspectorLocalReads).toHaveBeenCalledTimes(2),
    );
  });

  it("serializes disconnect/reconnect and releases remote readiness after the last transition", async () => {
    const disconnectGate = deferred();
    const connection = {
      disconnect: vi.fn(() => disconnectGate.promise),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
    } as unknown as BrowserWorkerConnection;
    const manager = new BrowserConnectionManager({
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
    } as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        connectionReady: Promise<void>;
      },
      { connection, connectionReady: Promise.resolve() },
    );

    const disconnect = manager.disconnect();
    const ready = manager.ensureReady("edge");
    const reconnect = manager.reconnect();
    await Promise.resolve();
    expect(connection.reconnect).not.toHaveBeenCalled();

    disconnectGate.resolve();
    await Promise.all([disconnect, reconnect, ready]);
    expect(connection.disconnect).toHaveBeenCalledOnce();
    expect(connection.reconnect).toHaveBeenCalledOnce();
    expect(connection.waitForServerConnection).toHaveBeenCalledOnce();
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("does not retain explicit-offline state when disconnect fails", async () => {
    const failure = new Error("worker disconnect failed");
    const connection = {
      disconnect: vi.fn(async () => {
        throw failure;
      }),
    } as unknown as BrowserWorkerConnection;
    const manager = new BrowserConnectionManager({
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
    } as DbForConnection);
    Object.assign(
      manager as unknown as {
        connection: BrowserWorkerConnection;
        connectionReady: Promise<void>;
      },
      { connection, connectionReady: Promise.resolve() },
    );

    await expect(manager.disconnect()).rejects.toBe(failure);
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("reacquires a fresh follower after a terminal follower failure", async () => {
    const first = {
      ready: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const second = {
      ready: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      waitForServerConnection: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const callbacks: Array<{ onFailure(error: unknown): void }> = [];
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks.push(input);
          return callbacks.length === 1 ? first : second;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    await Promise.resolve();

    const failure = new Error(
      "Protocol: maintained root occurrence sidecar length does not match root rows",
    );
    callbacks[0]?.onFailure(failure);

    await manager.reconnect();

    expect(host.runtimeSource.createBrowserWorkerConnection).toHaveBeenCalledTimes(2);
    expect(first.reconnect).not.toHaveBeenCalled();
    expect(second.reconnect).toHaveBeenCalledOnce();
    await expect(manager.ensureReady("edge")).resolves.toBeUndefined();
  });

  it("disconnects a worker created while offline before an immediate reconnect", async () => {
    const disconnectGate = deferred();
    const connection = {
      ready: vi.fn(async () => undefined),
      disconnect: vi.fn(() => disconnectGate.promise),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn(() => connection),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    await manager.disconnect();

    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });
    const reconnect = manager.reconnect();
    await vi.waitFor(() => expect(connection.disconnect).toHaveBeenCalledOnce());
    expect(connection.reconnect).not.toHaveBeenCalled();

    disconnectGate.resolve();
    await reconnect;
    expect(connection.reconnect).toHaveBeenCalledOnce();
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("adopts explicit offline state broadcast by another tab in the worker namespace", async () => {
    const connection = {
      ready: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    let callbacks:
      | {
          onExplicitOfflineChange?: (offline: boolean) => void;
        }
      | undefined;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn((input) => {
          callbacks = input;
          return connection;
        }),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    callbacks?.onExplicitOfflineChange?.(true);
    expect(manager.isExplicitlyOffline()).toBe(true);

    const reconnected = manager.waitForReconnect();
    callbacks?.onExplicitOfflineChange?.(false);
    await reconnected;
    expect(manager.isExplicitlyOffline()).toBe(false);
  });

  it("waits for worker transport state only while a follower is attaching", async () => {
    const ready = deferred();
    const connection = {
      ready: vi.fn(() => ready.promise),
      disconnect: vi.fn(async () => undefined),
      reconnect: vi.fn(async () => undefined),
      openInspectorControlPort: vi.fn(async () => ({}) as MessagePort),
    } as unknown as BrowserWorkerConnection;
    const host = {
      config: { serverUrl: "https://example.test" },
      isShuttingDown: false,
      runtimeSource: {
        createBrowserWorkerConnection: vi.fn(() => connection),
      },
      markUnauthenticated: vi.fn(),
      clearAuthError: vi.fn(),
      clearAuthenticatedInspectorLocalReads: vi.fn(),
    } as unknown as DbForConnection;
    const manager = new BrowserConnectionManager(host);
    (
      manager as unknown as {
        onClientCreated(input: {
          schemaKey: string;
          schema: Record<string, never>;
          client: JazzClient;
        }): void;
      }
    ).onClientCreated({ schemaKey: "empty", schema: {}, client: {} as JazzClient });

    const initial = manager.initialExplicitOfflineState();
    expect(initial).not.toBeNull();
    ready.resolve();
    await initial;
    expect(manager.initialExplicitOfflineState()).toBeNull();
  });
});
