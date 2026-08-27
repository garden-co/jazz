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
});
