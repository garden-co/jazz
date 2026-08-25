import { describe, expect, it, vi } from "vitest";
import type { JazzClient } from "../client.js";
import type { BrowserWorkerConnection } from "../runtime-source.js";
import { BrowserConnectionManager } from "./browser-connection-manager.js";
import type { DbForConnection } from "./types.js";

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
