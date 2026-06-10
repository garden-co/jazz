import { afterEach, describe, expect, it, vi } from "vitest";
import type { JazzClient } from "./client.js";
import { Db, type DbConfig } from "./db.js";
import {
  DbRuntimeModule,
  type DbRuntimeClientContext,
  type DbRuntimeTelemetryContext,
} from "./db-runtime-module.js";

const TELEMETRY_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
] as const;

class TestRuntimeModule extends DbRuntimeModule<DbConfig> {
  readonly createClientMock = vi.fn((_context: DbRuntimeClientContext<DbConfig>) => {
    return {
      onMutationError: vi.fn(),
      connectTransport: vi.fn(),
      shutdown: vi.fn(),
    } as unknown as JazzClient;
  });
  readonly installTelemetryMock = vi.fn(
    (_context: DbRuntimeTelemetryContext<DbConfig>) => this.disposeTelemetry,
  );

  constructor(private readonly disposeTelemetry?: () => void) {
    super();
  }

  protected override async loadRuntime(): Promise<void> {
    return;
  }

  override createClient(context: DbRuntimeClientContext<DbConfig>): JazzClient {
    return this.createClientMock(context);
  }

  override installTelemetry(context: DbRuntimeTelemetryContext<DbConfig>): (() => void) | null {
    return this.installTelemetryMock(context) ?? null;
  }
}

async function createTestDb(config: DbConfig, runtimeModule: TestRuntimeModule): Promise<Db> {
  await runtimeModule.load(config);
  return Db.create(config, runtimeModule);
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const key of TELEMETRY_ENV_KEYS) {
    delete process.env[key];
  }
});

describe("Db runtime telemetry", () => {
  it("does not start main-thread telemetry when telemetry is disabled", async () => {
    const runtimeModule = new TestRuntimeModule();
    const db = await createTestDb({ appId: "main-no-telemetry" }, runtimeModule);

    (db as any).installMainThreadWasmTelemetry();

    expect(runtimeModule.installTelemetryMock).not.toHaveBeenCalled();
    await db.shutdown();
  });

  it("starts main-thread telemetry only when a collector URL exists", async () => {
    const disposeTelemetryMock = vi.fn();
    const runtimeModule = new TestRuntimeModule(disposeTelemetryMock);
    const config = {
      appId: "main-with-telemetry",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    };
    const db = await createTestDb(config, runtimeModule);

    (db as any).installMainThreadWasmTelemetry();

    expect(runtimeModule.installTelemetryMock).toHaveBeenCalledTimes(1);
    expect(runtimeModule.installTelemetryMock).toHaveBeenCalledWith({
      config,
      collectorUrl: "http://127.0.0.1:54418",
      runtimeThread: "main",
    });

    await db.shutdown();
    expect(disposeTelemetryMock).toHaveBeenCalledTimes(1);
  });

  it("defaults WASM log level to debug when a collector URL exists", async () => {
    const runtimeModule = new TestRuntimeModule();
    const db = await createTestDb(
      {
        appId: "main-debug-telemetry",
        telemetryCollectorUrl: "http://127.0.0.1:54418",
      },
      runtimeModule,
    );

    (db as any).getClient({});

    expect(runtimeModule.createClientMock).toHaveBeenCalledTimes(1);
    expect(runtimeModule.createClientMock.mock.calls[0][0].config.logLevel).toBe("debug");
    await db.shutdown();
  });

  it("keeps explicit WASM log level when a collector URL exists", async () => {
    const runtimeModule = new TestRuntimeModule();
    const db = await createTestDb(
      {
        appId: "main-explicit-telemetry",
        telemetryCollectorUrl: "http://127.0.0.1:54418",
        logLevel: "warn",
      },
      runtimeModule,
    );

    (db as any).getClient({});

    expect(runtimeModule.createClientMock).toHaveBeenCalledTimes(1);
    expect(runtimeModule.createClientMock.mock.calls[0][0].config.logLevel).toBe("warn");
    await db.shutdown();
  });

  it("defaults worker bridge WASM log level to debug when a collector URL exists in env", async () => {
    process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL = "http://127.0.0.1:54418";
    const runtimeModule = new TestRuntimeModule();
    const db = await createTestDb({ appId: "worker-debug-telemetry" }, runtimeModule);

    const options = (db as any).buildWorkerBridgeOptions("{}");

    expect(options.logLevel).toBe("debug");
    expect(options.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
    await db.shutdown();
  });
});
