import { afterEach, describe, expect, it, vi } from "vitest";
import type { JazzClient } from "./client.js";
import { Db, type DbConfig } from "./db.js";
import {
  DbBackendModule,
  type DbBackendClientContext,
  type DbBackendTelemetryContext,
} from "./db-backend.js";

const TELEMETRY_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
] as const;

class TestBackendModule extends DbBackendModule<DbConfig> {
  readonly installTelemetryMock = vi.fn(
    (_context: DbBackendTelemetryContext<DbConfig>) => this.disposeTelemetry,
  );

  constructor(private readonly disposeTelemetry?: () => void) {
    super();
  }

  protected override async loadResources(): Promise<void> {
    return;
  }

  override createClient(_context: DbBackendClientContext<DbConfig>): JazzClient {
    throw new Error("createClient should not be called by telemetry tests");
  }

  override installTelemetry(context: DbBackendTelemetryContext<DbConfig>): (() => void) | null {
    return this.installTelemetryMock(context) ?? null;
  }
}

async function createTestDb(config: DbConfig, backend: TestBackendModule): Promise<Db> {
  await backend.load(config);
  return Db.create(config, backend);
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const key of TELEMETRY_ENV_KEYS) {
    delete process.env[key];
  }
});

describe("Db backend telemetry", () => {
  it("does not start main-thread telemetry when telemetry is disabled", async () => {
    const backend = new TestBackendModule();
    const db = await createTestDb({ appId: "main-no-telemetry" }, backend);

    (db as any).installMainThreadWasmTelemetry();

    expect(backend.installTelemetryMock).not.toHaveBeenCalled();
    await db.shutdown();
  });

  it("starts main-thread telemetry only when a collector URL exists", async () => {
    const disposeTelemetryMock = vi.fn();
    const backend = new TestBackendModule(disposeTelemetryMock);
    const config = {
      appId: "main-with-telemetry",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    };
    const db = await createTestDb(config, backend);

    (db as any).installMainThreadWasmTelemetry();

    expect(backend.installTelemetryMock).toHaveBeenCalledTimes(1);
    expect(backend.installTelemetryMock).toHaveBeenCalledWith({
      config,
      collectorUrl: "http://127.0.0.1:54418",
      runtimeThread: "main",
    });

    await db.shutdown();
    expect(disposeTelemetryMock).toHaveBeenCalledTimes(1);
  });
});
