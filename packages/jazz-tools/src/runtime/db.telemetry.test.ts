import { afterEach, describe, expect, it, vi } from "vitest";
import type { JazzClient } from "./client.js";
import { Db, type DbConfig } from "./db.js";
import {
  DirectCoreSource,
  type DirectCoreClientContext,
  type DirectCoreTelemetryContext,
} from "./direct-core-source.js";

const TELEMETRY_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
] as const;

class TestCoreSource extends DirectCoreSource<DbConfig> {
  readonly installTelemetryMock = vi.fn(
    (_context: DirectCoreTelemetryContext<DbConfig>) => this.disposeTelemetry,
  );

  constructor(private readonly disposeTelemetry?: () => void) {
    super();
  }

  protected override async loadCore(): Promise<void> {
    return;
  }

  override createClient(_context: DirectCoreClientContext<DbConfig>): JazzClient {
    throw new Error("createClient should not be called by telemetry tests");
  }

  override installTelemetry(context: DirectCoreTelemetryContext<DbConfig>): (() => void) | null {
    return this.installTelemetryMock(context) ?? null;
  }
}

async function createTestDb(config: DbConfig, coreSource: TestCoreSource): Promise<Db> {
  await coreSource.load(config);
  return Db.create(config, coreSource);
}

afterEach(() => {
  vi.restoreAllMocks();
  for (const key of TELEMETRY_ENV_KEYS) {
    delete process.env[key];
  }
});

describe("Db direct core telemetry", () => {
  it("does not start main-thread telemetry when telemetry is disabled", async () => {
    const coreSource = new TestCoreSource();
    const db = await createTestDb({ appId: "main-no-telemetry" }, coreSource);

    (db as any).installMainThreadCoreTelemetry();

    expect(coreSource.installTelemetryMock).not.toHaveBeenCalled();
    await db.shutdown();
  });

  it("starts main-thread telemetry only when a collector URL exists", async () => {
    const disposeTelemetryMock = vi.fn();
    const coreSource = new TestCoreSource(disposeTelemetryMock);
    const config = {
      appId: "main-with-telemetry",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    };
    const db = await createTestDb(config, coreSource);

    (db as any).installMainThreadCoreTelemetry();

    expect(coreSource.installTelemetryMock).toHaveBeenCalledTimes(1);
    expect(coreSource.installTelemetryMock).toHaveBeenCalledWith({
      config,
      collectorUrl: "http://127.0.0.1:54418",
      runtimeThread: "main",
    });

    await db.shutdown();
    expect(disposeTelemetryMock).toHaveBeenCalledTimes(1);
  });
});
