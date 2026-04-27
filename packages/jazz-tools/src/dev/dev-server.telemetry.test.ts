import { describe, expect, it, vi } from "vitest";

const devServerStart = vi.fn().mockResolvedValue({
  appId: "00000000-0000-0000-0000-000000000001",
  port: 19999,
  url: "http://127.0.0.1:19999",
  dataDir: "/tmp/jazz-dev-server-test",
  syncPayloadTelemetryIngestUrl:
    "http://127.0.0.1:19999/apps/00000000-0000-0000-0000-000000000001/dev/sync-payload-telemetry",
  stop: vi.fn().mockResolvedValue(undefined),
});

vi.mock("jazz-napi", () => ({
  DevServer: {
    start: devServerStart,
  },
}));

describe("startLocalJazzServer telemetry", () => {
  it("forwards telemetry options to DevServer.start", async () => {
    const { startLocalJazzServer } = await import("./dev-server.js");

    await startLocalJazzServer({
      port: 19999,
      inMemory: true,
      telemetry: { collectorUrl: "http://localhost:4317" },
    });

    expect(devServerStart).toHaveBeenCalledWith(
      expect.objectContaining({
        telemetry: { collectorUrl: "http://localhost:4317" },
      }),
    );
  });

  it("returns the sync payload telemetry ingest URL from the NAPI server handle", async () => {
    const { startLocalJazzServer } = await import("./dev-server.js");

    const handle = await startLocalJazzServer({
      port: 19999,
      inMemory: true,
      telemetry: true,
    });

    expect(handle.syncPayloadTelemetryIngestUrl).toBe(
      "http://127.0.0.1:19999/apps/00000000-0000-0000-0000-000000000001/dev/sync-payload-telemetry",
    );
  });
});
