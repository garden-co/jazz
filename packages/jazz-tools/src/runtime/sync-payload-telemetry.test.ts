import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildSyncPayloadTelemetryDecodeFailureRecord,
  buildSyncPayloadTelemetryRecords,
  sendSyncPayloadTelemetryRecords,
} from "./sync-payload-telemetry.js";
import { decodeSyncPayloadForTelemetry } from "jazz-wasm";

vi.mock("jazz-wasm", () => ({
  decodeSyncPayloadForTelemetry: vi.fn(),
}));

describe("sync-payload-telemetry", () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    vi.restoreAllMocks();
    (globalThis as { fetch: typeof fetch }).fetch = originalFetch;
  });

  it("builds structured records without log body for normal payloads", () => {
    vi.mocked(decodeSyncPayloadForTelemetry).mockReturnValue({
      ok: true,
      records: [{ payloadVariant: "QuerySettled", queryId: 7 }],
    });

    const records = buildSyncPayloadTelemetryRecords(new Uint8Array([1, 2, 3]), {
      appId: "app-1",
      ingestUrl: "http://localhost:4317",
      scope: "worker_bridge",
      direction: "main_to_worker",
      clientId: "alice",
      sourceFrameId: "frame-1",
      sourcePayloadIndex: 0,
      sourcePayloadCount: 1,
      sourceFrameBytes: 3,
    });

    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({
      appId: "app-1",
      severityText: "DEBUG",
      scope: "worker_bridge",
      direction: "main_to_worker",
      clientId: "alice",
      sourceFrameId: "frame-1",
      messageBytes: 3,
      messageEncoding: "binary",
      payloadVariant: "QuerySettled",
      queryId: 7,
    });
    expect(records[0]).not.toHaveProperty("logBody");
  });

  it("includes parsed logBody returned for decoded errors", () => {
    const logBody = { Error: { SessionRequired: { object_id: "row-1" } } };
    vi.mocked(decodeSyncPayloadForTelemetry).mockReturnValue({
      ok: true,
      records: [{ payloadVariant: "Error", errorVariant: "SessionRequired" }],
      logBody,
    });

    const records = buildSyncPayloadTelemetryRecords("{}", {
      appId: "app-1",
      ingestUrl: "http://localhost:4317",
      scope: "worker_bridge",
      direction: "worker_to_main",
    });

    expect(records[0]).toMatchObject({
      messageEncoding: "utf8",
      payloadVariant: "Error",
      errorVariant: "SessionRequired",
      logBody,
    });
  });

  it("builds decode failure records without raw payload bytes", () => {
    const record = buildSyncPayloadTelemetryDecodeFailureRecord(
      new Uint8Array([0xde, 0xad]),
      new Error("bad postcard"),
      {
        appId: "app-1",
        ingestUrl: "http://localhost:4317",
        scope: "worker_bridge",
        direction: "main_to_worker",
      },
    );

    expect(record).toMatchObject({
      decodeError: "bad postcard",
      messageBytes: 2,
      messageEncoding: "binary",
    });
    expect(record).not.toHaveProperty("messageBase64");
    expect(record).not.toHaveProperty("logBody");
  });

  it("posts records fire-and-forget and swallows fetch failures", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("offline"));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    sendSyncPayloadTelemetryRecords("http://localhost:4317/ingest", [
      { appId: "app-1", severityText: "DEBUG" },
    ]);
    await Promise.resolve();

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:4317/ingest",
      expect.objectContaining({
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ appId: "app-1", severityText: "DEBUG" }),
      }),
    );
  });
});
