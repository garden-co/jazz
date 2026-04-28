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
      collectorUrl: "http://localhost:4318",
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
      collectorUrl: "http://localhost:4318",
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
        collectorUrl: "http://localhost:4318",
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

  it("posts OTLP logs to the normalized collector endpoint and swallows fetch failures", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("offline"));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    sendSyncPayloadTelemetryRecords("http://localhost:4318", [
      {
        appId: "app-1",
        severityText: "DEBUG",
        scope: "worker_bridge",
        direction: "main_to_worker",
        messageBytes: 3,
        messageEncoding: "binary",
        payloadVariant: "QuerySettled",
        recordedAt: 1_775_000_000_000,
      },
    ]);
    await Promise.resolve();

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:4318/v1/logs",
      expect.objectContaining({
        method: "POST",
        headers: { "content-type": "application/json" },
        body: expect.any(String),
      }),
    );
    const firstFetchCall = fetchMock.mock.calls[0];
    expect(firstFetchCall).toBeDefined();
    const body = JSON.parse((firstFetchCall![1] as RequestInit).body as string);
    const logRecord = body.resourceLogs[0].scopeLogs[0].logRecords[0];
    expect(logRecord).toMatchObject({
      severityNumber: 5,
      severityText: "DEBUG",
      body: {
        stringValue: expect.stringContaining('"payloadVariant":"QuerySettled"'),
      },
    });
    expect(logRecord.attributes).toEqual(
      expect.arrayContaining([
        { key: "jazz.app_id", value: { stringValue: "app-1" } },
        { key: "jazz.scope", value: { stringValue: "worker_bridge" } },
        { key: "jazz.direction", value: { stringValue: "main_to_worker" } },
        { key: "jazz.message_bytes", value: { intValue: "3" } },
        { key: "jazz.payload_variant", value: { stringValue: "QuerySettled" } },
      ]),
    );
  });

  it("does not append /v1/logs when collector URL is already a logs endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    (globalThis as { fetch: typeof fetch }).fetch = fetchMock as unknown as typeof fetch;

    sendSyncPayloadTelemetryRecords("http://localhost:4318/v1/logs", [
      { appId: "app-1", severityText: "DEBUG", recordedAt: 1_775_000_000_000 },
    ]);
    await Promise.resolve();

    expect(fetchMock).toHaveBeenCalledWith(
      "http://localhost:4318/v1/logs",
      expect.objectContaining({ method: "POST" }),
    );
  });
});
