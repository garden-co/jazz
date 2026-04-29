import { afterEach, describe, expect, it, vi } from "vitest";
import {
  exportSyncPayloadTelemetryRecord,
  installWasmTraceTelemetry,
  normalizeOtlpEndpoint,
  type SyncPayloadTelemetryRecord,
} from "./sync-telemetry.js";

const originalFetch = globalThis.fetch;

afterEach(() => {
  vi.restoreAllMocks();
  if (originalFetch === undefined) {
    delete (globalThis as { fetch?: typeof fetch }).fetch;
  } else {
    globalThis.fetch = originalFetch;
  }
  delete (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;
});

describe("sync telemetry OTLP helpers", () => {
  it("normalizes collector base urls and full OTLP endpoints", () => {
    expect(normalizeOtlpEndpoint("http://localhost:4318", "logs")).toBe(
      "http://localhost:4318/v1/logs",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/logs", "logs")).toBe(
      "http://localhost:4318/v1/logs",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/logs", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
  });

  it("exports one sync payload record as an OTLP HTTP log without throwing on fetch failure", async () => {
    const fetchMock = vi.fn().mockRejectedValue(new Error("collector unavailable"));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const record: SyncPayloadTelemetryRecord = {
      appId: "telemetry-app",
      severityText: "DEBUG",
      scope: "worker_bridge",
      direction: "main_to_worker",
      sourcePayloadIndex: 0,
      sourcePayloadCount: 1,
      messageBytes: 3,
      messageEncoding: "binary",
      payloadVariant: "RowBatchCreated",
    };

    await expect(
      exportSyncPayloadTelemetryRecord("http://127.0.0.1:54418", record),
    ).resolves.toBeUndefined();

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]![0]).toBe("http://127.0.0.1:54418/v1/logs");
    const body = JSON.parse(String(fetchMock.mock.calls[0]![1]?.body));
    const logRecord = body.resourceLogs[0].scopeLogs[0].logRecords[0];
    expect(body.resourceLogs[0].resource.attributes).toContainEqual({
      key: "service.name",
      value: { stringValue: "jazz-browser" },
    });
    expect(body.resourceLogs[0].scopeLogs[0].scope.name).toBe("jazz-browser.sync-payload");
    expect(logRecord.severityText).toBe("DEBUG");
    expect(JSON.parse(logRecord.body.stringValue)).toMatchObject({
      appId: "telemetry-app",
      payloadVariant: "RowBatchCreated",
    });
    expect(logRecord.attributes).toContainEqual({
      key: "jazz.payload_variant",
      value: { stringValue: "RowBatchCreated" },
    });
  });

  it("installs a WASM span callback that exports OPFS spans as OTLP HTTP traces", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    installWasmTraceTelemetry({
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    const callback = (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;
    expect(callback).toBeTypeOf("function");
    (callback as (span: unknown) => void)({
      name: "OpfsBTree::put",
      target: "opfs_btree::db",
      level: "TRACE",
      fields: { key_len: "8", value_len: "32" },
      startUnixNano: "1775000000000000000",
      endUnixNano: "1775000000000123000",
    });

    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(fetchMock.mock.calls[0]![0]).toBe("http://127.0.0.1:54418/v1/traces");
    const body = JSON.parse(String(fetchMock.mock.calls[0]![1]?.body));
    const span = body.resourceSpans[0].scopeSpans[0].spans[0];
    expect(body.resourceSpans[0].scopeSpans[0].scope.name).toBe("jazz-wasm.tracing");
    expect(span.name).toBe("OpfsBTree::put");
    expect(span.attributes).toContainEqual({
      key: "jazz.runtime_thread",
      value: { stringValue: "worker" },
    });
  });
});
