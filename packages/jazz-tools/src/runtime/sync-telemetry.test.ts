import { afterEach, describe, expect, it, vi } from "vitest";

const otelMocks = vi.hoisted(() => {
  const logExporterConstructors = vi.fn();
  const logProviderConstructors = vi.fn();
  const logProcessorConstructors = vi.fn();
  const loggerNames: string[] = [];
  const loggerEmit = vi.fn();
  const logForceFlush = vi.fn(() => Promise.resolve());

  const traceExporterConstructors = vi.fn();
  const traceProviderConstructors = vi.fn();
  const traceProcessorConstructors = vi.fn();
  const tracerNames: string[] = [];
  const startSpan = vi.fn((name: string, options: unknown) => {
    const span = { name, options, end: vi.fn() };
    traceSpans.push(span);
    return span;
  });
  const traceSpans: Array<{
    name: string;
    options: unknown;
    end: ReturnType<typeof vi.fn>;
  }> = [];
  const traceForceFlush = vi.fn(() => Promise.resolve());

  return {
    logExporterConstructors,
    logProviderConstructors,
    logProcessorConstructors,
    loggerNames,
    loggerEmit,
    logForceFlush,
    traceExporterConstructors,
    traceProviderConstructors,
    traceProcessorConstructors,
    tracerNames,
    startSpan,
    traceSpans,
    traceForceFlush,
  };
});

vi.mock("@opentelemetry/exporter-logs-otlp-http", () => ({
  OTLPLogExporter: class {
    constructor(config: unknown) {
      otelMocks.logExporterConstructors(config);
    }
  },
}));

vi.mock("@opentelemetry/sdk-logs", () => ({
  LoggerProvider: class {
    constructor(config: unknown) {
      otelMocks.logProviderConstructors(config);
    }

    getLogger(name: string) {
      otelMocks.loggerNames.push(name);
      return { emit: otelMocks.loggerEmit };
    }

    forceFlush() {
      return otelMocks.logForceFlush();
    }
  },
  SimpleLogRecordProcessor: class {
    constructor(exporter: unknown) {
      otelMocks.logProcessorConstructors(exporter);
    }
  },
}));

vi.mock("@opentelemetry/api-logs", () => ({
  SeverityNumber: { DEBUG: 5 },
}));

vi.mock("@opentelemetry/exporter-trace-otlp-http", () => ({
  OTLPTraceExporter: class {
    constructor(config: unknown) {
      otelMocks.traceExporterConstructors(config);
    }
  },
}));

vi.mock("@opentelemetry/sdk-trace-base", () => ({
  BasicTracerProvider: class {
    constructor(config: unknown) {
      otelMocks.traceProviderConstructors(config);
    }

    getTracer(name: string) {
      otelMocks.tracerNames.push(name);
      return { startSpan: otelMocks.startSpan };
    }

    forceFlush() {
      return otelMocks.traceForceFlush();
    }
  },
  BatchSpanProcessor: class {
    constructor(exporter: unknown) {
      otelMocks.traceProcessorConstructors(exporter);
    }
  },
}));

vi.mock("@opentelemetry/api", () => ({
  SpanKind: { INTERNAL: 1 },
}));

vi.mock("@opentelemetry/resources", () => ({
  resourceFromAttributes: vi.fn((attributes: unknown) => ({ attributes })),
}));

import {
  exportSyncPayloadTelemetryRecord,
  installWasmTraceTelemetry,
  normalizeOtlpEndpoint,
  type SyncPayloadTelemetryRecord,
} from "./sync-telemetry.js";

const originalFetch = globalThis.fetch;

afterEach(() => {
  vi.restoreAllMocks();
  for (const mock of [
    otelMocks.logExporterConstructors,
    otelMocks.logProviderConstructors,
    otelMocks.logProcessorConstructors,
    otelMocks.loggerEmit,
    otelMocks.logForceFlush,
    otelMocks.traceExporterConstructors,
    otelMocks.traceProviderConstructors,
    otelMocks.traceProcessorConstructors,
    otelMocks.startSpan,
    otelMocks.traceForceFlush,
  ]) {
    mock.mockClear();
  }
  otelMocks.loggerNames.length = 0;
  otelMocks.tracerNames.length = 0;
  otelMocks.traceSpans.length = 0;
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

  it("exports one sync payload record through the official OTLP log exporter", async () => {
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

    expect(otelMocks.logExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54418/v1/logs",
    });
    expect(otelMocks.loggerNames).toContain("jazz-browser.sync-payload");
    expect(otelMocks.loggerEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        severityNumber: 5,
        severityText: "DEBUG",
        body: JSON.stringify(record),
        attributes: expect.objectContaining({
          "jazz.payload_variant": "RowBatchCreated",
        }),
      }),
    );
    expect(JSON.parse(otelMocks.loggerEmit.mock.calls[0]![0].body)).toMatchObject({
      appId: "telemetry-app",
      payloadVariant: "RowBatchCreated",
    });
    expect(otelMocks.logForceFlush).toHaveBeenCalledTimes(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("installs a WASM span callback that exports OPFS spans through the official trace exporter", async () => {
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

    await vi.waitFor(() => expect(otelMocks.traceForceFlush).toHaveBeenCalledTimes(1));
    expect(otelMocks.traceExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54418/v1/traces",
    });
    expect(otelMocks.tracerNames).toContain("jazz-wasm.tracing");
    expect(otelMocks.startSpan).toHaveBeenCalledWith(
      "OpfsBTree::put",
      expect.objectContaining({
        attributes: expect.objectContaining({
          "jazz.runtime_thread": "worker",
        }),
      }),
    );
    expect(otelMocks.traceSpans[0]!.end).toHaveBeenCalled();
    expect(otelMocks.traceForceFlush).toHaveBeenCalledTimes(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("buffers multiple WASM trace spans into one official trace provider flush", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 200 }));
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    installWasmTraceTelemetry({
      collectorUrl: "http://127.0.0.1:54419",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    const callback = (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__ as
      | ((span: unknown) => void)
      | undefined;
    expect(callback).toBeTypeOf("function");

    callback!({
      name: "OpfsBTree::put",
      target: "opfs_btree::db",
      level: "TRACE",
      fields: { key_len: "8" },
      startUnixNano: "1775000000000000000",
      endUnixNano: "1775000000000001000",
    });
    callback!({
      name: "OpfsBTree::get",
      target: "opfs_btree::db",
      level: "TRACE",
      fields: { key_len: "8" },
      startUnixNano: "1775000000000002000",
      endUnixNano: "1775000000000003000",
    });
    callback!({
      name: "OpfsBTree::range",
      target: "opfs_btree::db",
      level: "TRACE",
      fields: { prefix_len: "4" },
      startUnixNano: "1775000000000004000",
      endUnixNano: "1775000000000005000",
    });

    await vi.waitFor(() => expect(otelMocks.traceForceFlush).toHaveBeenCalledTimes(1));
    expect(otelMocks.traceExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54419/v1/traces",
    });
    expect(otelMocks.traceSpans.map((span) => span.name)).toEqual([
      "OpfsBTree::put",
      "OpfsBTree::get",
      "OpfsBTree::range",
    ]);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
