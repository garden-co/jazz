import { afterEach, describe, expect, it, vi } from "vitest";

const otelMocks = vi.hoisted(() => {
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
    traceExporterConstructors,
    traceProviderConstructors,
    traceProcessorConstructors,
    tracerNames,
    startSpan,
    traceSpans,
    traceForceFlush,
  };
});

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
  installWasmTraceTelemetry,
  normalizeOtlpEndpoint,
  resolveTelemetryCollectorUrlFromEnv,
} from "./sync-telemetry.js";

const originalFetch = globalThis.fetch;
const originalTelemetryEnv = {
  VITE_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL,
  NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
  PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
  EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
};

afterEach(() => {
  vi.restoreAllMocks();
  for (const mock of [
    otelMocks.traceExporterConstructors,
    otelMocks.traceProviderConstructors,
    otelMocks.traceProcessorConstructors,
    otelMocks.startSpan,
    otelMocks.traceForceFlush,
  ]) {
    mock.mockClear();
  }
  otelMocks.tracerNames.length = 0;
  otelMocks.traceSpans.length = 0;
  if (originalFetch === undefined) {
    delete (globalThis as { fetch?: typeof fetch }).fetch;
  } else {
    globalThis.fetch = originalFetch;
  }
  delete (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;

  for (const [key, value] of Object.entries(originalTelemetryEnv)) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
});

describe("telemetry OTLP helpers", () => {
  it("normalizes collector base urls and full OTLP endpoints", () => {
    expect(normalizeOtlpEndpoint("http://localhost:4318", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/traces", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/logs", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
  });

  it("resolves collector url from literal public env keys", () => {
    delete process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
    process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = " http://127.0.0.1:54418 ";
    delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
    delete process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;

    expect(resolveTelemetryCollectorUrlFromEnv()).toBe("http://127.0.0.1:54418");
  });

  it("uses the first configured public env key", () => {
    process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL = " http://127.0.0.1:54418 ";
    process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = "http://127.0.0.1:54419";
    process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = "http://127.0.0.1:54420";
    process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = "http://127.0.0.1:54421";

    expect(resolveTelemetryCollectorUrlFromEnv()).toBe("http://127.0.0.1:54418");
  });

  it("installs a WASM span callback that records OPFS spans through the official trace exporter", async () => {
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

    await vi.waitFor(() => expect(otelMocks.startSpan).toHaveBeenCalledTimes(1));
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
    expect(otelMocks.traceForceFlush).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("records multiple WASM trace spans without direct fetches or explicit flushes", async () => {
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

    await vi.waitFor(() => expect(otelMocks.startSpan).toHaveBeenCalledTimes(3));
    expect(otelMocks.traceExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54419/v1/traces",
    });
    expect(otelMocks.traceSpans.map((span) => span.name)).toEqual([
      "OpfsBTree::put",
      "OpfsBTree::get",
      "OpfsBTree::range",
    ]);
    expect(otelMocks.traceForceFlush).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("does not wait for a slow provider flush before recording later spans", async () => {
    let releaseFlush = () => {};
    const slowFlush = new Promise<void>((resolve) => {
      releaseFlush = resolve;
    });
    otelMocks.traceForceFlush.mockImplementationOnce(() => slowFlush);

    installWasmTraceTelemetry({
      collectorUrl: "http://127.0.0.1:54420",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    const callback = (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__ as
      | ((span: unknown) => void)
      | undefined;
    expect(callback).toBeTypeOf("function");

    let assertionError: unknown;
    try {
      callback!({
        name: "OpfsBTree::put",
        target: "opfs_btree::db",
        level: "TRACE",
        fields: { key_len: "8" },
        startUnixNano: "1775000000000000000",
        endUnixNano: "1775000000000001000",
      });

      await vi.waitFor(() => {
        expect(otelMocks.traceSpans.map((span) => span.name)).toEqual(["OpfsBTree::put"]);
      });

      callback!({
        name: "OpfsBTree::get",
        target: "opfs_btree::db",
        level: "TRACE",
        fields: { key_len: "8" },
        startUnixNano: "1775000000000002000",
        endUnixNano: "1775000000000003000",
      });

      await Promise.resolve();
      expect(otelMocks.traceSpans.map((span) => span.name)).toEqual([
        "OpfsBTree::put",
        "OpfsBTree::get",
      ]);
      expect(otelMocks.traceForceFlush).not.toHaveBeenCalled();
    } catch (error) {
      assertionError = error;
    } finally {
      releaseFlush();
      await Promise.resolve();
    }

    if (assertionError) throw assertionError;
  });

  it("returns a disposer that only clears its owned WASM span callback", () => {
    const dispose = installWasmTraceTelemetry({
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "main",
    });
    const installedCallback = (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;
    expect(installedCallback).toBeTypeOf("function");

    const replacementCallback = () => {};
    (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__ = replacementCallback;
    dispose();
    expect((globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__).toBe(
      replacementCallback,
    );

    const disposeReplacement = installWasmTraceTelemetry({
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "main",
    });
    expect((globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__).not.toBe(
      replacementCallback,
    );

    disposeReplacement();
    expect((globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__).toBeUndefined();
  });
});
