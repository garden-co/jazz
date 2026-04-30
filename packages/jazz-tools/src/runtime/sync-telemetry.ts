import type { SpanKind, TimeInput, Tracer } from "@opentelemetry/api";
import type { BasicTracerProvider } from "@opentelemetry/sdk-trace-base";

export const DEFAULT_TELEMETRY_COLLECTOR_URL = "http://localhost:4318";

export type TelemetryOptions = boolean | { collectorUrl?: string };

interface WasmTraceSpan {
  name?: unknown;
  target?: unknown;
  level?: unknown;
  fields?: unknown;
  startUnixNano?: unknown;
  endUnixNano?: unknown;
}

const MAX_WASM_TRACE_SPANS_PER_REQUEST = 256;
const MAX_PENDING_WASM_TRACE_SPANS = 5_000;
const TELEMETRY_COLLECTOR_URL_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
] as const;

type TelemetryAttributeValue = string | number | boolean;

interface WasmTraceExporterState {
  provider: BasicTracerProvider;
  tracer: Tracer;
  spanKindInternal: SpanKind;
}

const wasmTraceExporters = new Map<string, Promise<WasmTraceExporterState>>();

type ImportMetaWithEnv = ImportMeta & {
  env?: Record<string, string | undefined>;
};

export function resolveTelemetryCollectorUrl(
  telemetry: TelemetryOptions | undefined,
): string | undefined {
  if (telemetry === true) return DEFAULT_TELEMETRY_COLLECTOR_URL;
  if (telemetry === false || telemetry === undefined) return undefined;
  const collectorUrl = telemetry.collectorUrl?.trim();
  return collectorUrl || DEFAULT_TELEMETRY_COLLECTOR_URL;
}

export function resolveTelemetryCollectorUrlFromEnv(): string | undefined {
  const processEnv = typeof process !== "undefined" ? process.env : undefined;
  const metaEnv = (import.meta as ImportMetaWithEnv).env;

  return firstTelemetryUrlFromEnv(processEnv) ?? firstTelemetryUrlFromEnv(metaEnv);
}

function firstTelemetryUrlFromEnv(
  env: Record<string, string | undefined> | undefined,
): string | undefined {
  for (const key of TELEMETRY_COLLECTOR_URL_ENV_KEYS) {
    const value = env?.[key];
    const trimmed = value?.trim();
    if (trimmed) return trimmed;
  }
  return undefined;
}

export function normalizeOtlpEndpoint(collectorUrl: string, _signal: "traces"): string {
  const trimmed = collectorUrl.trim().replace(/\/+$/, "");
  const suffix = "/v1/traces";
  if (trimmed.endsWith("/v1/logs")) {
    return `${trimmed.slice(0, -"/v1/logs".length)}${suffix}`;
  }
  if (trimmed.endsWith("/v1/traces")) {
    return trimmed;
  }
  return `${trimmed}${suffix}`;
}

export function installWasmTraceTelemetry(options: {
  collectorUrl?: string;
  appId: string;
  runtimeThread: "main" | "worker";
}): () => void {
  if (!options.collectorUrl) return () => undefined;
  const globalRef = globalThis as Record<string, unknown>;
  const callback = (span: WasmTraceSpan) => {
    void recordWasmTraceSpan(options.collectorUrl!, options.appId, options.runtimeThread, span);
  };
  globalRef.__JAZZ_WASM_TRACE_SPAN__ = callback;
  return () => {
    if (globalRef.__JAZZ_WASM_TRACE_SPAN__ === callback) {
      delete globalRef.__JAZZ_WASM_TRACE_SPAN__;
    }
  };
}

async function recordWasmTraceSpan(
  collectorUrl: string,
  appId: string,
  runtimeThread: "main" | "worker",
  span: WasmTraceSpan,
): Promise<void> {
  try {
    const exporter = await getWasmTraceExporter(collectorUrl, appId);
    const otelSpan = exporter.tracer.startSpan(String(span.name ?? "wasm span"), {
      kind: exporter.spanKindInternal,
      startTime: unixNanoToTimeInput(span.startUnixNano),
      attributes: wasmTraceTelemetryAttributes(runtimeThread, span),
    });
    otelSpan.end(unixNanoToTimeInput(span.endUnixNano));
  } catch {
    // Silent by design.
  }
}

function getWasmTraceExporter(
  collectorUrl: string,
  appId: string,
): Promise<WasmTraceExporterState> {
  const url = normalizeOtlpEndpoint(collectorUrl, "traces");
  const cacheKey = `${url}\n${appId}`;
  const cached = wasmTraceExporters.get(cacheKey);
  if (cached) return cached;

  const created = createWasmTraceExporter(url, appId).catch((error) => {
    wasmTraceExporters.delete(cacheKey);
    throw error;
  });
  wasmTraceExporters.set(cacheKey, created);
  return created;
}

async function createWasmTraceExporter(
  url: string,
  appId: string,
): Promise<WasmTraceExporterState> {
  const [
    { OTLPTraceExporter },
    { BasicTracerProvider, BatchSpanProcessor },
    { SpanKind },
    { resourceFromAttributes },
  ] = await Promise.all([
    import("@opentelemetry/exporter-trace-otlp-http"),
    import("@opentelemetry/sdk-trace-base"),
    import("@opentelemetry/api"),
    import("@opentelemetry/resources"),
  ]);
  const otlpExporter = new OTLPTraceExporter({ url });
  const provider = new BasicTracerProvider({
    resource: resourceFromAttributes({
      "service.name": "jazz-browser",
      "telemetry.sdk.language": "webjs",
      "jazz.app_id": appId,
    }),
    spanProcessors: [
      new BatchSpanProcessor(otlpExporter, {
        maxExportBatchSize: MAX_WASM_TRACE_SPANS_PER_REQUEST,
        maxQueueSize: MAX_PENDING_WASM_TRACE_SPANS,
        scheduledDelayMillis: 1_000,
      }),
    ],
  });
  return {
    provider,
    tracer: provider.getTracer("jazz-wasm.tracing"),
    spanKindInternal: SpanKind.INTERNAL,
  };
}

function wasmTraceTelemetryAttributes(
  runtimeThread: "main" | "worker",
  span: WasmTraceSpan,
): Record<string, TelemetryAttributeValue> {
  return {
    "jazz.runtime_thread": runtimeThread,
    "jazz.span.level": String(span.level ?? ""),
    "jazz.span.target": String(span.target ?? ""),
    "jazz.span.fields": JSON.stringify(span.fields ?? {}),
  };
}

function unixNanoToTimeInput(value: unknown): TimeInput {
  const fallback = BigInt(Date.now()) * 1_000_000n;
  let nanoseconds = fallback;
  if (typeof value === "bigint") {
    nanoseconds = value;
  } else if (typeof value === "number" && Number.isFinite(value)) {
    nanoseconds = BigInt(Math.trunc(value));
  } else if (typeof value === "string" && /^[0-9]+$/.test(value)) {
    nanoseconds = BigInt(value);
  }

  const seconds = nanoseconds / 1_000_000_000n;
  const nanos = nanoseconds % 1_000_000_000n;
  return [Number(seconds), Number(nanos)];
}
