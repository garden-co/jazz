import type { TimeInput, Tracer } from "@opentelemetry/api";

export const DEFAULT_TELEMETRY_COLLECTOR_URL = "http://localhost:4318";

export type TelemetryOptions = boolean | string;

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
// SpanKind.INTERNAL — inlined to avoid a dynamic import of @opentelemetry/api.
const SPAN_KIND_INTERNAL = 1;

let cachedExporter: Promise<{ tracer: Tracer }> | null = null;
let warnedOnExportFailure = false;

type ImportMetaWithEnv = ImportMeta & {
  env?: Record<string, string | undefined>;
};

export function resolveTelemetryCollectorUrl(
  telemetry: TelemetryOptions | undefined,
): string | undefined {
  if (telemetry === true) return DEFAULT_TELEMETRY_COLLECTOR_URL;
  if (typeof telemetry !== "string") return undefined;
  return telemetry.trim() || undefined;
}

// Bundlers (Vite, Next/Webpack DefinePlugin, esbuild) only inline
// `process.env.X` / `import.meta.env.X` when both the object chain and the
// property name are literal in the source — computed keys, aliased env
// objects, and dynamic indexing all defeat static replacement.
export function resolveTelemetryCollectorUrlFromEnv(): string | undefined {
  const hasProcess = typeof process !== "undefined";
  return (
    trim(hasProcess ? process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim(hasProcess ? process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL : undefined) ??
    trim((import.meta as ImportMetaWithEnv).env?.VITE_JAZZ_TELEMETRY_COLLECTOR_URL)
  );
}

function trim(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
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
  cachedExporter = null;
  warnedOnExportFailure = false;
  const url = normalizeOtlpEndpoint(options.collectorUrl, "traces");
  const { appId, runtimeThread } = options;
  const globalRef = globalThis as Record<string, unknown>;
  const callback = (span: WasmTraceSpan) => {
    void recordWasmTraceSpan(url, appId, runtimeThread, span);
  };
  globalRef.__JAZZ_WASM_TRACE_SPAN__ = callback;
  return () => {
    if (globalRef.__JAZZ_WASM_TRACE_SPAN__ === callback) {
      delete globalRef.__JAZZ_WASM_TRACE_SPAN__;
    }
  };
}

async function recordWasmTraceSpan(
  url: string,
  appId: string,
  runtimeThread: "main" | "worker",
  span: WasmTraceSpan,
): Promise<void> {
  try {
    if (!cachedExporter) cachedExporter = createWasmTraceExporter(url, appId);
    const { tracer } = await cachedExporter;
    const otelSpan = tracer.startSpan(String(span.name ?? "wasm span"), {
      kind: SPAN_KIND_INTERNAL,
      startTime: unixNanoToTimeInput(span.startUnixNano),
      attributes: {
        "jazz.runtime_thread": runtimeThread,
        "jazz.span.level": String(span.level ?? ""),
        "jazz.span.target": String(span.target ?? ""),
        "jazz.span.fields": String(span.fields ?? ""),
      },
    });
    otelSpan.end(unixNanoToTimeInput(span.endUnixNano));
  } catch (error) {
    if (!warnedOnExportFailure) {
      warnedOnExportFailure = true;
      console.warn("[jazz] WASM trace telemetry export failed:", error);
    }
  }
}

async function createWasmTraceExporter(url: string, appId: string): Promise<{ tracer: Tracer }> {
  const [
    { OTLPTraceExporter },
    { BasicTracerProvider, BatchSpanProcessor },
    { resourceFromAttributes },
  ] = await Promise.all([
    import("@opentelemetry/exporter-trace-otlp-http"),
    import("@opentelemetry/sdk-trace-base"),
    import("@opentelemetry/resources"),
  ]);
  const provider = new BasicTracerProvider({
    resource: resourceFromAttributes({
      "service.name": "jazz-browser",
      "telemetry.sdk.language": "webjs",
      "jazz.app_id": appId,
    }),
    spanProcessors: [
      new BatchSpanProcessor(new OTLPTraceExporter({ url }), {
        maxExportBatchSize: MAX_WASM_TRACE_SPANS_PER_REQUEST,
        maxQueueSize: MAX_PENDING_WASM_TRACE_SPANS,
        scheduledDelayMillis: 1_000,
      }),
    ],
  });
  return { tracer: provider.getTracer("jazz-wasm.tracing") };
}

function unixNanoToTimeInput(value: unknown): TimeInput {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    throw new TypeError(`expected nanosecond string, got ${typeof value}: ${String(value)}`);
  }
  const nanoseconds = BigInt(value);
  const seconds = nanoseconds / 1_000_000_000n;
  const nanos = nanoseconds % 1_000_000_000n;
  return [Number(seconds), Number(nanos)];
}
