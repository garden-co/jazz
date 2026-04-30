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

interface PendingWasmTraceSpan {
  collectorUrl: string;
  appId: string;
  runtimeThread: "main" | "worker";
  span: WasmTraceSpan;
}

const MAX_WASM_TRACE_SPANS_PER_REQUEST = 256;
const MAX_PENDING_WASM_TRACE_SPANS = 5_000;

let pendingWasmTraceSpans: PendingWasmTraceSpan[] = [];
let wasmTraceFlushQueued = false;
let wasmTraceFlushInFlight = false;

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
  const processViteUrl =
    typeof process !== "undefined" && process.env
      ? process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL
      : undefined;
  const processNextPublicUrl =
    typeof process !== "undefined" && process.env
      ? process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL
      : undefined;
  const processPublicUrl =
    typeof process !== "undefined" && process.env
      ? process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL
      : undefined;
  const processExpoPublicUrl =
    typeof process !== "undefined" && process.env
      ? process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL
      : undefined;
  const metaEnv = (import.meta as ImportMetaWithEnv).env;

  return firstNonEmptyTelemetryUrl([
    processViteUrl,
    processNextPublicUrl,
    processPublicUrl,
    processExpoPublicUrl,
    metaEnv?.VITE_JAZZ_TELEMETRY_COLLECTOR_URL,
    metaEnv?.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
    metaEnv?.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
    metaEnv?.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
  ]);
}

function firstNonEmptyTelemetryUrl(values: Array<string | undefined>): string | undefined {
  for (const value of values) {
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
    void exportWasmTraceSpan(options.collectorUrl!, options.appId, options.runtimeThread, span);
  };
  globalRef.__JAZZ_WASM_TRACE_SPAN__ = callback;
  return () => {
    if (globalRef.__JAZZ_WASM_TRACE_SPAN__ === callback) {
      delete globalRef.__JAZZ_WASM_TRACE_SPAN__;
    }
  };
}

async function exportWasmTraceSpan(
  collectorUrl: string,
  appId: string,
  runtimeThread: "main" | "worker",
  span: WasmTraceSpan,
): Promise<void> {
  enqueueWasmTraceSpan({ collectorUrl, appId, runtimeThread, span });
}

function enqueueWasmTraceSpan(span: PendingWasmTraceSpan): void {
  if (pendingWasmTraceSpans.length >= MAX_PENDING_WASM_TRACE_SPANS) {
    pendingWasmTraceSpans.shift();
  }
  pendingWasmTraceSpans.push(span);

  if (wasmTraceFlushQueued || wasmTraceFlushInFlight) return;
  wasmTraceFlushQueued = true;
  queueMicrotask(() => {
    void flushWasmTraceSpans();
  });
}

async function flushWasmTraceSpans(): Promise<void> {
  if (wasmTraceFlushInFlight) return;
  wasmTraceFlushQueued = false;
  wasmTraceFlushInFlight = true;

  try {
    while (pendingWasmTraceSpans.length > 0) {
      const batch = takeNextWasmTraceSpanBatch();
      if (!batch) break;
      await exportWasmTraceSpanBatch(batch);
    }
  } finally {
    wasmTraceFlushInFlight = false;
    if (pendingWasmTraceSpans.length > 0) {
      wasmTraceFlushQueued = true;
      queueMicrotask(() => {
        void flushWasmTraceSpans();
      });
    }
  }
}

function takeNextWasmTraceSpanBatch(): PendingWasmTraceSpan[] | undefined {
  const first = pendingWasmTraceSpans[0];
  if (!first) return undefined;

  const batch: PendingWasmTraceSpan[] = [];
  for (let index = 0; index < pendingWasmTraceSpans.length; ) {
    const candidate = pendingWasmTraceSpans[index]!;
    if (
      candidate.collectorUrl === first.collectorUrl &&
      candidate.appId === first.appId &&
      candidate.runtimeThread === first.runtimeThread
    ) {
      batch.push(candidate);
      pendingWasmTraceSpans.splice(index, 1);
      if (batch.length >= MAX_WASM_TRACE_SPANS_PER_REQUEST) break;
      continue;
    }
    index += 1;
  }
  return batch;
}

async function exportWasmTraceSpanBatch(batch: PendingWasmTraceSpan[]): Promise<void> {
  const first = batch[0];
  if (!first) return;

  try {
    const exporter = await getWasmTraceExporter(first.collectorUrl, first.appId);
    for (const { runtimeThread, span } of batch) {
      const otelSpan = exporter.tracer.startSpan(String(span.name ?? "wasm span"), {
        kind: exporter.spanKindInternal,
        startTime: unixNanoToTimeInput(span.startUnixNano),
        attributes: wasmTraceTelemetryAttributes(runtimeThread, span),
      });
      otelSpan.end(unixNanoToTimeInput(span.endUnixNano));
    }
    await exporter.provider.forceFlush();
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
