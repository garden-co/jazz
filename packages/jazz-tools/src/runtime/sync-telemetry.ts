import type { TimeInput, Tracer } from "@opentelemetry/api";
import type { WasmTraceEntry } from "jazz-wasm";

export const DEFAULT_TELEMETRY_COLLECTOR_URL = "http://localhost:4318";

export type TelemetryOptions = boolean | string;

type TelemetrySignal = "traces" | "logs";
type TelemetryAttributeValue = string | number | boolean;
type RuntimeThread = "main" | "worker";
type ImportMetaWithEnv = ImportMeta & {
  env?: Record<string, string | undefined>;
};

type WasmTelemetryModule = {
  setTraceEntryCollectionEnabled(enabled: boolean): void;
  drainTraceEntries(): WasmTraceEntry[];
  subscribeTraceEntries(callback: () => void): () => void;
};

interface WasmTelemetryExporterState {
  tracer: Tracer;
  logger: {
    emit(record: {
      timestamp?: TimeInput;
      severityNumber?: number;
      severityText?: string;
      body?: string;
      attributes?: Record<string, TelemetryAttributeValue>;
    }): void;
  };
}

const MAX_WASM_TELEMETRY_EXPORT_BATCH_SIZE = 256;
const MAX_PENDING_WASM_TELEMETRY_RECORDS = 5_000;
// SpanKind.INTERNAL — inlined to avoid a dynamic import of @opentelemetry/api.
const SPAN_KIND_INTERNAL = 1;
const SEVERITY_NUMBER = {
  TRACE: 1,
  DEBUG: 5,
  INFO: 9,
  WARN: 13,
  ERROR: 17,
} as const;

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

export function normalizeOtlpEndpoint(collectorUrl: string, signal: TelemetrySignal): string {
  const trimmed = collectorUrl.trim().replace(/\/+$/, "");
  const suffix = `/v1/${signal}`;
  if (trimmed.endsWith("/v1/logs")) {
    return `${trimmed.slice(0, -"/v1/logs".length)}${suffix}`;
  }
  if (trimmed.endsWith("/v1/traces")) {
    return `${trimmed.slice(0, -"/v1/traces".length)}${suffix}`;
  }
  return `${trimmed}${suffix}`;
}

export function installWasmTelemetry(options: {
  wasmModule: WasmTelemetryModule;
  collectorUrl?: string;
  appId: string;
  runtimeThread: RuntimeThread;
}): () => void {
  if (!options.collectorUrl) return () => undefined;

  const traceUrl = normalizeOtlpEndpoint(options.collectorUrl, "traces");
  const logUrl = normalizeOtlpEndpoint(options.collectorUrl, "logs");
  const { appId, runtimeThread, wasmModule } = options;

  if (!hasWasmTelemetryHooks(wasmModule)) {
    console.warn("[jazz] WASM telemetry unavailable: trace entry hooks are missing.");
    return () => undefined;
  }

  let cachedExporter: Promise<WasmTelemetryExporterState> | null = null;
  let warnedOnExportFailure = false;
  const warnOnce = (error: unknown) => {
    if (warnedOnExportFailure) return;
    warnedOnExportFailure = true;
    console.warn("[jazz] WASM telemetry export failed:", error);
  };

  const exportEntries = async (entries: WasmTraceEntry[]): Promise<void> => {
    if (!cachedExporter) cachedExporter = createWasmTelemetryExporter(traceUrl, logUrl, appId);
    let exporter: WasmTelemetryExporterState;
    try {
      exporter = await cachedExporter;
    } catch (error) {
      cachedExporter = null;
      warnOnce(error);
      return;
    }
    for (const entry of entries) {
      try {
        recordWasmTelemetryEntry(exporter, runtimeThread, entry);
      } catch (error) {
        warnOnce(error);
      }
    }
  };

  let disposed = false;
  let drainMicrotaskPending = false;

  const drain = () => {
    const entries = wasmModule.drainTraceEntries();
    if (!Array.isArray(entries) || entries.length === 0) return;
    void exportEntries(entries);
  };

  const scheduleDrain = () => {
    if (disposed || drainMicrotaskPending) return;
    drainMicrotaskPending = true;
    queueMicrotask(() => {
      drainMicrotaskPending = false;
      if (disposed) return;
      drain();
    });
  };

  const unsubscribeTraceEntries = wasmModule.subscribeTraceEntries(scheduleDrain);
  wasmModule.setTraceEntryCollectionEnabled(true);

  return () => {
    if (disposed) return;
    disposed = true;
    unsubscribeTraceEntries();
    drain();
    wasmModule.setTraceEntryCollectionEnabled(false);
  };
}

function hasWasmTelemetryHooks(wasmModule: WasmTelemetryModule): boolean {
  return (
    typeof wasmModule.subscribeTraceEntries === "function" &&
    typeof wasmModule.drainTraceEntries === "function" &&
    typeof wasmModule.setTraceEntryCollectionEnabled === "function"
  );
}

function recordWasmTelemetryEntry(
  exporter: WasmTelemetryExporterState,
  runtimeThread: RuntimeThread,
  entry: WasmTraceEntry,
): void {
  if (entry.kind === "span") {
    const baseAttrs: Record<string, TelemetryAttributeValue> = {
      "jazz.runtime_thread": runtimeThread,
      "jazz.span.sequence": entry.sequence,
      "jazz.span.level": entry.level,
      "jazz.span.target": entry.target,
      "jazz.span.fields": stringifyFieldsOrEmpty(entry.fields),
    };
    const otelSpan = exporter.tracer.startSpan(entry.name || "wasm span", {
      kind: SPAN_KIND_INTERNAL,
      startTime: entry.startUnixNano,
      attributes: hasOwnProperties(entry.fields)
        ? { ...promotedFieldAttributes(entry.fields), ...baseAttrs }
        : baseAttrs,
    });
    otelSpan.end(entry.endUnixNano);
    return;
  }

  if (entry.kind === "log") {
    const baseAttrs: Record<string, TelemetryAttributeValue> = {
      "jazz.runtime_thread": runtimeThread,
      "jazz.log.sequence": entry.sequence,
      "jazz.log.target": entry.target,
      "jazz.log.fields": stringifyFieldsOrEmpty(entry.fields),
    };
    exporter.logger.emit({
      timestamp: entry.timestampUnixNano,
      severityNumber: severityNumber(entry.level),
      severityText: entry.level,
      body: entry.message,
      attributes: hasOwnProperties(entry.fields)
        ? { ...promotedFieldAttributes(entry.fields), ...baseAttrs }
        : baseAttrs,
    });
    return;
  }

  exporter.logger.emit({
    severityNumber: SEVERITY_NUMBER.WARN,
    severityText: "WARN",
    body: `Dropped ${entry.count} WASM telemetry records`,
    attributes: {
      "jazz.runtime_thread": runtimeThread,
      "jazz.telemetry.dropped_count": entry.count,
    },
  });
}

async function createWasmTelemetryExporter(
  traceUrl: string,
  logUrl: string,
  appId: string,
): Promise<WasmTelemetryExporterState> {
  const [
    { OTLPTraceExporter },
    { BasicTracerProvider, BatchSpanProcessor },
    { OTLPLogExporter },
    { LoggerProvider, BatchLogRecordProcessor },
    { resourceFromAttributes },
  ] = await Promise.all([
    import("@opentelemetry/exporter-trace-otlp-http"),
    import("@opentelemetry/sdk-trace-base"),
    import("@opentelemetry/exporter-logs-otlp-http"),
    import("@opentelemetry/sdk-logs"),
    import("@opentelemetry/resources"),
  ]);
  const resource = resourceFromAttributes({
    "service.name": "jazz-browser",
    "telemetry.sdk.language": "webjs",
    "jazz.app_id": appId,
  });
  const batchOptions = {
    maxExportBatchSize: MAX_WASM_TELEMETRY_EXPORT_BATCH_SIZE,
    maxQueueSize: MAX_PENDING_WASM_TELEMETRY_RECORDS,
    scheduledDelayMillis: 1_000,
  };
  const traceProvider = new BasicTracerProvider({
    resource,
    spanProcessors: [
      new BatchSpanProcessor(new OTLPTraceExporter({ url: traceUrl }), batchOptions),
    ],
  });
  const loggerProvider = new LoggerProvider({
    resource,
    processors: [new BatchLogRecordProcessor(new OTLPLogExporter({ url: logUrl }), batchOptions)],
  });

  return {
    tracer: traceProvider.getTracer("jazz-wasm.tracing"),
    logger: loggerProvider.getLogger("jazz-wasm.tracing"),
  };
}

// Promote each tracing field into a top-level OTel attribute so spans from the
// browser line up with `tracing-opentelemetry`-emitted server spans (which put
// `payload`, `peer_kind`, etc. directly on `SpanAttributes`). The aggregated
// `jazz.span.fields` JSON blob is still emitted alongside for back-compat.
//
// Spread these BEFORE the reserved `jazz.*` keys so a colliding user field
// can't shadow them.
function promotedFieldAttributes(
  fields: Record<string, unknown> | undefined,
): Record<string, TelemetryAttributeValue> {
  if (!fields || typeof fields !== "object") return {};
  const out: Record<string, TelemetryAttributeValue> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (key.startsWith("jazz.")) continue; // reserved namespace
    if (value === null || value === undefined) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      out[key] = value;
    } else {
      out[key] = JSON.stringify(value);
    }
  }
  return out;
}

function severityNumber(level: string): number {
  switch (level.toUpperCase()) {
    case "TRACE":
      return SEVERITY_NUMBER.TRACE;
    case "DEBUG":
      return SEVERITY_NUMBER.DEBUG;
    case "INFO":
      return SEVERITY_NUMBER.INFO;
    case "WARN":
    case "WARNING":
      return SEVERITY_NUMBER.WARN;
    case "ERROR":
      return SEVERITY_NUMBER.ERROR;
    default:
      return 0;
  }
}

function hasOwnProperties(fields: Record<string, unknown> | undefined): boolean {
  if (!fields) return false;
  for (const _key in fields) return true;
  return false;
}

// Skip the JSON.stringify call entirely when fields is missing/empty — most
// hot-path log entries (one per insert) carry only `message`, no fields.
function stringifyFieldsOrEmpty(fields: Record<string, unknown> | undefined): string {
  return hasOwnProperties(fields) ? JSON.stringify(fields) : "{}";
}
