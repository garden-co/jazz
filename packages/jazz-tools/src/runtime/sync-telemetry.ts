import { resolveTelemetryCollectorUrlFromEnv } from "./telemetry-env.js";
export { resolveTelemetryCollectorUrlFromEnv } from "./telemetry-env.js";
import type { TimeInput, Tracer } from "@opentelemetry/api";

export const DEFAULT_TELEMETRY_COLLECTOR_URL = "http://localhost:4318";

export type TelemetryOptions = boolean | string;

type TelemetrySignal = "traces" | "logs";
type TelemetryAttributeValue = string | number | boolean;
type RuntimeThread = "main" | "worker";
export type WasmTraceEntry =
  | {
      kind: "span";
      sequence: number;
      level: string;
      target: string;
      name?: string;
      fields?: Record<string, unknown>;
      startUnixNano: TimeInput;
      endUnixNano: TimeInput;
    }
  | {
      kind: "log";
      sequence: number;
      level: string;
      target: string;
      message?: string;
      fields?: Record<string, unknown>;
      timestampUnixNano: TimeInput;
    }
  | {
      kind: "dropped";
      count: number;
    };

type WasmTelemetryModule = {
  setTraceEntryCollectionEnabled(enabled: boolean): void;
  drainTraceEntries(): WasmTraceEntry[];
  subscribeTraceEntries(callback: () => void): () => void;
};

type WasmTelemetryExporterState = {
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
  shutdown(): Promise<unknown> | unknown;
};

type WasmTelemetryCollectorIdentity = {
  traceUrl: string;
  logUrl: string;
};

type WasmTelemetryCollectorState = {
  identity: WasmTelemetryCollectorIdentity;
  runtimeThread: RuntimeThread;
  leases: Set<number>;
  nextLeaseId: number;
  closing: boolean;
  unsubscribe: (() => void) | null;
  exporter: Promise<WasmTelemetryExporterState> | null;
  exportTail: Promise<void>;
  shutdownScheduled: boolean;
  drainMicrotaskPending: boolean;
};

// The generated jazz-wasm namespace is the module-global identity. A broker
// worker can host several contexts in one realm, but the Rust queue and
// subscriber are still singleton resources for that namespace.
const wasmTelemetryCollectors = new WeakMap<WasmTelemetryModule, WasmTelemetryCollectorState>();

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
  wasmModule: unknown;
  collectorUrl?: string;
  runtimeThread: RuntimeThread;
}): () => void {
  if (!options.collectorUrl) return () => undefined;

  const identity = {
    traceUrl: normalizeOtlpEndpoint(options.collectorUrl, "traces"),
    logUrl: normalizeOtlpEndpoint(options.collectorUrl, "logs"),
  };
  const { runtimeThread, wasmModule } = options;

  if (!hasWasmTelemetryHooks(wasmModule)) {
    console.warn("[jazz] WASM telemetry unavailable: trace entry hooks are missing.");
    return () => undefined;
  }

  let state = wasmTelemetryCollectors.get(wasmModule);
  if (state) {
    if (state.closing) {
      throw new Error("WASM telemetry collector is closing");
    }
    if (state.runtimeThread !== runtimeThread) {
      throw new Error("incompatible WASM telemetry runtime thread");
    }
    if (
      state.identity.traceUrl !== identity.traceUrl ||
      state.identity.logUrl !== identity.logUrl
    ) {
      throw new Error("incompatible WASM telemetry collector");
    }
    return acquireCollectorLease(wasmModule, state);
  }
  state = {
    identity,
    runtimeThread,
    leases: new Set(),
    nextLeaseId: 0,
    closing: false,
    unsubscribe: null,
    exporter: null,
    exportTail: Promise.resolve(),
    shutdownScheduled: false,
    drainMicrotaskPending: false,
  };
  // Publish the rollback-capable state before invoking foreign hooks. A hook
  // failure must not leave a partially visible collector epoch behind.
  wasmTelemetryCollectors.set(wasmModule, state);

  const drain = () => {
    const entries = wasmModule.drainTraceEntries();
    if (!Array.isArray(entries) || entries.length === 0) return;
    enqueueWasmTelemetryExport(state!, entries);
  };
  const scheduleDrain = () => {
    if (state!.closing || state!.drainMicrotaskPending) return;
    state!.drainMicrotaskPending = true;
    queueMicrotask(() => {
      state!.drainMicrotaskPending = false;
      if (state!.closing) return;
      drain();
    });
  };

  let collectionEnableAttempted = false;
  try {
    state.unsubscribe = wasmModule.subscribeTraceEntries(scheduleDrain);
    collectionEnableAttempted = true;
    wasmModule.setTraceEntryCollectionEnabled(true);
    return acquireCollectorLease(wasmModule, state);
  } catch (error) {
    if (collectionEnableAttempted) {
      try {
        wasmModule.setTraceEntryCollectionEnabled(false);
      } catch {
        // Preserve the failure that prevented telemetry installation.
      }
    }
    try {
      state.unsubscribe?.();
    } catch {
      // Preserve the failure that prevented telemetry installation.
    }
    state.unsubscribe = null;
    if (wasmTelemetryCollectors.get(wasmModule) === state) {
      wasmTelemetryCollectors.delete(wasmModule);
    }
    throw error;
  }
}

function acquireCollectorLease(
  wasmModule: WasmTelemetryModule,
  state: WasmTelemetryCollectorState,
): () => void {
  const leaseId = state.nextLeaseId++;
  state.leases.add(leaseId);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    state.leases.delete(leaseId);
    if (state.leases.size > 0 || state.closing) return;

    state.closing = true;
    let teardownError: unknown;
    try {
      state.unsubscribe?.();
    } catch (error) {
      teardownError = error;
    } finally {
      try {
        // This is deliberately synchronous: entries accepted before closing
        // belong to this epoch even though export delivery remains async.
        const entries = wasmModule.drainTraceEntries();
        if (Array.isArray(entries) && entries.length > 0) {
          enqueueWasmTelemetryExport(state, entries);
        }
      } catch (error) {
        teardownError ??= error;
      } finally {
        try {
          wasmModule.setTraceEntryCollectionEnabled(false);
        } catch (error) {
          teardownError ??= error;
        } finally {
          state.unsubscribe = null;
          scheduleExporterShutdown(state);
          if (!teardownError && wasmTelemetryCollectors.get(wasmModule) === state) {
            wasmTelemetryCollectors.delete(wasmModule);
          }
        }
      }
    }
    if (teardownError) throw teardownError;
  };
}

function enqueueWasmTelemetryExport(
  state: WasmTelemetryCollectorState,
  entries: WasmTraceEntry[],
): void {
  state.exportTail = state.exportTail
    .catch(() => undefined)
    .then(async () => {
      if (!state.exporter) {
        state.exporter = createWasmTelemetryExporter(
          state.identity.traceUrl,
          state.identity.logUrl,
        );
      }
      try {
        const exporter = await state.exporter;
        for (const entry of entries) {
          try {
            recordWasmTelemetryEntry(exporter, state.runtimeThread, entry);
          } catch (error) {
            console.warn("[jazz] WASM telemetry export failed:", error);
          }
        }
      } catch (error) {
        state.exporter = null;
        console.warn("[jazz] WASM telemetry export failed:", error);
      }
    });
}

function scheduleExporterShutdown(state: WasmTelemetryCollectorState): void {
  if (state.shutdownScheduled) return;
  state.shutdownScheduled = true;
  // Always attach the shutdown continuation. Exporter creation is lazy and
  // can begin only after this release has already started.
  void state.exportTail
    .catch(() => undefined)
    .then(async () => {
      try {
        const exporter = state.exporter ? await state.exporter : undefined;
        await exporter?.shutdown();
      } catch (error) {
        console.warn("[jazz] WASM telemetry shutdown failed:", error);
      }
    });
}

function hasWasmTelemetryHooks(wasmModule: unknown): wasmModule is WasmTelemetryModule {
  if (!wasmModule || typeof wasmModule !== "object") return false;
  const hooks = wasmModule as Partial<WasmTelemetryModule>;
  return (
    typeof hooks.subscribeTraceEntries === "function" &&
    typeof hooks.drainTraceEntries === "function" &&
    typeof hooks.setTraceEntryCollectionEnabled === "function"
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
    shutdown: async () => {
      await Promise.all([
        typeof traceProvider.shutdown === "function" ? traceProvider.shutdown() : undefined,
        typeof loggerProvider.shutdown === "function" ? loggerProvider.shutdown() : undefined,
      ]);
    },
  };
}

function promotedFieldAttributes(
  fields: Record<string, unknown> | undefined,
): Record<string, TelemetryAttributeValue> {
  if (!fields || typeof fields !== "object") return {};
  const attributes: Record<string, TelemetryAttributeValue> = {};
  for (const [key, value] of Object.entries(fields)) {
    if (key.startsWith("jazz.")) continue;
    if (value === null || value === undefined) continue;
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
      attributes[key] = value;
    } else {
      attributes[key] = JSON.stringify(value);
    }
  }
  return attributes;
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

function stringifyFieldsOrEmpty(fields: Record<string, unknown> | undefined): string {
  return hasOwnProperties(fields) ? JSON.stringify(fields) : "{}";
}
