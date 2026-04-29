import type { Logger } from "@opentelemetry/api-logs";
import type { SpanKind, TimeInput, Tracer } from "@opentelemetry/api";
import type { LoggerProvider } from "@opentelemetry/sdk-logs";
import type { BasicTracerProvider } from "@opentelemetry/sdk-trace-base";

export const DEFAULT_TELEMETRY_COLLECTOR_URL = "http://localhost:4318";

export type TelemetryOptions = boolean | { collectorUrl?: string };

export type SyncPayloadTelemetryScope = "worker_bridge" | "websocket";
export type SyncPayloadTelemetryDirection =
  | "main_to_worker"
  | "worker_to_main"
  | "client_to_server"
  | "server_to_client";

export interface SyncPayloadTelemetryRecord {
  appId?: string;
  severityText: "DEBUG";
  scope: SyncPayloadTelemetryScope;
  direction: SyncPayloadTelemetryDirection;
  clientId?: string;
  connectionId?: string;
  sequence?: number;
  sourceFrameId?: string;
  sourcePayloadIndex?: number;
  sourcePayloadCount?: number;
  sourceFrameBytes?: number;
  messageBytes?: number;
  messageEncoding?: "binary" | "utf8" | string;
  recordedAt?: string;
  decodeError?: string;
  logBody?: unknown;
  payloadVariant?: string;
  rowId?: string;
  tableName?: string;
  tableNameError?: string;
  branchName?: string;
  batchId?: string;
  queryId?: string;
  schemaHash?: string;
  schemaHashError?: string;
  durabilityTier?: string;
  errorVariant?: string;
  errorCode?: string;
  memberIndex?: number;
  memberCount?: number;
}

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

interface SyncPayloadLogExporterState {
  logger: Logger;
  provider: LoggerProvider;
  severityDebug: number;
}

interface WasmTraceExporterState {
  provider: BasicTracerProvider;
  tracer: Tracer;
  spanKindInternal: SpanKind;
}

const syncPayloadLogExporters = new Map<string, Promise<SyncPayloadLogExporterState>>();
const wasmTraceExporters = new Map<string, Promise<WasmTraceExporterState>>();

const TELEMETRY_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
];

type ImportMetaWithEnv = ImportMeta & {
  env?: Record<string, string | undefined>;
};

const ATTRIBUTE_KEYS: Array<[keyof SyncPayloadTelemetryRecord, string]> = [
  ["appId", "jazz.app_id"],
  ["scope", "jazz.scope"],
  ["direction", "jazz.direction"],
  ["clientId", "jazz.client_id"],
  ["connectionId", "jazz.connection_id"],
  ["sequence", "jazz.sequence"],
  ["sourceFrameId", "jazz.source_frame_id"],
  ["sourcePayloadIndex", "jazz.source_payload_index"],
  ["sourcePayloadCount", "jazz.source_payload_count"],
  ["sourceFrameBytes", "jazz.source_frame_bytes"],
  ["messageBytes", "jazz.message_bytes"],
  ["messageEncoding", "jazz.message_encoding"],
  ["decodeError", "jazz.decode_error"],
  ["payloadVariant", "jazz.payload_variant"],
  ["rowId", "jazz.row_id"],
  ["tableName", "jazz.table_name"],
  ["tableNameError", "jazz.table_name_error"],
  ["branchName", "jazz.branch_name"],
  ["batchId", "jazz.batch_id"],
  ["queryId", "jazz.query_id"],
  ["schemaHash", "jazz.schema_hash"],
  ["schemaHashError", "jazz.schema_hash_error"],
  ["durabilityTier", "jazz.durability_tier"],
  ["errorVariant", "jazz.error_variant"],
  ["errorCode", "jazz.error_code"],
  ["memberIndex", "jazz.member_index"],
  ["memberCount", "jazz.member_count"],
];

export function resolveTelemetryCollectorUrl(
  telemetry: TelemetryOptions | undefined,
): string | undefined {
  if (telemetry === true) return DEFAULT_TELEMETRY_COLLECTOR_URL;
  if (telemetry === false || telemetry === undefined) return undefined;
  const collectorUrl = telemetry.collectorUrl?.trim();
  return collectorUrl || DEFAULT_TELEMETRY_COLLECTOR_URL;
}

export function resolveTelemetryCollectorUrlFromEnv(): string | undefined {
  for (const key of TELEMETRY_ENV_KEYS) {
    const value = readPublicEnv(key)?.trim();
    if (value) return value;
  }
  return undefined;
}

function readPublicEnv(key: string): string | undefined {
  if (typeof process !== "undefined" && process.env) {
    const value = process.env[key];
    if (value !== undefined) return value;
  }

  return (import.meta as ImportMetaWithEnv).env?.[key];
}

export function normalizeOtlpEndpoint(collectorUrl: string, signal: "logs" | "traces"): string {
  const trimmed = collectorUrl.trim().replace(/\/+$/, "");
  const suffix = signal === "logs" ? "/v1/logs" : "/v1/traces";
  if (trimmed.endsWith("/v1/logs")) {
    return signal === "logs" ? trimmed : `${trimmed.slice(0, -"/v1/logs".length)}${suffix}`;
  }
  if (trimmed.endsWith("/v1/traces")) {
    return signal === "traces" ? trimmed : `${trimmed.slice(0, -"/v1/traces".length)}${suffix}`;
  }
  return `${trimmed}${suffix}`;
}

export async function exportSyncPayloadTelemetryRecord(
  collectorUrl: string,
  record: SyncPayloadTelemetryRecord,
): Promise<void> {
  try {
    const exporter = await getSyncPayloadLogExporter(collectorUrl);
    exporter.logger.emit({
      timestamp: record.recordedAt ? new Date(record.recordedAt) : new Date(),
      observedTimestamp: new Date(),
      severityNumber: exporter.severityDebug,
      severityText: record.severityText,
      body: JSON.stringify(record),
      attributes: syncPayloadTelemetryAttributes(record),
    });
    await exporter.provider.forceFlush();
  } catch {
    // Dev telemetry must never make sync paths noisy or fragile.
  }
}

export function observeSyncPayloadsForTelemetry(options: {
  collectorUrl?: string;
  appId: string;
  scope: SyncPayloadTelemetryScope;
  direction: SyncPayloadTelemetryDirection;
  payloads: (Uint8Array | string)[];
}): void {
  if (!options.collectorUrl || options.payloads.length === 0) return;
  const sourcePayloadCount = options.payloads.length;
  for (const [sourcePayloadIndex, payload] of options.payloads.entries()) {
    const telemetryPayload = typeof payload === "string" ? payload : new Uint8Array(payload);
    void exportObservedPayload(
      options.collectorUrl,
      {
        appId: options.appId,
        severityText: "DEBUG",
        scope: options.scope,
        direction: options.direction,
        sourcePayloadIndex,
        sourcePayloadCount,
        messageBytes: payloadByteLength(payload),
        messageEncoding: typeof payload === "string" ? "utf8" : "binary",
      },
      telemetryPayload,
    );
  }
}

export function installWasmTraceTelemetry(options: {
  collectorUrl?: string;
  appId: string;
  runtimeThread: "main" | "worker";
}): void {
  if (!options.collectorUrl) return;
  const globalRef = globalThis as Record<string, unknown>;
  globalRef.__JAZZ_WASM_TRACE_SPAN__ = (span: WasmTraceSpan) => {
    void exportWasmTraceSpan(options.collectorUrl!, options.appId, options.runtimeThread, span);
  };
}

async function exportObservedPayload(
  collectorUrl: string,
  baseRecord: SyncPayloadTelemetryRecord,
  payload: Uint8Array | string,
): Promise<void> {
  const records = await decodePayloadTelemetryRecords(baseRecord, payload);
  for (const record of records) {
    await exportSyncPayloadTelemetryRecord(collectorUrl, record);
  }
}

async function decodePayloadTelemetryRecords(
  baseRecord: SyncPayloadTelemetryRecord,
  payload: Uint8Array | string,
): Promise<SyncPayloadTelemetryRecord[]> {
  try {
    const wasmModule = await import("jazz-wasm");
    const decode = (wasmModule as Record<string, unknown>).decodeSyncPayloadTelemetry;
    if (typeof decode !== "function") {
      throw new Error("decodeSyncPayloadTelemetry is unavailable");
    }
    const result = decode(payload) as { records?: Array<Partial<SyncPayloadTelemetryRecord>> };
    const records = Array.isArray(result.records) ? result.records : [];
    if (records.length === 0) {
      return [{ ...baseRecord, severityText: "DEBUG", recordedAt: new Date().toISOString() }];
    }
    return records.map((record) => ({
      ...record,
      ...baseRecord,
      severityText: "DEBUG",
      recordedAt: new Date().toISOString(),
    }));
  } catch (error) {
    return [
      {
        ...baseRecord,
        severityText: "DEBUG",
        recordedAt: new Date().toISOString(),
        decodeError: error instanceof Error ? error.message : String(error),
      },
    ];
  }
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

function getSyncPayloadLogExporter(collectorUrl: string): Promise<SyncPayloadLogExporterState> {
  const url = normalizeOtlpEndpoint(collectorUrl, "logs");
  const cached = syncPayloadLogExporters.get(url);
  if (cached) return cached;

  const created = createSyncPayloadLogExporter(url).catch((error) => {
    syncPayloadLogExporters.delete(url);
    throw error;
  });
  syncPayloadLogExporters.set(url, created);
  return created;
}

async function createSyncPayloadLogExporter(url: string): Promise<SyncPayloadLogExporterState> {
  const [
    { OTLPLogExporter },
    { LoggerProvider, SimpleLogRecordProcessor },
    { SeverityNumber },
    { resourceFromAttributes },
  ] = await Promise.all([
    import("@opentelemetry/exporter-logs-otlp-http"),
    import("@opentelemetry/sdk-logs"),
    import("@opentelemetry/api-logs"),
    import("@opentelemetry/resources"),
  ]);
  const otlpExporter = new OTLPLogExporter({ url });
  const provider = new LoggerProvider({
    resource: resourceFromAttributes({
      "service.name": "jazz-browser",
      "telemetry.sdk.language": "webjs",
    }),
    processors: [new SimpleLogRecordProcessor(otlpExporter)],
  });
  return {
    provider,
    logger: provider.getLogger("jazz-browser.sync-payload"),
    severityDebug: SeverityNumber.DEBUG,
  };
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

function syncPayloadTelemetryAttributes(
  record: SyncPayloadTelemetryRecord,
): Record<string, TelemetryAttributeValue> {
  const attributes: Record<string, TelemetryAttributeValue> = {};
  for (const [field, key] of ATTRIBUTE_KEYS) {
    const value = record[field];
    if (value === undefined || value === null) continue;
    attributes[key] =
      typeof value === "number" || typeof value === "boolean" ? value : String(value);
  }
  return attributes;
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

function payloadByteLength(payload: Uint8Array | string): number {
  if (typeof payload === "string") return new TextEncoder().encode(payload).byteLength;
  return payload.byteLength;
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
