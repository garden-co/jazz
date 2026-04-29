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
    await fetch(normalizeOtlpEndpoint(collectorUrl, "logs"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(buildLogRequest(record)),
    });
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
  try {
    const startUnixNano = stringOrNow(span.startUnixNano);
    const endUnixNano = stringOrNow(span.endUnixNano);
    await fetch(normalizeOtlpEndpoint(collectorUrl, "traces"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        resourceSpans: [
          {
            resource: {
              attributes: [
                otlpStringAttribute("service.name", "jazz-browser"),
                otlpStringAttribute("telemetry.sdk.language", "webjs"),
                otlpStringAttribute("jazz.app_id", appId),
              ],
            },
            scopeSpans: [
              {
                scope: { name: "jazz-wasm.tracing" },
                spans: [
                  {
                    traceId: randomHex(16),
                    spanId: randomHex(8),
                    name: String(span.name ?? "wasm span"),
                    kind: 1,
                    startTimeUnixNano: startUnixNano,
                    endTimeUnixNano: endUnixNano,
                    attributes: [
                      otlpStringAttribute("jazz.runtime_thread", runtimeThread),
                      otlpStringAttribute("jazz.span.level", String(span.level ?? "")),
                      otlpStringAttribute("jazz.span.target", String(span.target ?? "")),
                      otlpStringAttribute("jazz.span.fields", JSON.stringify(span.fields ?? {})),
                    ],
                  },
                ],
              },
            ],
          },
        ],
      }),
    });
  } catch {
    // Silent by design.
  }
}

function buildLogRequest(record: SyncPayloadTelemetryRecord): unknown {
  return {
    resourceLogs: [
      {
        resource: {
          attributes: [
            otlpStringAttribute("service.name", "jazz-browser"),
            otlpStringAttribute("telemetry.sdk.language", "webjs"),
          ],
        },
        scopeLogs: [
          {
            scope: { name: "jazz-browser.sync-payload" },
            logRecords: [
              {
                timeUnixNano: nowUnixNanoString(),
                severityNumber: 5,
                severityText: record.severityText,
                body: { stringValue: JSON.stringify(record) },
                attributes: ATTRIBUTE_KEYS.flatMap(([field, key]) =>
                  otlpAttributeFromValue(key, record[field]),
                ),
              },
            ],
          },
        ],
      },
    ],
  };
}

function otlpAttributeFromValue(key: string, value: unknown): unknown[] {
  if (value === undefined || value === null) return [];
  if (typeof value === "number") return [{ key, value: { intValue: String(value) } }];
  if (typeof value === "boolean") return [{ key, value: { boolValue: value } }];
  return [otlpStringAttribute(key, String(value))];
}

function otlpStringAttribute(key: string, value: string): unknown {
  return { key, value: { stringValue: value } };
}

function payloadByteLength(payload: Uint8Array | string): number {
  if (typeof payload === "string") return new TextEncoder().encode(payload).byteLength;
  return payload.byteLength;
}

function nowUnixNanoString(): string {
  return (BigInt(Date.now()) * 1_000_000n).toString();
}

function stringOrNow(value: unknown): string {
  return typeof value === "string" && value.length > 0 ? value : nowUnixNanoString();
}

function randomHex(bytes: number): string {
  const values = new Uint8Array(bytes);
  crypto.getRandomValues(values);
  return Array.from(values, (value) => value.toString(16).padStart(2, "0")).join("");
}
