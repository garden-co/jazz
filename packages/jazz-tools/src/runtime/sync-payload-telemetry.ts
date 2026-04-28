import { decodeSyncPayloadForTelemetry } from "jazz-wasm";

export type SyncPayloadTelemetryScope = "worker_bridge" | "websocket";
export type SyncPayloadTelemetryDirection =
  | "main_to_worker"
  | "worker_to_main"
  | "client_to_server"
  | "server_to_client";

export type SyncPayloadTelemetryOptions = {
  appId: string;
  collectorUrl: string;
};

export type BuildSyncPayloadTelemetryRecordOptions = SyncPayloadTelemetryOptions & {
  scope: SyncPayloadTelemetryScope;
  direction: SyncPayloadTelemetryDirection;
  clientId?: string | null;
  sequence?: number | null;
  sourceFrameId?: string | null;
  sourcePayloadIndex?: number | null;
  sourcePayloadCount?: number | null;
  sourceFrameBytes?: number | null;
};

export function buildSyncPayloadTelemetryRecords(
  payload: Uint8Array | string,
  options: BuildSyncPayloadTelemetryRecordOptions,
): Record<string, unknown>[] {
  const decoded = decodeSyncPayloadForTelemetry(payload);
  const common = buildCommonRecord(payload, options);

  return decoded.records.map((fields) => ({
    ...common,
    ...fields,
    ...(decoded.logBody == null ? {} : { logBody: decoded.logBody }),
  }));
}

export function buildSyncPayloadTelemetryDecodeFailureRecord(
  payload: Uint8Array | string,
  error: unknown,
  options: BuildSyncPayloadTelemetryRecordOptions,
): Record<string, unknown> {
  return {
    ...buildCommonRecord(payload, options),
    decodeError: error instanceof Error ? error.message : String(error),
  };
}

export function sendSyncPayloadTelemetryRecords(
  collectorUrl: string | undefined,
  records: Record<string, unknown>[],
): void {
  if (!collectorUrl || records.length === 0 || typeof fetch !== "function") return;

  for (const record of records) {
    void fetch(normalizeOtlpLogsUrl(collectorUrl), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(buildOtlpLogsBody(record)),
    }).catch(() => undefined);
  }
}

export function postSyncPayloadTelemetry(
  payload: Uint8Array | string,
  options: BuildSyncPayloadTelemetryRecordOptions,
): void {
  if (!options.collectorUrl || typeof fetch !== "function") return;

  const records = buildRecordsOrDecodeFailure(payload, options);
  sendSyncPayloadTelemetryRecords(options.collectorUrl, records);
}

function buildRecordsOrDecodeFailure(
  payload: Uint8Array | string,
  options: BuildSyncPayloadTelemetryRecordOptions,
): Record<string, unknown>[] {
  try {
    return buildSyncPayloadTelemetryRecords(payload, options);
  } catch (error) {
    return [buildSyncPayloadTelemetryDecodeFailureRecord(payload, error, options)];
  }
}

function buildCommonRecord(
  payload: Uint8Array | string,
  options: BuildSyncPayloadTelemetryRecordOptions,
): Record<string, unknown> {
  return dropUndefined({
    appId: options.appId,
    severityText: "DEBUG",
    scope: options.scope,
    direction: options.direction,
    clientId: options.clientId,
    sequence: options.sequence,
    sourceFrameId: options.sourceFrameId,
    sourcePayloadIndex: options.sourcePayloadIndex,
    sourcePayloadCount: options.sourcePayloadCount,
    sourceFrameBytes: options.sourceFrameBytes,
    messageBytes: messageBytes(payload),
    messageEncoding: typeof payload === "string" ? "utf8" : "binary",
    recordedAt: Date.now(),
  });
}

function messageBytes(payload: Uint8Array | string): number {
  if (typeof payload !== "string") return payload.byteLength;
  if (typeof TextEncoder === "undefined") return payload.length;
  return new TextEncoder().encode(payload).byteLength;
}

function dropUndefined(record: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(record).filter(([, value]) => value !== undefined && value !== null),
  );
}

function normalizeOtlpLogsUrl(collectorUrl: string): string {
  const trimmed = collectorUrl.replace(/\/+$/, "");
  return trimmed.endsWith("/v1/logs") ? trimmed : `${trimmed}/v1/logs`;
}

function buildOtlpLogsBody(record: Record<string, unknown>): Record<string, unknown> {
  return {
    resourceLogs: [
      {
        resource: {
          attributes: [
            otlpAttribute("service.name", "jazz-browser"),
            otlpAttribute("telemetry.sdk.language", "webjs"),
          ],
        },
        scopeLogs: [
          {
            scope: { name: "jazz-browser.sync-payload" },
            logRecords: [
              {
                timeUnixNano: String(BigInt(Number(record.recordedAt ?? Date.now())) * 1_000_000n),
                severityNumber: 5,
                severityText: "DEBUG",
                body: { stringValue: JSON.stringify(record) },
                attributes: recordAttributes(record),
              },
            ],
          },
        ],
      },
    ],
  };
}

function recordAttributes(record: Record<string, unknown>): Record<string, unknown>[] {
  const attributes: Record<string, unknown>[] = [];
  pushString(attributes, "jazz.app_id", record.appId);
  pushString(attributes, "jazz.scope", record.scope);
  pushString(attributes, "jazz.direction", record.direction);
  pushString(attributes, "jazz.client_id", record.clientId);
  pushString(attributes, "jazz.connection_id", record.connectionId);
  pushInt(attributes, "jazz.sequence", record.sequence);
  pushString(attributes, "jazz.source_frame_id", record.sourceFrameId);
  pushInt(attributes, "jazz.source_payload_index", record.sourcePayloadIndex);
  pushInt(attributes, "jazz.source_payload_count", record.sourcePayloadCount);
  pushInt(attributes, "jazz.source_frame_bytes", record.sourceFrameBytes);
  pushInt(attributes, "jazz.message_bytes", record.messageBytes);
  pushString(attributes, "jazz.message_encoding", record.messageEncoding);
  pushString(attributes, "jazz.decode_error", record.decodeError);
  pushString(attributes, "jazz.payload_variant", record.payloadVariant);
  pushString(attributes, "jazz.row_id", record.rowId);
  pushString(attributes, "jazz.table_name", record.tableName);
  pushString(attributes, "jazz.table_name_error", record.tableNameError);
  pushString(attributes, "jazz.branch_name", record.branchName);
  pushString(attributes, "jazz.batch_id", record.batchId);
  pushInt(attributes, "jazz.query_id", record.queryId);
  pushString(attributes, "jazz.schema_hash", record.schemaHash);
  pushString(attributes, "jazz.schema_hash_error", record.schemaHashError);
  pushString(attributes, "jazz.durability_tier", record.durabilityTier);
  pushString(attributes, "jazz.error_variant", record.errorVariant);
  pushString(attributes, "jazz.error_code", record.errorCode);
  pushInt(attributes, "jazz.member_index", record.memberIndex);
  pushInt(attributes, "jazz.member_count", record.memberCount);
  return attributes;
}

function pushString(attributes: Record<string, unknown>[], key: string, value: unknown): void {
  if (typeof value !== "string") return;
  attributes.push(otlpAttribute(key, value));
}

function pushInt(attributes: Record<string, unknown>[], key: string, value: unknown): void {
  if (typeof value !== "number") return;
  attributes.push({ key, value: { intValue: String(value) } });
}

function otlpAttribute(key: string, value: string): Record<string, unknown> {
  return { key, value: { stringValue: value } };
}
