import { decodeSyncPayloadForTelemetry } from "jazz-wasm";

export type SyncPayloadTelemetryScope = "worker_bridge" | "websocket";
export type SyncPayloadTelemetryDirection =
  | "main_to_worker"
  | "worker_to_main"
  | "client_to_server"
  | "server_to_client";

export type SyncPayloadTelemetryOptions = {
  appId: string;
  ingestUrl: string;
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
  ingestUrl: string | undefined,
  records: Record<string, unknown>[],
): void {
  if (!ingestUrl || records.length === 0 || typeof fetch !== "function") return;

  for (const record of records) {
    void fetch(ingestUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(record),
    }).catch(() => undefined);
  }
}

export function postSyncPayloadTelemetry(
  payload: Uint8Array | string,
  options: BuildSyncPayloadTelemetryRecordOptions,
): void {
  if (!options.ingestUrl || typeof fetch !== "function") return;

  const records = buildRecordsOrDecodeFailure(payload, options);
  sendSyncPayloadTelemetryRecords(options.ingestUrl, records);
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
