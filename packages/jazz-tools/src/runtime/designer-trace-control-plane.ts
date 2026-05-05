import type { InsertHandle, WriteHandle } from "./client.js";
import type { TableProxy } from "./db.js";
import { app as designerTraceApp } from "./designer-trace-phase0.schema.js";

export const DESIGNER_TRACE_SCHEMA_VERSION = "trace.designer.v1";

export type DesignerTraceJson = Record<string, unknown>;

export interface DesignerTraceSessionContext {
  sessionId: string;
  sessionRowId: string;
  workspaceId: string;
  writerId: string;
  replicationScope: string;
  privacyMode: string;
  schemaVersion?: string;
}

export interface DesignerTraceBatch {
  batchId(): string;
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): InsertHandle<T>;
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): WriteHandle;
}

export interface DesignerTraceDb {
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): InsertHandle<T>;
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): WriteHandle;
  beginDirectBatch<T, Init>(table: TableProxy<T, Init>): DesignerTraceBatch;
}

export type DesignerObjectRefRole =
  | "telemetry_payload"
  | "index_manifest"
  | "index_delta"
  | "index_latest"
  | "workspace_snapshot"
  | "trace_projection"
  | (string & {});

export interface DesignerObjectRefRow {
  id: string;
  object_ref_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  role: string;
  schema_version: string;
  provider: string;
  bucket?: string | null;
  key: string;
  uri: string;
  content_hash?: string | null;
  content_encoding?: string | null;
  content_type?: string | null;
  size_bytes?: number | null;
  replication_scope: string;
  privacy_mode: string;
  access_policy_json: DesignerTraceJson;
  metadata_json: DesignerTraceJson;
  created_at: Date;
}

export type DesignerObjectRefInit = Omit<DesignerObjectRefRow, "id">;

export interface DesignerUploadJobRow {
  id: string;
  upload_job_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  target_kind: string;
  target_id: string;
  status: "pending" | "queued" | "uploaded" | "failed" | "skipped" | (string & {});
  backend: string;
  object_ref_id?: string | null;
  object_ref_row_id?: string | null;
  attempt_count: number;
  last_error?: string | null;
  next_retry_at?: Date | null;
  access_policy_json: DesignerTraceJson;
  request_json: DesignerTraceJson;
  created_at: Date;
  updated_at: Date;
}

export type DesignerUploadJobInit = Omit<DesignerUploadJobRow, "id">;

export interface DesignerUploadReceiptRow {
  id: string;
  receipt_id: string;
  upload_job_id: string;
  upload_job_row_id?: string | null;
  session_id: string;
  session_row_id: string;
  object_ref_id: string;
  object_ref_row_id?: string | null;
  backend: string;
  storage_backend?: string | null;
  bucket?: string | null;
  region?: string | null;
  key: string;
  uri: string;
  received_at: Date;
  metadata_json: DesignerTraceJson;
}

export type DesignerUploadReceiptInit = Omit<DesignerUploadReceiptRow, "id">;

export interface DesignerCodebaseIndexSnapshotRow {
  id: string;
  snapshot_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  checkpoint_id?: string | null;
  phase: string;
  root_path: string;
  project_hash?: string | null;
  file_count: number;
  changed_path_count: number;
  manifest_object_ref_id?: string | null;
  manifest_object_ref_row_id?: string | null;
  delta_object_ref_id?: string | null;
  delta_object_ref_row_id?: string | null;
  latest_object_ref_id?: string | null;
  latest_object_ref_row_id?: string | null;
  access_policy_json: DesignerTraceJson;
  metadata_json: DesignerTraceJson;
  captured_at: Date;
}

export type DesignerCodebaseIndexSnapshotInit = Omit<DesignerCodebaseIndexSnapshotRow, "id">;

export interface DesignerTraceEventRow {
  id: string;
  event_id: string;
  session_id: string;
  session_row_id: string;
  schema_version: string;
  kind: string;
  occurred_at: Date;
  writer_id: string;
  replication_scope: string;
  privacy_mode: string;
  canonical_hash: string;
  code_state_id?: string | null;
  buffer_state_id?: string | null;
  checkpoint_id?: string | null;
  chunk_hash?: string | null;
  projection_id?: string | null;
  git_snapshot_id?: string | null;
  payload_json: DesignerTraceJson;
  refs_json: DesignerTraceJson;
}

export type DesignerTraceEventInit = Omit<DesignerTraceEventRow, "id">;

export interface DesignerObjectRefInput {
  objectRefId?: string;
  role?: DesignerObjectRefRole;
  provider: string;
  bucket?: string | null;
  key: string;
  uri?: string;
  contentHash?: string | null;
  contentEncoding?: string | null;
  contentType?: string | null;
  sizeBytes?: number | null;
  accessPolicy?: DesignerTraceJson;
  metadata?: DesignerTraceJson;
}

export interface RecordTelemetryEventInput {
  eventId?: string;
  kind: string;
  occurredAt?: Date;
  writerId?: string;
  replicationScope?: string;
  privacyMode?: string;
  canonicalHash?: string;
  codeStateId?: string | null;
  bufferStateId?: string | null;
  checkpointId?: string | null;
  chunkHash?: string | null;
  projectionId?: string | null;
  gitSnapshotId?: string | null;
  payload?: DesignerTraceJson;
  refs?: DesignerTraceJson;
  objectRefs?: DesignerObjectRefInput[];
  uploadBackend?: string;
}

export interface CodebaseIndexObjectRefsInput {
  manifest?: DesignerObjectRefInput;
  delta?: DesignerObjectRefInput;
  latest?: DesignerObjectRefInput;
}

export interface RecordCodebaseIndexSnapshotInput {
  snapshotId?: string;
  workspaceId?: string;
  checkpointId?: string | null;
  phase: string;
  rootPath: string;
  projectHash?: string | null;
  fileCount: number;
  changedPathCount?: number;
  capturedAt?: Date;
  metadata?: DesignerTraceJson;
  accessPolicy?: DesignerTraceJson;
  objectRefs?: CodebaseIndexObjectRefsInput;
  uploadBackend?: string;
}

export interface RecordUploadReceiptInput {
  receiptId?: string;
  uploadJobId: string;
  uploadJobRowId?: string | null;
  objectRefId: string;
  objectRefRowId?: string | null;
  backend: string;
  storageBackend?: string | null;
  bucket?: string | null;
  region?: string | null;
  key: string;
  uri: string;
  receivedAt?: Date;
  metadata?: DesignerTraceJson;
}

export interface DesignerTraceControlPlaneOptions {
  session: DesignerTraceSessionContext;
  now?: () => Date;
  idFactory?: (prefix: string) => string;
  defaultUploadBackend?: string;
}

export interface TelemetryEventWrite {
  batchId: string;
  event: InsertHandle<DesignerTraceEventRow>;
  objectRefs: InsertHandle<DesignerObjectRefRow>[];
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface CodebaseIndexSnapshotWrite {
  batchId: string;
  snapshot: InsertHandle<DesignerCodebaseIndexSnapshotRow>;
  objectRefs: {
    manifest?: InsertHandle<DesignerObjectRefRow>;
    delta?: InsertHandle<DesignerObjectRefRow>;
    latest?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface UploadReceiptWrite {
  batchId: string;
  receipt: InsertHandle<DesignerUploadReceiptRow>;
  uploadJobUpdate?: WriteHandle;
}

function makeTable<Row, Init>(table: string): TableProxy<Row, Init> {
  return {
    _table: table,
    _schema: designerTraceApp.wasmSchema,
    _rowType: undefined as unknown as Row,
    _initType: undefined as unknown as Init,
  };
}

export const designerTraceTables = {
  traceEvents: makeTable<DesignerTraceEventRow, DesignerTraceEventInit>("trace_events"),
  objectRefs: makeTable<DesignerObjectRefRow, DesignerObjectRefInit>("object_refs"),
  uploadJobs: makeTable<DesignerUploadJobRow, DesignerUploadJobInit>("upload_jobs"),
  uploadReceipts: makeTable<DesignerUploadReceiptRow, DesignerUploadReceiptInit>("upload_receipts"),
  codebaseIndexSnapshots: makeTable<
    DesignerCodebaseIndexSnapshotRow,
    DesignerCodebaseIndexSnapshotInit
  >("codebase_index_snapshots"),
} as const;

export function createDesignerTraceControlPlane(
  db: DesignerTraceDb,
  options: DesignerTraceControlPlaneOptions,
) {
  const now = options.now ?? (() => new Date());
  const idFactory = options.idFactory ?? createId;
  const defaultUploadBackend = options.defaultUploadBackend ?? "object-storage";
  const session = options.session;

  const schemaVersion = () => session.schemaVersion ?? DESIGNER_TRACE_SCHEMA_VERSION;
  const accessPolicy = (override?: DesignerTraceJson): DesignerTraceJson => ({
    replication_scope: session.replicationScope,
    privacy_mode: session.privacyMode,
    explicit_access_proof_required: true,
    ...(override ?? {}),
  });

  const insertObjectRef = (
    batch: DesignerTraceBatch,
    input: DesignerObjectRefInput,
    role: DesignerObjectRefRole,
    workspaceId = session.workspaceId,
  ): InsertHandle<DesignerObjectRefRow> => {
    const row: DesignerObjectRefInit = {
      object_ref_id: input.objectRefId ?? idFactory("object-ref"),
      session_id: session.sessionId,
      session_row_id: session.sessionRowId,
      workspace_id: workspaceId,
      role: input.role ?? role,
      schema_version: schemaVersion(),
      provider: input.provider,
      bucket: input.bucket ?? null,
      key: input.key,
      uri: input.uri ?? composeObjectUri(input),
      content_hash: input.contentHash ?? null,
      content_encoding: input.contentEncoding ?? null,
      content_type: input.contentType ?? null,
      size_bytes: input.sizeBytes ?? null,
      replication_scope: session.replicationScope,
      privacy_mode: session.privacyMode,
      access_policy_json: accessPolicy(input.accessPolicy),
      metadata_json: input.metadata ?? {},
      created_at: now(),
    };
    return batch.insert(designerTraceTables.objectRefs, row);
  };

  const insertUploadJob = (
    batch: DesignerTraceBatch,
    objectRef: InsertHandle<DesignerObjectRefRow>,
    targetKind: string,
    targetId: string,
    uploadBackend: string,
    workspaceId = session.workspaceId,
  ): InsertHandle<DesignerUploadJobRow> => {
    const objectRow = objectRef.value;
    const row: DesignerUploadJobInit = {
      upload_job_id: idFactory("upload-job"),
      session_id: session.sessionId,
      session_row_id: session.sessionRowId,
      workspace_id: workspaceId,
      target_kind: targetKind,
      target_id: targetId,
      status: "pending",
      backend: uploadBackend,
      object_ref_id: objectRow.object_ref_id,
      object_ref_row_id: objectRow.id,
      attempt_count: 0,
      last_error: null,
      next_retry_at: null,
      access_policy_json: objectRow.access_policy_json,
      request_json: {
        target_kind: targetKind,
        target_id: targetId,
        object: {
          object_ref_id: objectRow.object_ref_id,
          provider: objectRow.provider,
          bucket: objectRow.bucket,
          key: objectRow.key,
          uri: objectRow.uri,
          content_hash: objectRow.content_hash,
          content_type: objectRow.content_type,
          size_bytes: objectRow.size_bytes,
        },
      },
      created_at: now(),
      updated_at: now(),
    };
    return batch.insert(designerTraceTables.uploadJobs, row);
  };

  return {
    tables: designerTraceTables,

    recordTelemetryEvent(input: RecordTelemetryEventInput): TelemetryEventWrite {
      const eventId = input.eventId ?? idFactory("trace-event");
      const batch = db.beginDirectBatch(designerTraceTables.traceEvents);
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const objectRefs = (input.objectRefs ?? []).map((objectRefInput) =>
        insertObjectRef(batch, objectRefInput, "telemetry_payload"),
      );
      const uploadJobs = objectRefs.map((objectRef) =>
        insertUploadJob(batch, objectRef, "trace_event", eventId, uploadBackend),
      );
      const objectRefIds = objectRefs.map((objectRef) => objectRef.value.object_ref_id);
      const refsJson: DesignerTraceJson = {
        ...(input.refs ?? {}),
        ...(objectRefIds.length > 0 ? { object_ref_ids: objectRefIds } : {}),
      };
      const payloadJson = input.payload ?? {};
      const row: DesignerTraceEventInit = {
        event_id: eventId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        schema_version: schemaVersion(),
        kind: input.kind,
        occurred_at: input.occurredAt ?? now(),
        writer_id: input.writerId ?? session.writerId,
        replication_scope: input.replicationScope ?? session.replicationScope,
        privacy_mode: input.privacyMode ?? session.privacyMode,
        canonical_hash:
          input.canonicalHash ??
          hashCanonical({
            event_id: eventId,
            kind: input.kind,
            payload_json: payloadJson,
            refs_json: refsJson,
          }),
        code_state_id: input.codeStateId ?? null,
        buffer_state_id: input.bufferStateId ?? null,
        checkpoint_id: input.checkpointId ?? null,
        chunk_hash: input.chunkHash ?? null,
        projection_id: input.projectionId ?? null,
        git_snapshot_id: input.gitSnapshotId ?? null,
        payload_json: payloadJson,
        refs_json: refsJson,
      };
      return {
        batchId: batch.batchId(),
        event: batch.insert(designerTraceTables.traceEvents, row),
        objectRefs,
        uploadJobs,
      };
    },

    recordCodebaseIndexSnapshot(
      input: RecordCodebaseIndexSnapshotInput,
    ): CodebaseIndexSnapshotWrite {
      const snapshotId = input.snapshotId ?? idFactory("codebase-index-snapshot");
      const workspaceId = input.workspaceId ?? session.workspaceId;
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const batch = db.beginDirectBatch(designerTraceTables.codebaseIndexSnapshots);
      const objectRefs = {
        manifest: input.objectRefs?.manifest
          ? insertObjectRef(batch, input.objectRefs.manifest, "index_manifest", workspaceId)
          : undefined,
        delta: input.objectRefs?.delta
          ? insertObjectRef(batch, input.objectRefs.delta, "index_delta", workspaceId)
          : undefined,
        latest: input.objectRefs?.latest
          ? insertObjectRef(batch, input.objectRefs.latest, "index_latest", workspaceId)
          : undefined,
      };
      const uploadJobs = [objectRefs.manifest, objectRefs.delta, objectRefs.latest]
        .filter((handle): handle is InsertHandle<DesignerObjectRefRow> => handle !== undefined)
        .map((objectRef) =>
          insertUploadJob(
            batch,
            objectRef,
            "codebase_index_snapshot",
            snapshotId,
            uploadBackend,
            workspaceId,
          ),
        );
      const row: DesignerCodebaseIndexSnapshotInit = {
        snapshot_id: snapshotId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        workspace_id: workspaceId,
        checkpoint_id: input.checkpointId ?? null,
        phase: input.phase,
        root_path: input.rootPath,
        project_hash: input.projectHash ?? null,
        file_count: input.fileCount,
        changed_path_count: input.changedPathCount ?? 0,
        manifest_object_ref_id: objectRefs.manifest?.value.object_ref_id ?? null,
        manifest_object_ref_row_id: objectRefs.manifest?.value.id ?? null,
        delta_object_ref_id: objectRefs.delta?.value.object_ref_id ?? null,
        delta_object_ref_row_id: objectRefs.delta?.value.id ?? null,
        latest_object_ref_id: objectRefs.latest?.value.object_ref_id ?? null,
        latest_object_ref_row_id: objectRefs.latest?.value.id ?? null,
        access_policy_json: accessPolicy(input.accessPolicy),
        metadata_json: input.metadata ?? {},
        captured_at: input.capturedAt ?? now(),
      };
      return {
        batchId: batch.batchId(),
        snapshot: batch.insert(designerTraceTables.codebaseIndexSnapshots, row),
        objectRefs,
        uploadJobs,
      };
    },

    recordUploadReceipt(input: RecordUploadReceiptInput): UploadReceiptWrite {
      const batch = db.beginDirectBatch(designerTraceTables.uploadReceipts);
      const row: DesignerUploadReceiptInit = {
        receipt_id: input.receiptId ?? idFactory("upload-receipt"),
        upload_job_id: input.uploadJobId,
        upload_job_row_id: input.uploadJobRowId ?? null,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        object_ref_id: input.objectRefId,
        object_ref_row_id: input.objectRefRowId ?? null,
        backend: input.backend,
        storage_backend: input.storageBackend ?? null,
        bucket: input.bucket ?? null,
        region: input.region ?? null,
        key: input.key,
        uri: input.uri,
        received_at: input.receivedAt ?? now(),
        metadata_json: input.metadata ?? {},
      };
      const receipt = batch.insert(designerTraceTables.uploadReceipts, row);
      const uploadJobUpdate = input.uploadJobRowId
        ? batch.update(designerTraceTables.uploadJobs, input.uploadJobRowId, {
            status: "uploaded",
            last_error: null,
            updated_at: row.received_at,
          })
        : undefined;
      return {
        batchId: batch.batchId(),
        receipt,
        uploadJobUpdate,
      };
    },
  };
}

function createId(prefix: string): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return `${prefix}-${cryptoObj.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function composeObjectUri(input: DesignerObjectRefInput): string {
  if (input.provider === "s3" && input.bucket) {
    return `s3://${input.bucket}/${input.key}`;
  }
  if (input.provider === "oci" && input.bucket) {
    return `oci://${input.bucket}/${input.key}`;
  }
  if (input.provider === "local") {
    return input.key.startsWith("/") ? `file://${input.key}` : `local://${input.key}`;
  }
  if (input.bucket) {
    return `${input.provider}://${input.bucket}/${input.key}`;
  }
  return `${input.provider}://${input.key}`;
}

function hashCanonical(value: unknown): string {
  let hash = 0x811c9dc5;
  const text = stableJson(value);
  for (let index = 0; index < text.length; index += 1) {
    hash ^= text.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return `fnv1a32:${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

function stableJson(value: unknown): string {
  if (value === undefined) {
    return "undefined";
  }
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "undefined";
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableJson(entry)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  return `{${keys
    .filter((key) => record[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
    .join(",")}}`;
}
