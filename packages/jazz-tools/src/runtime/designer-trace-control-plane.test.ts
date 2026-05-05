import { describe, expect, it } from "vitest";
import { InsertHandle, JazzClient, WriteHandle } from "./client.js";
import type { TableProxy } from "./db.js";
import {
  createDesignerTraceControlPlane,
  type DesignerTraceBatch,
  type DesignerTraceDb,
} from "./designer-trace-control-plane.js";

const fixedNow = new Date("2026-05-05T12:00:00.000Z");

class FakeDb implements DesignerTraceDb {
  readonly inserts: Array<{
    table: string;
    data: Record<string, unknown>;
    batchId: string;
  }> = [];
  readonly updates: Array<{
    table: string;
    id: string;
    data: Record<string, unknown>;
    batchId: string;
  }> = [];

  #nextRowId = 1;
  #nextBatchId = 1;
  readonly #client = {
    waitForPersistedBatch: async () => {},
  } as JazzClient;

  beginDirectBatch<T, Init>(_table: TableProxy<T, Init>): DesignerTraceBatch {
    const batchId = `direct-batch-${this.#nextBatchId++}`;
    return {
      batchId: () => batchId,
      insert: <Row, RowInit>(table: TableProxy<Row, RowInit>, data: RowInit): InsertHandle<Row> =>
        this.insertWithBatch(table, data, batchId),
      update: <Row, RowInit>(
        table: TableProxy<Row, RowInit>,
        id: string,
        data: Partial<RowInit>,
      ): WriteHandle => this.updateWithBatch(table, id, data, batchId),
    };
  }

  insert<T, Init>(table: TableProxy<T, Init>, data: Init): InsertHandle<T> {
    return this.insertWithBatch(table, data, `insert-batch-${this.#nextBatchId++}`);
  }

  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): WriteHandle {
    return this.updateWithBatch(table, id, data, `update-batch-${this.#nextBatchId++}`);
  }

  private insertWithBatch<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    batchId: string,
  ): InsertHandle<T> {
    const row = {
      id: `${table._table}-row-${this.#nextRowId++}`,
      ...(data as Record<string, unknown>),
    };
    this.inserts.push({ table: table._table, data: row, batchId });
    return new InsertHandle(row as T, batchId, this.#client);
  }

  private updateWithBatch<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    batchId: string,
  ): WriteHandle {
    this.updates.push({ table: table._table, id, data: data as Record<string, unknown>, batchId });
    return new WriteHandle(batchId, this.#client);
  }
}

function createControlPlane(db: FakeDb) {
  return createDesignerTraceControlPlane(db, {
    session: {
      sessionId: "session-1",
      sessionRowId: "00000000-0000-0000-0000-000000000001",
      workspaceId: "workspace-1",
      writerId: "writer-1",
      replicationScope: "account_sync",
      privacyMode: "private",
    },
    now: () => fixedNow,
    idFactory: (prefix) => `${prefix}-generated`,
    defaultUploadBackend: "object-storage.local",
  });
}

describe("designer trace control plane", () => {
  it("records telemetry with object refs and pending upload work in one direct batch", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);

    const write = controlPlane.recordTelemetryEvent({
      eventId: "event-indexer-finished",
      kind: "designer.indexer.scan.finished",
      payload: {
        fileCount: 42,
        durationMs: 17,
      },
      refs: {
        rootPath: "/repo",
      },
      objectRefs: [
        {
          role: "telemetry_payload",
          objectRefId: "object-telemetry-payload",
          provider: "s3",
          bucket: "designer-index",
          key: "telemetry/session-1/event-indexer-finished.json",
          contentHash: "sha256:telemetry",
          contentType: "application/json",
          sizeBytes: 512,
        },
      ],
    });

    expect(db.inserts.map((insert) => insert.table)).toEqual([
      "object_refs",
      "upload_jobs",
      "trace_events",
    ]);
    expect(new Set(db.inserts.map((insert) => insert.batchId))).toEqual(new Set(["direct-batch-1"]));
    expect(write.event.value).toMatchObject({
      event_id: "event-indexer-finished",
      session_id: "session-1",
      session_row_id: "00000000-0000-0000-0000-000000000001",
      kind: "designer.indexer.scan.finished",
      writer_id: "writer-1",
      replication_scope: "account_sync",
      privacy_mode: "private",
      payload_json: {
        fileCount: 42,
        durationMs: 17,
      },
      refs_json: {
        rootPath: "/repo",
        object_ref_ids: ["object-telemetry-payload"],
      },
    });
    expect(write.objectRefs[0]?.value).toMatchObject({
      object_ref_id: "object-telemetry-payload",
      role: "telemetry_payload",
      provider: "s3",
      bucket: "designer-index",
      key: "telemetry/session-1/event-indexer-finished.json",
      uri: "s3://designer-index/telemetry/session-1/event-indexer-finished.json",
      content_hash: "sha256:telemetry",
      content_type: "application/json",
      size_bytes: 512,
      access_policy_json: {
        replication_scope: "account_sync",
        privacy_mode: "private",
        explicit_access_proof_required: true,
      },
    });
    expect(write.uploadJobs[0]?.value).toMatchObject({
      target_kind: "trace_event",
      target_id: "event-indexer-finished",
      status: "pending",
      backend: "object-storage.local",
      object_ref_id: "object-telemetry-payload",
      object_ref_row_id: "object_refs-row-1",
    });
  });

  it("records a codebase index snapshot with object storage refs and upload jobs", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);

    const write = controlPlane.recordCodebaseIndexSnapshot({
      snapshotId: "index-snapshot-1",
      checkpointId: "checkpoint-1",
      phase: "manifest",
      rootPath: "/repo",
      projectHash: "sha256:project",
      fileCount: 128,
      changedPathCount: 7,
      metadata: {
        ignored: ["node_modules", ".git"],
      },
      objectRefs: {
        manifest: {
          objectRefId: "object-manifest",
          provider: "s3",
          bucket: "designer-index",
          key: "workspaces/workspace-1/embeddable_files.txt",
          contentHash: "sha256:manifest",
          contentType: "text/plain",
        },
        delta: {
          objectRefId: "object-delta",
          provider: "s3",
          bucket: "designer-index",
          key: "workspaces/workspace-1/delta.json",
          contentHash: "sha256:delta",
          contentType: "application/json",
        },
      },
    });

    expect(db.inserts.map((insert) => insert.table)).toEqual([
      "object_refs",
      "object_refs",
      "upload_jobs",
      "upload_jobs",
      "codebase_index_snapshots",
    ]);
    expect(write.snapshot.value).toMatchObject({
      snapshot_id: "index-snapshot-1",
      session_id: "session-1",
      session_row_id: "00000000-0000-0000-0000-000000000001",
      workspace_id: "workspace-1",
      checkpoint_id: "checkpoint-1",
      phase: "manifest",
      root_path: "/repo",
      project_hash: "sha256:project",
      file_count: 128,
      changed_path_count: 7,
      manifest_object_ref_id: "object-manifest",
      manifest_object_ref_row_id: "object_refs-row-1",
      delta_object_ref_id: "object-delta",
      delta_object_ref_row_id: "object_refs-row-2",
      latest_object_ref_id: null,
      access_policy_json: {
        replication_scope: "account_sync",
        privacy_mode: "private",
        explicit_access_proof_required: true,
      },
      metadata_json: {
        ignored: ["node_modules", ".git"],
      },
    });
    expect(write.uploadJobs.map((handle) => handle.value)).toEqual([
      expect.objectContaining({
        target_kind: "codebase_index_snapshot",
        target_id: "index-snapshot-1",
        object_ref_id: "object-manifest",
        object_ref_row_id: "object_refs-row-1",
      }),
      expect.objectContaining({
        target_kind: "codebase_index_snapshot",
        target_id: "index-snapshot-1",
        object_ref_id: "object-delta",
        object_ref_row_id: "object_refs-row-2",
      }),
    ]);
  });

  it("records upload receipts and marks the matching upload job uploaded", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);

    const write = controlPlane.recordUploadReceipt({
      receiptId: "receipt-1",
      uploadJobId: "upload-job-1",
      uploadJobRowId: "upload_jobs-row-3",
      objectRefId: "object-manifest",
      objectRefRowId: "object_refs-row-1",
      backend: "object-storage.local",
      storageBackend: "s3",
      bucket: "designer-index",
      region: "us-west-2",
      key: "workspaces/workspace-1/embeddable_files.txt",
      uri: "s3://designer-index/workspaces/workspace-1/embeddable_files.txt",
      metadata: {
        etag: "etag-1",
      },
    });

    expect(write.receipt.value).toMatchObject({
      receipt_id: "receipt-1",
      upload_job_id: "upload-job-1",
      upload_job_row_id: "upload_jobs-row-3",
      object_ref_id: "object-manifest",
      object_ref_row_id: "object_refs-row-1",
      backend: "object-storage.local",
      storage_backend: "s3",
      bucket: "designer-index",
      region: "us-west-2",
      key: "workspaces/workspace-1/embeddable_files.txt",
      uri: "s3://designer-index/workspaces/workspace-1/embeddable_files.txt",
      received_at: fixedNow,
      metadata_json: {
        etag: "etag-1",
      },
    });
    expect(db.updates).toEqual([
      {
        table: "upload_jobs",
        id: "upload_jobs-row-3",
        batchId: "direct-batch-1",
        data: {
          status: "uploaded",
          last_error: null,
          updated_at: fixedNow,
        },
      },
    ]);
  });
});
