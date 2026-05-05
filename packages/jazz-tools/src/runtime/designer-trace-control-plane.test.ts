import { describe, expect, it } from "vitest";
import { InsertHandle, JazzClient, WriteHandle } from "./client.js";
import type { TableProxy } from "./db.js";
import {
  DesignerTraceAccessPolicyError,
  DesignerTraceUploadError,
  createDesignerTraceControlPlane,
  designerTraceTables,
  hashDesignerTraceContent,
  type DesignerTraceBatch,
  type DesignerTraceControlPlaneOptions,
  type DesignerTraceDb,
  type DesignerTraceObjectStorageProvider,
} from "./designer-trace-control-plane.js";

const fixedNow = new Date("2026-05-05T12:00:00.000Z");
const leaseExpiresAt = new Date("2026-05-05T12:05:00.000Z");

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
  } as unknown as JazzClient;

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

  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): WriteHandle {
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

function createControlPlane(db: FakeDb, overrides: Partial<DesignerTraceControlPlaneOptions> = {}) {
  const base: DesignerTraceControlPlaneOptions = {
    session: {
      sessionId: "session-1",
      sessionRowId: "00000000-0000-0000-0000-000000000001",
      workspaceId: "workspace-1",
      writerId: "writer-1",
      replicationScope: "account_sync",
      privacyMode: "private",
    },
    accessProof: {
      proofId: "proof-1",
      kind: "workspace-writer",
      workspaceId: "workspace-1",
      writerId: "writer-1",
      issuedAt: new Date("2026-05-05T11:55:00.000Z"),
      expiresAt: new Date("2026-05-05T12:30:00.000Z"),
    },
    now: () => fixedNow,
    idFactory: (prefix) => `${prefix}-generated`,
    defaultUploadBackend: "object-storage.local",
  };
  return createDesignerTraceControlPlane(db, {
    ...base,
    ...overrides,
    session: {
      ...base.session,
      ...(overrides.session ?? {}),
    },
  });
}

function contentHash(content: string): string {
  return hashDesignerTraceContent(new TextEncoder().encode(content));
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
    expect(new Set(db.inserts.map((insert) => insert.batchId))).toEqual(
      new Set(["direct-batch-1"]),
    );
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
    expect(write.event.value.canonical_hash).toMatch(/^sha256:[a-f0-9]{64}$/);
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
        allowed_workspace_ids: ["workspace-1"],
        allowed_writer_ids: ["writer-1"],
      },
    });
    expect(write.uploadJobs[0]?.value).toMatchObject({
      target_kind: "trace_event",
      target_id: "event-indexer-finished",
      status: "pending",
      backend: "object-storage.local",
      object_ref_id: "object-telemetry-payload",
      object_ref_row_id: "object_refs-row-1",
      attempt_count: 0,
      claimed_by: null,
      lease_expires_at: null,
      completed_at: null,
      failed_at: null,
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
        allowed_workspace_ids: ["workspace-1"],
        allowed_writer_ids: ["writer-1"],
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

  it("records turn, command, VCS, and autonomy provenance for empty-commit investigations", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);

    const turnWrite = controlPlane.recordAgentTurn({
      turnId: "codex-turn-49",
      provider: "codex",
      providerSessionId: "019df058-869b-7f71-aae8-f36fec2ffd7a",
      providerTurnOrdinal: 49,
      cwd: "/Users/nikitavoloboev/work/codex-launch-zed-60464684-20260504001650",
      repoRoot: "/Users/nikitavoloboev/code/prom",
      branchName: "live",
      model: "gpt-5.5",
      status: "completed",
      transcriptObjectRef: {
        objectRefId: "object-transcript-turn-49",
        provider: "s3",
        bucket: "designer-trace",
        key: "codex/019df058/turn-49.jsonl",
        contentHash: "sha256:transcript",
        contentType: "application/x-jsonlines",
      },
      metadata: {
        startUserOrdinal: 49,
      },
    });

    const commandWrite = controlPlane.recordToolInvocation({
      invocationId: "tool-git-empty-commit",
      agentTurnId: turnWrite.turn.value.turn_id,
      agentTurnRowId: turnWrite.turn.value.id,
      toolName: "exec_command",
      commandSummary: "git commit --allow-empty",
      cwd: "/Users/nikitavoloboev/code/prom",
      status: "success",
      exitCode: 0,
      stdoutObjectRef: {
        objectRefId: "object-git-stdout",
        provider: "s3",
        bucket: "designer-trace",
        key: "codex/019df058/tool-git-empty-commit/stdout.txt",
        contentHash: "sha256:stdout",
        contentType: "text/plain",
      },
      metadata: {
        argv: ["git", "commit", "--allow-empty"],
      },
    });

    const vcsWrite = controlPlane.recordVcsOperation({
      vcsOperationId: "vcs-empty-commit-71fc8bc9",
      agentTurnId: turnWrite.turn.value.turn_id,
      agentTurnRowId: turnWrite.turn.value.id,
      toolInvocationId: commandWrite.invocation.value.invocation_id,
      toolInvocationRowId: commandWrite.invocation.value.id,
      repoRoot: "/Users/nikitavoloboev/code/prom",
      vcsKind: "git",
      operationKind: "commit",
      refName: "refs/heads/nikiv-live",
      beforeOid: "45f510b058e2",
      afterOid: "71fc8bc9f177",
      commitOid: "71fc8bc9f177",
      treeOid: "tree-same-as-parent",
      parentOids: ["45f510b058e2"],
      isEmptyCommit: true,
      traceRefs: [],
      status: "created",
      metadata: {
        committedAt: "2026-05-05T07:36:52.000Z",
        stamped: false,
      },
    });

    const autonomyWrite = controlPlane.recordAutonomyDecision({
      decisionId: "autonomy-empty-commit-allowed",
      agentTurnId: turnWrite.turn.value.turn_id,
      agentTurnRowId: turnWrite.turn.value.id,
      toolInvocationId: commandWrite.invocation.value.invocation_id,
      toolInvocationRowId: commandWrite.invocation.value.id,
      daemonRunId: "prom-tip-sync-daemon-20260505",
      decisionKind: "commit-turn-gate",
      decision: "allowed",
      resourceKind: "git-ref",
      resourceId: "refs/heads/nikiv-live",
      ownerSessionId: "019df058-869b-7f71-aae8-f36fec2ffd7a",
      status: "missed-invariant",
      reason: "empty commit without trace ref was not rejected",
      resource: {
        refName: "refs/heads/nikiv-live",
        beforeOid: "45f510b058e2",
        afterOid: "71fc8bc9f177",
      },
      invariant: {
        requireTraceRef: true,
        rejectEmptyCommit: true,
      },
      outcome: {
        repaired: false,
      },
    });

    expect(db.inserts.map((insert) => insert.table)).toEqual([
      "object_refs",
      "upload_jobs",
      "agent_turns",
      "object_refs",
      "upload_jobs",
      "tool_invocations",
      "vcs_operations",
      "autonomy_decisions",
    ]);
    expect(Object.keys(designerTraceTables.agentTurns._schema)).toEqual(
      expect.arrayContaining([
        "agent_turns",
        "tool_invocations",
        "vcs_operations",
        "autonomy_decisions",
      ]),
    );
    expect(turnWrite.turn.value).toMatchObject({
      turn_id: "codex-turn-49",
      provider: "codex",
      provider_session_id: "019df058-869b-7f71-aae8-f36fec2ffd7a",
      provider_turn_ordinal: 49,
      workspace_id: "workspace-1",
      transcript_object_ref_id: "object-transcript-turn-49",
      transcript_object_ref_row_id: "object_refs-row-1",
      metadata_json: {
        startUserOrdinal: 49,
      },
    });
    expect(commandWrite.invocation.value).toMatchObject({
      invocation_id: "tool-git-empty-commit",
      agent_turn_id: "codex-turn-49",
      agent_turn_row_id: "agent_turns-row-3",
      command_summary: "git commit --allow-empty",
      command_hash: expect.stringMatching(/^sha256:[a-f0-9]{64}$/),
      stdout_object_ref_id: "object-git-stdout",
      stdout_object_ref_row_id: "object_refs-row-4",
      exit_code: 0,
      status: "success",
    });
    expect(vcsWrite.operation.value).toMatchObject({
      vcs_operation_id: "vcs-empty-commit-71fc8bc9",
      agent_turn_id: "codex-turn-49",
      tool_invocation_id: "tool-git-empty-commit",
      repo_root: "/Users/nikitavoloboev/code/prom",
      vcs_kind: "git",
      operation_kind: "commit",
      ref_name: "refs/heads/nikiv-live",
      before_oid: "45f510b058e2",
      after_oid: "71fc8bc9f177",
      commit_oid: "71fc8bc9f177",
      parent_oids_json: ["45f510b058e2"],
      is_empty_commit: true,
      trace_refs_json: [],
      metadata_json: {
        committedAt: "2026-05-05T07:36:52.000Z",
        stamped: false,
      },
    });
    expect(autonomyWrite.decision.value).toMatchObject({
      decision_id: "autonomy-empty-commit-allowed",
      agent_turn_id: "codex-turn-49",
      tool_invocation_id: "tool-git-empty-commit",
      daemon_run_id: "prom-tip-sync-daemon-20260505",
      decision_kind: "commit-turn-gate",
      decision: "allowed",
      resource_kind: "git-ref",
      resource_id: "refs/heads/nikiv-live",
      owner_session_id: "019df058-869b-7f71-aae8-f36fec2ffd7a",
      status: "missed-invariant",
      reason: "empty commit without trace ref was not rejected",
      invariant_json: {
        requireTraceRef: true,
        rejectEmptyCommit: true,
      },
      outcome_json: {
        repaired: false,
      },
    });
  });

  it("requires a live access proof when policies require explicit proof", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db, { accessProof: undefined });

    expect(() =>
      controlPlane.recordCodebaseIndexSnapshot({
        snapshotId: "index-snapshot-1",
        phase: "manifest",
        rootPath: "/repo",
        fileCount: 1,
      }),
    ).toThrow(DesignerTraceAccessPolicyError);
  });

  it("rejects access proofs for the wrong writer", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db, {
      accessProof: {
        proofId: "proof-1",
        kind: "workspace-writer",
        workspaceId: "workspace-1",
        writerId: "writer-2",
        issuedAt: new Date("2026-05-05T11:55:00.000Z"),
        expiresAt: new Date("2026-05-05T12:30:00.000Z"),
      },
    });

    expect(() =>
      controlPlane.recordTelemetryEvent({
        kind: "designer.indexer.scan.finished",
        objectRefs: [
          {
            provider: "s3",
            bucket: "designer-index",
            key: "telemetry/event.json",
          },
        ],
      }),
    ).toThrow(DesignerTraceAccessPolicyError);
  });

  it("claims and heartbeats upload jobs with bounded leases", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);

    controlPlane.claimUploadJob({
      uploadJobRowId: "upload_jobs-row-1",
      workerId: "worker-1",
      leaseExpiresAt,
    });
    controlPlane.heartbeatUploadJob({
      uploadJobRowId: "upload_jobs-row-1",
      workerId: "worker-1",
      leaseExpiresAt,
    });

    expect(db.updates).toEqual([
      {
        table: "upload_jobs",
        id: "upload_jobs-row-1",
        batchId: "direct-batch-1",
        data: {
          status: "queued",
          claimed_by: "worker-1",
          lease_expires_at: leaseExpiresAt,
          last_heartbeat_at: fixedNow,
          updated_at: fixedNow,
        },
      },
      {
        table: "upload_jobs",
        id: "upload_jobs-row-1",
        batchId: "direct-batch-2",
        data: {
          claimed_by: "worker-1",
          lease_expires_at: leaseExpiresAt,
          last_heartbeat_at: fixedNow,
          updated_at: fixedNow,
        },
      },
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
          next_retry_at: null,
          claimed_by: null,
          lease_expires_at: null,
          last_heartbeat_at: null,
          completed_at: fixedNow,
          failed_at: null,
          updated_at: fixedNow,
        },
      },
    ]);
  });

  it("processes an upload job through an object storage provider and records the receipt", async () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);
    const content = '{"files":["src/index.ts"]}';
    const write = controlPlane.recordTelemetryEvent({
      eventId: "event-with-payload",
      kind: "designer.telemetry.payload.ready",
      objectRefs: [
        {
          objectRefId: "object-payload",
          provider: "s3",
          bucket: "designer-index",
          key: "telemetry/session-1/event-with-payload.json",
          contentHash: contentHash(content),
          contentType: "application/json",
          sizeBytes: new TextEncoder().encode(content).byteLength,
          metadata: {
            trace: "telemetry",
          },
        },
      ],
    });
    let storedContentHash: string | undefined;
    const provider: DesignerTraceObjectStorageProvider = {
      async putObject(input) {
        storedContentHash = input.contentHash;
        expect(input).toMatchObject({
          provider: "s3",
          bucket: "designer-index",
          key: "telemetry/session-1/event-with-payload.json",
          uri: "s3://designer-index/telemetry/session-1/event-with-payload.json",
          contentType: "application/json",
          sizeBytes: new TextEncoder().encode(content).byteLength,
          accessPolicy: {
            replication_scope: "account_sync",
            privacy_mode: "private",
          },
          metadata: {
            trace: "telemetry",
            source: "indexer",
          },
        });
        return {
          storageBackend: "s3",
          bucket: input.bucket,
          key: input.key,
          uri: input.uri,
          contentHash: input.contentHash,
          sizeBytes: input.sizeBytes,
          etag: "etag-payload",
          metadata: {
            storage_class: "standard",
          },
          receivedAt: fixedNow,
        };
      },
    };

    const result = await controlPlane.processUploadJob({
      uploadJob: write.uploadJobs[0]!.value,
      objectRef: write.objectRefs[0]!.value,
      content,
      provider,
      metadata: {
        source: "indexer",
      },
    });

    expect(storedContentHash).toBe(contentHash(content));
    expect(result.receiptWrite.receipt.value).toMatchObject({
      upload_job_id: "upload-job-generated",
      object_ref_id: "object-payload",
      backend: "object-storage.local",
      storage_backend: "s3",
      bucket: "designer-index",
      key: "telemetry/session-1/event-with-payload.json",
      uri: "s3://designer-index/telemetry/session-1/event-with-payload.json",
      metadata_json: {
        storage_class: "standard",
        content_hash: contentHash(content),
        size_bytes: new TextEncoder().encode(content).byteLength,
        etag: "etag-payload",
      },
    });
    expect(db.updates.at(-1)).toMatchObject({
      table: "upload_jobs",
      id: "upload_jobs-row-2",
      data: {
        status: "uploaded",
        completed_at: fixedNow,
        failed_at: null,
      },
    });
  });

  it("rejects upload content that does not match the object ref hash", async () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);
    const write = controlPlane.recordTelemetryEvent({
      kind: "designer.telemetry.payload.ready",
      objectRefs: [
        {
          objectRefId: "object-payload",
          provider: "s3",
          bucket: "designer-index",
          key: "telemetry/session-1/event-with-payload.json",
          contentHash: contentHash("expected"),
          sizeBytes: new TextEncoder().encode("expected").byteLength,
        },
      ],
    });
    const provider: DesignerTraceObjectStorageProvider = {
      async putObject() {
        throw new Error("should not store mismatched content");
      },
    };

    await expect(
      controlPlane.processUploadJob({
        uploadJob: write.uploadJobs[0]!.value,
        objectRef: write.objectRefs[0]!.value,
        content: "actual",
        provider,
      }),
    ).rejects.toThrow(DesignerTraceUploadError);
  });

  it("records upload failures with attempts and retry policy", () => {
    const db = new FakeDb();
    const controlPlane = createControlPlane(db);
    const nextRetryAt = new Date("2026-05-05T12:01:00.000Z");

    controlPlane.recordUploadFailure({
      uploadJob: {
        id: "upload_jobs-row-1",
        upload_job_id: "upload-job-1",
        session_id: "session-1",
        session_row_id: "00000000-0000-0000-0000-000000000001",
        workspace_id: "workspace-1",
        target_kind: "trace_event",
        target_id: "event-1",
        status: "queued",
        backend: "object-storage.local",
        object_ref_id: "object-1",
        object_ref_row_id: "object_refs-row-1",
        attempt_count: 2,
        last_error: null,
        next_retry_at: null,
        claimed_by: "worker-1",
        lease_expires_at: leaseExpiresAt,
        last_heartbeat_at: fixedNow,
        completed_at: null,
        failed_at: null,
        access_policy_json: {},
        request_json: {},
        created_at: fixedNow,
        updated_at: fixedNow,
      },
      workerId: "worker-1",
      error: new Error("s3 503"),
      nextRetryAt,
    });

    expect(db.updates).toEqual([
      {
        table: "upload_jobs",
        id: "upload_jobs-row-1",
        batchId: "direct-batch-1",
        data: {
          status: "failed",
          attempt_count: 3,
          last_error: "s3 503",
          next_retry_at: nextRetryAt,
          claimed_by: null,
          lease_expires_at: null,
          last_heartbeat_at: null,
          completed_at: null,
          failed_at: fixedNow,
          updated_at: fixedNow,
        },
      },
    ]);
  });
});
