import type { InsertHandle, WriteHandle } from "./client.js";
import type { TableProxy } from "./db.js";
import { app as designerTraceApp } from "./designer-trace-phase0.schema.js";

export const DESIGNER_TRACE_SCHEMA_VERSION = "trace.designer.v1";

export type DesignerTraceJson = Record<string, unknown>;

export type DesignerUploadJobStatus =
  | "pending"
  | "queued"
  | "uploaded"
  | "failed"
  | "skipped"
  | (string & {});

export interface DesignerAccessPolicy {
  replication_scope: string;
  privacy_mode: string;
  explicit_access_proof_required: boolean;
  allowed_workspace_ids?: string[];
  allowed_writer_ids?: string[];
  object_storage_scope?: string;
  proof_id?: string;
  proof_kind?: string;
  [key: string]: unknown;
}

export interface DesignerAccessProof {
  proofId: string;
  kind: string;
  workspaceId?: string;
  writerId?: string;
  issuedAt?: Date;
  expiresAt?: Date;
  claims?: DesignerTraceJson;
}

export class DesignerTraceControlPlaneError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DesignerTraceControlPlaneError";
  }
}

export class DesignerTraceAccessPolicyError extends DesignerTraceControlPlaneError {
  constructor(message: string) {
    super(message);
    this.name = "DesignerTraceAccessPolicyError";
  }
}

export class DesignerTraceUploadError extends DesignerTraceControlPlaneError {
  constructor(message: string) {
    super(message);
    this.name = "DesignerTraceUploadError";
  }
}

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
  | "turn_transcript"
  | "tool_stdout"
  | "tool_stderr"
  | "vcs_diff"
  | "workspace_merge_diff"
  | "workspace_conflict_marker"
  | "workspace_conflict_diff"
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
  status: DesignerUploadJobStatus;
  backend: string;
  object_ref_id?: string | null;
  object_ref_row_id?: string | null;
  attempt_count: number;
  last_error?: string | null;
  next_retry_at?: Date | null;
  claimed_by?: string | null;
  lease_expires_at?: Date | null;
  last_heartbeat_at?: Date | null;
  completed_at?: Date | null;
  failed_at?: Date | null;
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

export interface DesignerAgentTurnRow {
  id: string;
  turn_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  provider: string;
  provider_session_id: string;
  provider_turn_ordinal?: number | null;
  provider_turn_id?: string | null;
  cwd: string;
  repo_root?: string | null;
  branch_name?: string | null;
  model?: string | null;
  transcript_object_ref_id?: string | null;
  transcript_object_ref_row_id?: string | null;
  status: string;
  started_at: Date;
  completed_at?: Date | null;
  metadata_json: DesignerTraceJson;
}

export type DesignerAgentTurnInit = Omit<DesignerAgentTurnRow, "id">;

export interface DesignerToolInvocationRow {
  id: string;
  invocation_id: string;
  session_id: string;
  session_row_id: string;
  agent_turn_id?: string | null;
  agent_turn_row_id?: string | null;
  tool_name: string;
  command_hash: string;
  command_summary: string;
  cwd: string;
  started_at: Date;
  completed_at?: Date | null;
  exit_code?: number | null;
  status: string;
  stdout_object_ref_id?: string | null;
  stdout_object_ref_row_id?: string | null;
  stderr_object_ref_id?: string | null;
  stderr_object_ref_row_id?: string | null;
  metadata_json: DesignerTraceJson;
}

export type DesignerToolInvocationInit = Omit<DesignerToolInvocationRow, "id">;

export interface DesignerVcsOperationRow {
  id: string;
  vcs_operation_id: string;
  session_id: string;
  session_row_id: string;
  agent_turn_id?: string | null;
  agent_turn_row_id?: string | null;
  tool_invocation_id?: string | null;
  tool_invocation_row_id?: string | null;
  repo_root: string;
  vcs_kind: string;
  operation_kind: string;
  ref_name?: string | null;
  before_oid?: string | null;
  after_oid?: string | null;
  commit_oid?: string | null;
  tree_oid?: string | null;
  parent_oids_json: string[];
  is_empty_commit?: boolean | null;
  trace_refs_json: string[];
  jj_operation_id?: string | null;
  git_reflog_selector?: string | null;
  diff_object_ref_id?: string | null;
  diff_object_ref_row_id?: string | null;
  status: string;
  started_at: Date;
  completed_at?: Date | null;
  metadata_json: DesignerTraceJson;
}

export type DesignerVcsOperationInit = Omit<DesignerVcsOperationRow, "id">;

export interface DesignerWorkspaceWorktreeRow {
  id: string;
  worktree_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  writer_id: string;
  owner_kind: string;
  local_path: string;
  remote_path?: string | null;
  repo_root: string;
  vcs_kind: string;
  branch_name: string;
  live_branch_name: string;
  base_oid?: string | null;
  head_oid?: string | null;
  status: string;
  metadata_json: DesignerTraceJson;
  created_at: Date;
  updated_at: Date;
}

export type DesignerWorkspaceWorktreeInit = Omit<DesignerWorkspaceWorktreeRow, "id">;

export interface DesignerWorkspaceMergeAttemptRow {
  id: string;
  merge_attempt_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  agent_turn_id?: string | null;
  agent_turn_row_id?: string | null;
  vcs_operation_id?: string | null;
  vcs_operation_row_id?: string | null;
  source_worktree_id?: string | null;
  source_worktree_row_id?: string | null;
  source_writer_id: string;
  source_branch_name: string;
  source_worktree_path: string;
  target_branch_name: string;
  target_worktree_path: string;
  base_oid?: string | null;
  source_oid?: string | null;
  target_before_oid?: string | null;
  target_after_oid?: string | null;
  result_status: string;
  conflict_count: number;
  diff_object_ref_id?: string | null;
  diff_object_ref_row_id?: string | null;
  started_at: Date;
  completed_at?: Date | null;
  metadata_json: DesignerTraceJson;
}

export type DesignerWorkspaceMergeAttemptInit = Omit<DesignerWorkspaceMergeAttemptRow, "id">;

export interface DesignerWorkspaceConflictRow {
  id: string;
  conflict_id: string;
  session_id: string;
  session_row_id: string;
  workspace_id: string;
  merge_attempt_id: string;
  merge_attempt_row_id?: string | null;
  agent_turn_id?: string | null;
  agent_turn_row_id?: string | null;
  path: string;
  conflict_kind: string;
  resolution_status: string;
  live_branch_name: string;
  source_branch_name: string;
  base_oid?: string | null;
  ours_oid?: string | null;
  theirs_oid?: string | null;
  marker_object_ref_id?: string | null;
  marker_object_ref_row_id?: string | null;
  diff_object_ref_id?: string | null;
  diff_object_ref_row_id?: string | null;
  metadata_json: DesignerTraceJson;
  created_at: Date;
  resolved_at?: Date | null;
}

export type DesignerWorkspaceConflictInit = Omit<DesignerWorkspaceConflictRow, "id">;

export interface DesignerAutonomyDecisionRow {
  id: string;
  decision_id: string;
  session_id: string;
  session_row_id: string;
  agent_turn_id?: string | null;
  agent_turn_row_id?: string | null;
  tool_invocation_id?: string | null;
  tool_invocation_row_id?: string | null;
  daemon_run_id?: string | null;
  decision_kind: string;
  decision: string;
  resource_kind: string;
  resource_id: string;
  owner_session_id?: string | null;
  lease_id?: string | null;
  command_id?: string | null;
  status: string;
  reason?: string | null;
  resource_json: DesignerTraceJson;
  invariant_json: DesignerTraceJson;
  outcome_json: DesignerTraceJson;
  decided_at: Date;
  metadata_json: DesignerTraceJson;
}

export type DesignerAutonomyDecisionInit = Omit<DesignerAutonomyDecisionRow, "id">;

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

export type DesignerTraceObjectContent = string | Uint8Array | ArrayBuffer;

export interface DesignerTraceObjectStoragePutInput {
  provider: string;
  bucket?: string | null;
  key: string;
  uri: string;
  content: Uint8Array;
  contentHash: string;
  contentEncoding?: string | null;
  contentType?: string | null;
  sizeBytes: number;
  accessPolicy: DesignerAccessPolicy;
  metadata: DesignerTraceJson;
}

export interface DesignerTraceObjectStoragePutReceipt {
  storageBackend?: string | null;
  bucket?: string | null;
  region?: string | null;
  key?: string | null;
  uri?: string | null;
  contentHash?: string | null;
  sizeBytes?: number | null;
  etag?: string | null;
  metadata?: DesignerTraceJson;
  receivedAt?: Date;
}

export interface DesignerTraceObjectStorageProvider {
  putObject(
    input: DesignerTraceObjectStoragePutInput,
  ): Promise<DesignerTraceObjectStoragePutReceipt>;
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

export interface RecordAgentTurnInput {
  turnId?: string;
  workspaceId?: string;
  provider: string;
  providerSessionId: string;
  providerTurnOrdinal?: number | null;
  providerTurnId?: string | null;
  cwd: string;
  repoRoot?: string | null;
  branchName?: string | null;
  model?: string | null;
  status?: string;
  startedAt?: Date;
  completedAt?: Date | null;
  metadata?: DesignerTraceJson;
  transcriptObjectRef?: DesignerObjectRefInput;
  uploadBackend?: string;
}

export interface RecordToolInvocationInput {
  invocationId?: string;
  agentTurnId?: string | null;
  agentTurnRowId?: string | null;
  toolName: string;
  commandHash?: string;
  commandSummary: string;
  cwd: string;
  startedAt?: Date;
  completedAt?: Date | null;
  exitCode?: number | null;
  status?: string;
  stdoutObjectRef?: DesignerObjectRefInput;
  stderrObjectRef?: DesignerObjectRefInput;
  metadata?: DesignerTraceJson;
  uploadBackend?: string;
}

export interface RecordVcsOperationInput {
  vcsOperationId?: string;
  agentTurnId?: string | null;
  agentTurnRowId?: string | null;
  toolInvocationId?: string | null;
  toolInvocationRowId?: string | null;
  repoRoot: string;
  vcsKind: string;
  operationKind: string;
  refName?: string | null;
  beforeOid?: string | null;
  afterOid?: string | null;
  commitOid?: string | null;
  treeOid?: string | null;
  parentOids?: string[];
  isEmptyCommit?: boolean | null;
  traceRefs?: string[];
  jjOperationId?: string | null;
  gitReflogSelector?: string | null;
  diffObjectRef?: DesignerObjectRefInput;
  status?: string;
  startedAt?: Date;
  completedAt?: Date | null;
  metadata?: DesignerTraceJson;
  uploadBackend?: string;
}

export interface RecordWorkspaceWorktreeInput {
  worktreeId?: string;
  workspaceId?: string;
  writerId?: string;
  ownerKind: string;
  localPath: string;
  remotePath?: string | null;
  repoRoot: string;
  vcsKind: string;
  branchName: string;
  liveBranchName?: string;
  baseOid?: string | null;
  headOid?: string | null;
  status?: string;
  createdAt?: Date;
  updatedAt?: Date;
  metadata?: DesignerTraceJson;
}

export interface RecordWorkspaceMergeAttemptInput {
  mergeAttemptId?: string;
  workspaceId?: string;
  agentTurnId?: string | null;
  agentTurnRowId?: string | null;
  vcsOperationId?: string | null;
  vcsOperationRowId?: string | null;
  sourceWorktreeId?: string | null;
  sourceWorktreeRowId?: string | null;
  sourceWriterId: string;
  sourceBranchName: string;
  sourceWorktreePath: string;
  targetBranchName?: string;
  targetWorktreePath: string;
  baseOid?: string | null;
  sourceOid?: string | null;
  targetBeforeOid?: string | null;
  targetAfterOid?: string | null;
  status?: string;
  conflictCount?: number;
  startedAt?: Date;
  completedAt?: Date | null;
  diffObjectRef?: DesignerObjectRefInput;
  metadata?: DesignerTraceJson;
  uploadBackend?: string;
}

export interface RecordWorkspaceConflictInput {
  conflictId?: string;
  workspaceId?: string;
  mergeAttemptId: string;
  mergeAttemptRowId?: string | null;
  agentTurnId?: string | null;
  agentTurnRowId?: string | null;
  path: string;
  conflictKind: string;
  resolutionStatus?: string;
  liveBranchName?: string;
  sourceBranchName: string;
  baseOid?: string | null;
  oursOid?: string | null;
  theirsOid?: string | null;
  markerObjectRef?: DesignerObjectRefInput;
  diffObjectRef?: DesignerObjectRefInput;
  metadata?: DesignerTraceJson;
  createdAt?: Date;
  resolvedAt?: Date | null;
  uploadBackend?: string;
}

export interface RecordAutonomyDecisionInput {
  decisionId?: string;
  agentTurnId?: string | null;
  agentTurnRowId?: string | null;
  toolInvocationId?: string | null;
  toolInvocationRowId?: string | null;
  daemonRunId?: string | null;
  decisionKind: string;
  decision: string;
  resourceKind: string;
  resourceId: string;
  ownerSessionId?: string | null;
  leaseId?: string | null;
  commandId?: string | null;
  status?: string;
  reason?: string | null;
  resource?: DesignerTraceJson;
  invariant?: DesignerTraceJson;
  outcome?: DesignerTraceJson;
  decidedAt?: Date;
  metadata?: DesignerTraceJson;
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

export interface ClaimUploadJobInput {
  uploadJobRowId: string;
  workerId: string;
  leaseExpiresAt?: Date;
}

export interface HeartbeatUploadJobInput {
  uploadJobRowId: string;
  workerId: string;
  leaseExpiresAt?: Date;
}

export interface ProcessUploadJobInput {
  uploadJob: DesignerUploadJobRow;
  objectRef: DesignerObjectRefRow;
  content: DesignerTraceObjectContent;
  provider: DesignerTraceObjectStorageProvider;
  workerId?: string;
  receivedAt?: Date;
  metadata?: DesignerTraceJson;
}

export interface RecordUploadFailureInput {
  uploadJob: DesignerUploadJobRow;
  error: unknown;
  workerId?: string;
  nextRetryAt?: Date | null;
  failedAt?: Date;
}

export interface DesignerTraceControlPlaneOptions {
  session: DesignerTraceSessionContext;
  now?: () => Date;
  idFactory?: (prefix: string) => string;
  defaultUploadBackend?: string;
  defaultLeaseMs?: number;
  accessProof?: DesignerAccessProof;
  hashCanonical?: (value: unknown) => string;
  hashContent?: (bytes: Uint8Array) => string;
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

export interface AgentTurnWrite {
  batchId: string;
  turn: InsertHandle<DesignerAgentTurnRow>;
  objectRefs: {
    transcript?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface ToolInvocationWrite {
  batchId: string;
  invocation: InsertHandle<DesignerToolInvocationRow>;
  objectRefs: {
    stdout?: InsertHandle<DesignerObjectRefRow>;
    stderr?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface VcsOperationWrite {
  batchId: string;
  operation: InsertHandle<DesignerVcsOperationRow>;
  objectRefs: {
    diff?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface WorkspaceWorktreeWrite {
  batchId: string;
  worktree: InsertHandle<DesignerWorkspaceWorktreeRow>;
}

export interface WorkspaceMergeAttemptWrite {
  batchId: string;
  mergeAttempt: InsertHandle<DesignerWorkspaceMergeAttemptRow>;
  objectRefs: {
    diff?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface WorkspaceConflictWrite {
  batchId: string;
  conflict: InsertHandle<DesignerWorkspaceConflictRow>;
  objectRefs: {
    marker?: InsertHandle<DesignerObjectRefRow>;
    diff?: InsertHandle<DesignerObjectRefRow>;
  };
  uploadJobs: InsertHandle<DesignerUploadJobRow>[];
}

export interface AutonomyDecisionWrite {
  batchId: string;
  decision: InsertHandle<DesignerAutonomyDecisionRow>;
}

export interface UploadReceiptWrite {
  batchId: string;
  receipt: InsertHandle<DesignerUploadReceiptRow>;
  uploadJobUpdate?: WriteHandle;
}

export interface UploadJobUpdateWrite {
  batchId: string;
  uploadJobUpdate: WriteHandle;
}

export interface ProcessUploadJobWrite {
  storageReceipt: DesignerTraceObjectStoragePutReceipt;
  receiptWrite: UploadReceiptWrite;
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
  agentTurns: makeTable<DesignerAgentTurnRow, DesignerAgentTurnInit>("agent_turns"),
  toolInvocations: makeTable<DesignerToolInvocationRow, DesignerToolInvocationInit>(
    "tool_invocations",
  ),
  vcsOperations: makeTable<DesignerVcsOperationRow, DesignerVcsOperationInit>("vcs_operations"),
  workspaceWorktrees: makeTable<DesignerWorkspaceWorktreeRow, DesignerWorkspaceWorktreeInit>(
    "workspace_worktrees",
  ),
  workspaceMergeAttempts: makeTable<
    DesignerWorkspaceMergeAttemptRow,
    DesignerWorkspaceMergeAttemptInit
  >("workspace_merge_attempts"),
  workspaceConflicts: makeTable<DesignerWorkspaceConflictRow, DesignerWorkspaceConflictInit>(
    "workspace_conflicts",
  ),
  autonomyDecisions: makeTable<DesignerAutonomyDecisionRow, DesignerAutonomyDecisionInit>(
    "autonomy_decisions",
  ),
} as const;

export function createDesignerTraceControlPlane(
  db: DesignerTraceDb,
  options: DesignerTraceControlPlaneOptions,
) {
  const now = options.now ?? (() => new Date());
  const idFactory = options.idFactory ?? createId;
  const defaultUploadBackend = options.defaultUploadBackend ?? "object-storage";
  const defaultLeaseMs = options.defaultLeaseMs ?? 5 * 60 * 1000;
  const hashCanonical = options.hashCanonical ?? hashDesignerTraceCanonical;
  const hashContent = options.hashContent ?? hashDesignerTraceContent;
  const session = options.session;

  const schemaVersion = () => session.schemaVersion ?? DESIGNER_TRACE_SCHEMA_VERSION;
  const accessPolicy = (
    override?: DesignerTraceJson,
    workspaceId = session.workspaceId,
  ): DesignerAccessPolicy => {
    const policy = {
      replication_scope: session.replicationScope,
      privacy_mode: session.privacyMode,
      explicit_access_proof_required: true,
      allowed_workspace_ids: [workspaceId],
      allowed_writer_ids: [session.writerId],
      ...(override ?? {}),
    } as DesignerAccessPolicy;
    assertAccessPolicy(policy, session, options.accessProof, now(), workspaceId);
    return policy;
  };

  const insertObjectRef = (
    batch: DesignerTraceBatch,
    input: DesignerObjectRefInput,
    role: DesignerObjectRefRole,
    workspaceId = session.workspaceId,
  ): InsertHandle<DesignerObjectRefRow> => {
    assertObjectRefInput(input);
    const createdAt = now();
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
      access_policy_json: accessPolicy(input.accessPolicy, workspaceId),
      metadata_json: input.metadata ?? {},
      created_at: createdAt,
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
    const createdAt = now();
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
      claimed_by: null,
      lease_expires_at: null,
      last_heartbeat_at: null,
      completed_at: null,
      failed_at: null,
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
      created_at: createdAt,
      updated_at: createdAt,
    };
    return batch.insert(designerTraceTables.uploadJobs, row);
  };

  const assertUploadRowsForSession = (
    uploadJob: DesignerUploadJobRow,
    objectRef: DesignerObjectRefRow,
    workerId?: string,
  ) => {
    if (uploadJob.session_id !== session.sessionId || objectRef.session_id !== session.sessionId) {
      throw new DesignerTraceUploadError("upload job and object ref must belong to this session");
    }
    if (
      uploadJob.session_row_id !== session.sessionRowId ||
      objectRef.session_row_id !== session.sessionRowId
    ) {
      throw new DesignerTraceUploadError(
        "upload job and object ref must belong to this session row",
      );
    }
    if (uploadJob.workspace_id !== objectRef.workspace_id) {
      throw new DesignerTraceUploadError(
        "upload job workspace does not match object ref workspace",
      );
    }
    if (uploadJob.object_ref_id && uploadJob.object_ref_id !== objectRef.object_ref_id) {
      throw new DesignerTraceUploadError("upload job object_ref_id does not match object ref");
    }
    if (uploadJob.object_ref_row_id && uploadJob.object_ref_row_id !== objectRef.id) {
      throw new DesignerTraceUploadError("upload job object_ref_row_id does not match object ref");
    }
    if (!["pending", "queued", "failed"].includes(uploadJob.status)) {
      throw new DesignerTraceUploadError(
        `upload job ${uploadJob.upload_job_id} cannot be processed from status ${uploadJob.status}`,
      );
    }
    if (uploadJob.claimed_by && workerId && uploadJob.claimed_by !== workerId) {
      throw new DesignerTraceUploadError(
        `upload job ${uploadJob.upload_job_id} is claimed by ${uploadJob.claimed_by}`,
      );
    }
    assertAccessPolicy(
      objectRef.access_policy_json as DesignerAccessPolicy,
      session,
      options.accessProof,
      now(),
      objectRef.workspace_id,
    );
    assertAccessPolicy(
      uploadJob.access_policy_json as DesignerAccessPolicy,
      session,
      options.accessProof,
      now(),
      uploadJob.workspace_id,
    );
  };

  const recordUploadReceipt = (input: RecordUploadReceiptInput): UploadReceiptWrite => {
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
          next_retry_at: null,
          claimed_by: null,
          lease_expires_at: null,
          last_heartbeat_at: null,
          completed_at: row.received_at,
          failed_at: null,
          updated_at: row.received_at,
        })
      : undefined;
    return {
      batchId: batch.batchId(),
      receipt,
      uploadJobUpdate,
    };
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
        access_policy_json: accessPolicy(input.accessPolicy, workspaceId),
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

    recordAgentTurn(input: RecordAgentTurnInput): AgentTurnWrite {
      const turnId = input.turnId ?? idFactory("agent-turn");
      const workspaceId = input.workspaceId ?? session.workspaceId;
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const batch = db.beginDirectBatch(designerTraceTables.agentTurns);
      const transcript = input.transcriptObjectRef
        ? insertObjectRef(batch, input.transcriptObjectRef, "turn_transcript", workspaceId)
        : undefined;
      const uploadJobs = transcript
        ? [insertUploadJob(batch, transcript, "agent_turn", turnId, uploadBackend, workspaceId)]
        : [];
      const row: DesignerAgentTurnInit = {
        turn_id: turnId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        workspace_id: workspaceId,
        provider: input.provider,
        provider_session_id: input.providerSessionId,
        provider_turn_ordinal: input.providerTurnOrdinal ?? null,
        provider_turn_id: input.providerTurnId ?? null,
        cwd: input.cwd,
        repo_root: input.repoRoot ?? null,
        branch_name: input.branchName ?? null,
        model: input.model ?? null,
        transcript_object_ref_id: transcript?.value.object_ref_id ?? null,
        transcript_object_ref_row_id: transcript?.value.id ?? null,
        status: input.status ?? "running",
        started_at: input.startedAt ?? now(),
        completed_at: input.completedAt ?? null,
        metadata_json: input.metadata ?? {},
      };
      return {
        batchId: batch.batchId(),
        turn: batch.insert(designerTraceTables.agentTurns, row),
        objectRefs: { transcript },
        uploadJobs,
      };
    },

    recordToolInvocation(input: RecordToolInvocationInput): ToolInvocationWrite {
      const invocationId = input.invocationId ?? idFactory("tool-invocation");
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const batch = db.beginDirectBatch(designerTraceTables.toolInvocations);
      const stdout = input.stdoutObjectRef
        ? insertObjectRef(batch, input.stdoutObjectRef, "tool_stdout")
        : undefined;
      const stderr = input.stderrObjectRef
        ? insertObjectRef(batch, input.stderrObjectRef, "tool_stderr")
        : undefined;
      const uploadJobs = [stdout, stderr]
        .filter((handle): handle is InsertHandle<DesignerObjectRefRow> => handle !== undefined)
        .map((objectRef) =>
          insertUploadJob(batch, objectRef, "tool_invocation", invocationId, uploadBackend),
        );
      const row: DesignerToolInvocationInit = {
        invocation_id: invocationId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        agent_turn_id: input.agentTurnId ?? null,
        agent_turn_row_id: input.agentTurnRowId ?? null,
        tool_name: input.toolName,
        command_hash:
          input.commandHash ??
          hashCanonical({
            tool_name: input.toolName,
            command_summary: input.commandSummary,
            cwd: input.cwd,
          }),
        command_summary: input.commandSummary,
        cwd: input.cwd,
        started_at: input.startedAt ?? now(),
        completed_at: input.completedAt ?? null,
        exit_code: input.exitCode ?? null,
        status: input.status ?? "running",
        stdout_object_ref_id: stdout?.value.object_ref_id ?? null,
        stdout_object_ref_row_id: stdout?.value.id ?? null,
        stderr_object_ref_id: stderr?.value.object_ref_id ?? null,
        stderr_object_ref_row_id: stderr?.value.id ?? null,
        metadata_json: input.metadata ?? {},
      };
      return {
        batchId: batch.batchId(),
        invocation: batch.insert(designerTraceTables.toolInvocations, row),
        objectRefs: { stdout, stderr },
        uploadJobs,
      };
    },

    recordVcsOperation(input: RecordVcsOperationInput): VcsOperationWrite {
      const vcsOperationId = input.vcsOperationId ?? idFactory("vcs-operation");
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const batch = db.beginDirectBatch(designerTraceTables.vcsOperations);
      const diff = input.diffObjectRef
        ? insertObjectRef(batch, input.diffObjectRef, "vcs_diff")
        : undefined;
      const uploadJobs = diff
        ? [insertUploadJob(batch, diff, "vcs_operation", vcsOperationId, uploadBackend)]
        : [];
      const row: DesignerVcsOperationInit = {
        vcs_operation_id: vcsOperationId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        agent_turn_id: input.agentTurnId ?? null,
        agent_turn_row_id: input.agentTurnRowId ?? null,
        tool_invocation_id: input.toolInvocationId ?? null,
        tool_invocation_row_id: input.toolInvocationRowId ?? null,
        repo_root: input.repoRoot,
        vcs_kind: input.vcsKind,
        operation_kind: input.operationKind,
        ref_name: input.refName ?? null,
        before_oid: input.beforeOid ?? null,
        after_oid: input.afterOid ?? null,
        commit_oid: input.commitOid ?? null,
        tree_oid: input.treeOid ?? null,
        parent_oids_json: input.parentOids ?? [],
        is_empty_commit: input.isEmptyCommit ?? null,
        trace_refs_json: input.traceRefs ?? [],
        jj_operation_id: input.jjOperationId ?? null,
        git_reflog_selector: input.gitReflogSelector ?? null,
        diff_object_ref_id: diff?.value.object_ref_id ?? null,
        diff_object_ref_row_id: diff?.value.id ?? null,
        status: input.status ?? "observed",
        started_at: input.startedAt ?? now(),
        completed_at: input.completedAt ?? null,
        metadata_json: input.metadata ?? {},
      };
      return {
        batchId: batch.batchId(),
        operation: batch.insert(designerTraceTables.vcsOperations, row),
        objectRefs: { diff },
        uploadJobs,
      };
    },

    recordWorkspaceWorktree(input: RecordWorkspaceWorktreeInput): WorkspaceWorktreeWrite {
      const worktreeId = input.worktreeId ?? idFactory("workspace-worktree");
      const workspaceId = input.workspaceId ?? session.workspaceId;
      const createdAt = input.createdAt ?? now();
      accessPolicy(undefined, workspaceId);
      const batch = db.beginDirectBatch(designerTraceTables.workspaceWorktrees);
      const row: DesignerWorkspaceWorktreeInit = {
        worktree_id: worktreeId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        workspace_id: workspaceId,
        writer_id: input.writerId ?? session.writerId,
        owner_kind: input.ownerKind,
        local_path: input.localPath,
        remote_path: input.remotePath ?? null,
        repo_root: input.repoRoot,
        vcs_kind: input.vcsKind,
        branch_name: input.branchName,
        live_branch_name: input.liveBranchName ?? "live",
        base_oid: input.baseOid ?? null,
        head_oid: input.headOid ?? null,
        status: input.status ?? "active",
        metadata_json: input.metadata ?? {},
        created_at: createdAt,
        updated_at: input.updatedAt ?? createdAt,
      };
      return {
        batchId: batch.batchId(),
        worktree: batch.insert(designerTraceTables.workspaceWorktrees, row),
      };
    },

    recordWorkspaceMergeAttempt(
      input: RecordWorkspaceMergeAttemptInput,
    ): WorkspaceMergeAttemptWrite {
      const mergeAttemptId = input.mergeAttemptId ?? idFactory("workspace-merge-attempt");
      const workspaceId = input.workspaceId ?? session.workspaceId;
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      const conflictCount = input.conflictCount ?? 0;
      if (conflictCount < 0) {
        throw new DesignerTraceControlPlaneError("merge attempt conflict count cannot be negative");
      }
      accessPolicy(undefined, workspaceId);
      const batch = db.beginDirectBatch(designerTraceTables.workspaceMergeAttempts);
      const diff = input.diffObjectRef
        ? insertObjectRef(batch, input.diffObjectRef, "workspace_merge_diff", workspaceId)
        : undefined;
      const uploadJobs = diff
        ? [
            insertUploadJob(
              batch,
              diff,
              "workspace_merge_attempt",
              mergeAttemptId,
              uploadBackend,
              workspaceId,
            ),
          ]
        : [];
      const row: DesignerWorkspaceMergeAttemptInit = {
        merge_attempt_id: mergeAttemptId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        workspace_id: workspaceId,
        agent_turn_id: input.agentTurnId ?? null,
        agent_turn_row_id: input.agentTurnRowId ?? null,
        vcs_operation_id: input.vcsOperationId ?? null,
        vcs_operation_row_id: input.vcsOperationRowId ?? null,
        source_worktree_id: input.sourceWorktreeId ?? null,
        source_worktree_row_id: input.sourceWorktreeRowId ?? null,
        source_writer_id: input.sourceWriterId,
        source_branch_name: input.sourceBranchName,
        source_worktree_path: input.sourceWorktreePath,
        target_branch_name: input.targetBranchName ?? "live",
        target_worktree_path: input.targetWorktreePath,
        base_oid: input.baseOid ?? null,
        source_oid: input.sourceOid ?? null,
        target_before_oid: input.targetBeforeOid ?? null,
        target_after_oid: input.targetAfterOid ?? null,
        result_status: input.status ?? "started",
        conflict_count: conflictCount,
        diff_object_ref_id: diff?.value.object_ref_id ?? null,
        diff_object_ref_row_id: diff?.value.id ?? null,
        started_at: input.startedAt ?? now(),
        completed_at: input.completedAt ?? null,
        metadata_json: input.metadata ?? {},
      };
      return {
        batchId: batch.batchId(),
        mergeAttempt: batch.insert(designerTraceTables.workspaceMergeAttempts, row),
        objectRefs: { diff },
        uploadJobs,
      };
    },

    recordWorkspaceConflict(input: RecordWorkspaceConflictInput): WorkspaceConflictWrite {
      const conflictId = input.conflictId ?? idFactory("workspace-conflict");
      const workspaceId = input.workspaceId ?? session.workspaceId;
      const uploadBackend = input.uploadBackend ?? defaultUploadBackend;
      accessPolicy(undefined, workspaceId);
      const batch = db.beginDirectBatch(designerTraceTables.workspaceConflicts);
      const marker = input.markerObjectRef
        ? insertObjectRef(batch, input.markerObjectRef, "workspace_conflict_marker", workspaceId)
        : undefined;
      const diff = input.diffObjectRef
        ? insertObjectRef(batch, input.diffObjectRef, "workspace_conflict_diff", workspaceId)
        : undefined;
      const uploadJobs = [marker, diff]
        .filter((handle): handle is InsertHandle<DesignerObjectRefRow> => handle !== undefined)
        .map((objectRef) =>
          insertUploadJob(
            batch,
            objectRef,
            "workspace_conflict",
            conflictId,
            uploadBackend,
            workspaceId,
          ),
        );
      const row: DesignerWorkspaceConflictInit = {
        conflict_id: conflictId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        workspace_id: workspaceId,
        merge_attempt_id: input.mergeAttemptId,
        merge_attempt_row_id: input.mergeAttemptRowId ?? null,
        agent_turn_id: input.agentTurnId ?? null,
        agent_turn_row_id: input.agentTurnRowId ?? null,
        path: input.path,
        conflict_kind: input.conflictKind,
        resolution_status: input.resolutionStatus ?? "unresolved",
        live_branch_name: input.liveBranchName ?? "live",
        source_branch_name: input.sourceBranchName,
        base_oid: input.baseOid ?? null,
        ours_oid: input.oursOid ?? null,
        theirs_oid: input.theirsOid ?? null,
        marker_object_ref_id: marker?.value.object_ref_id ?? null,
        marker_object_ref_row_id: marker?.value.id ?? null,
        diff_object_ref_id: diff?.value.object_ref_id ?? null,
        diff_object_ref_row_id: diff?.value.id ?? null,
        metadata_json: input.metadata ?? {},
        created_at: input.createdAt ?? now(),
        resolved_at: input.resolvedAt ?? null,
      };
      return {
        batchId: batch.batchId(),
        conflict: batch.insert(designerTraceTables.workspaceConflicts, row),
        objectRefs: { marker, diff },
        uploadJobs,
      };
    },

    recordAutonomyDecision(input: RecordAutonomyDecisionInput): AutonomyDecisionWrite {
      const decisionId = input.decisionId ?? idFactory("autonomy-decision");
      const batch = db.beginDirectBatch(designerTraceTables.autonomyDecisions);
      const row: DesignerAutonomyDecisionInit = {
        decision_id: decisionId,
        session_id: session.sessionId,
        session_row_id: session.sessionRowId,
        agent_turn_id: input.agentTurnId ?? null,
        agent_turn_row_id: input.agentTurnRowId ?? null,
        tool_invocation_id: input.toolInvocationId ?? null,
        tool_invocation_row_id: input.toolInvocationRowId ?? null,
        daemon_run_id: input.daemonRunId ?? null,
        decision_kind: input.decisionKind,
        decision: input.decision,
        resource_kind: input.resourceKind,
        resource_id: input.resourceId,
        owner_session_id: input.ownerSessionId ?? null,
        lease_id: input.leaseId ?? null,
        command_id: input.commandId ?? null,
        status: input.status ?? "observed",
        reason: input.reason ?? null,
        resource_json: input.resource ?? {},
        invariant_json: input.invariant ?? {},
        outcome_json: input.outcome ?? {},
        decided_at: input.decidedAt ?? now(),
        metadata_json: input.metadata ?? {},
      };
      return {
        batchId: batch.batchId(),
        decision: batch.insert(designerTraceTables.autonomyDecisions, row),
      };
    },

    recordUploadReceipt(input: RecordUploadReceiptInput): UploadReceiptWrite {
      return recordUploadReceipt(input);
    },

    claimUploadJob(input: ClaimUploadJobInput): UploadJobUpdateWrite {
      const claimedAt = now();
      const leaseExpiresAt = input.leaseExpiresAt ?? new Date(claimedAt.getTime() + defaultLeaseMs);
      assertFutureLease(claimedAt, leaseExpiresAt);
      const batch = db.beginDirectBatch(designerTraceTables.uploadJobs);
      const uploadJobUpdate = batch.update(designerTraceTables.uploadJobs, input.uploadJobRowId, {
        status: "queued",
        claimed_by: input.workerId,
        lease_expires_at: leaseExpiresAt,
        last_heartbeat_at: claimedAt,
        updated_at: claimedAt,
      });
      return {
        batchId: batch.batchId(),
        uploadJobUpdate,
      };
    },

    heartbeatUploadJob(input: HeartbeatUploadJobInput): UploadJobUpdateWrite {
      const heartbeatAt = now();
      const leaseExpiresAt =
        input.leaseExpiresAt ?? new Date(heartbeatAt.getTime() + defaultLeaseMs);
      assertFutureLease(heartbeatAt, leaseExpiresAt);
      const batch = db.beginDirectBatch(designerTraceTables.uploadJobs);
      const uploadJobUpdate = batch.update(designerTraceTables.uploadJobs, input.uploadJobRowId, {
        claimed_by: input.workerId,
        lease_expires_at: leaseExpiresAt,
        last_heartbeat_at: heartbeatAt,
        updated_at: heartbeatAt,
      });
      return {
        batchId: batch.batchId(),
        uploadJobUpdate,
      };
    },

    async processUploadJob(input: ProcessUploadJobInput): Promise<ProcessUploadJobWrite> {
      assertUploadRowsForSession(input.uploadJob, input.objectRef, input.workerId);
      const content = normalizeObjectContent(input.content);
      const computedContentHash = hashContent(content);
      const expectedContentHash = input.objectRef.content_hash ?? computedContentHash;
      if (input.objectRef.content_hash && input.objectRef.content_hash !== computedContentHash) {
        throw new DesignerTraceUploadError(
          `object content hash mismatch for ${input.objectRef.object_ref_id}`,
        );
      }
      if (input.objectRef.size_bytes !== null && input.objectRef.size_bytes !== undefined) {
        if (input.objectRef.size_bytes !== content.byteLength) {
          throw new DesignerTraceUploadError(
            `object size mismatch for ${input.objectRef.object_ref_id}`,
          );
        }
      }
      const policy = input.objectRef.access_policy_json as DesignerAccessPolicy;
      const storageReceipt = await input.provider.putObject({
        provider: input.objectRef.provider,
        bucket: input.objectRef.bucket ?? null,
        key: input.objectRef.key,
        uri: input.objectRef.uri,
        content,
        contentHash: expectedContentHash,
        contentEncoding: input.objectRef.content_encoding ?? null,
        contentType: input.objectRef.content_type ?? null,
        sizeBytes: content.byteLength,
        accessPolicy: policy,
        metadata: {
          ...input.objectRef.metadata_json,
          ...(input.metadata ?? {}),
        },
      });
      const receiptWrite = recordUploadReceipt({
        uploadJobId: input.uploadJob.upload_job_id,
        uploadJobRowId: input.uploadJob.id,
        objectRefId: input.objectRef.object_ref_id,
        objectRefRowId: input.objectRef.id,
        backend: input.uploadJob.backend,
        storageBackend: storageReceipt.storageBackend ?? input.objectRef.provider,
        bucket: storageReceipt.bucket ?? input.objectRef.bucket ?? null,
        region: storageReceipt.region ?? null,
        key: storageReceipt.key ?? input.objectRef.key,
        uri: storageReceipt.uri ?? input.objectRef.uri,
        receivedAt: input.receivedAt ?? storageReceipt.receivedAt ?? now(),
        metadata: {
          ...(storageReceipt.metadata ?? {}),
          content_hash: storageReceipt.contentHash ?? expectedContentHash,
          size_bytes: storageReceipt.sizeBytes ?? content.byteLength,
          ...(storageReceipt.etag ? { etag: storageReceipt.etag } : {}),
        },
      });
      return {
        storageReceipt,
        receiptWrite,
      };
    },

    recordUploadFailure(input: RecordUploadFailureInput): UploadJobUpdateWrite {
      if (input.uploadJob.session_id !== session.sessionId) {
        throw new DesignerTraceUploadError("upload job must belong to this session");
      }
      if (
        input.uploadJob.claimed_by &&
        input.workerId &&
        input.uploadJob.claimed_by !== input.workerId
      ) {
        throw new DesignerTraceUploadError(
          `upload job ${input.uploadJob.upload_job_id} is claimed by ${input.uploadJob.claimed_by}`,
        );
      }
      const failedAt = input.failedAt ?? now();
      const batch = db.beginDirectBatch(designerTraceTables.uploadJobs);
      const uploadJobUpdate = batch.update(designerTraceTables.uploadJobs, input.uploadJob.id, {
        status: "failed",
        attempt_count: input.uploadJob.attempt_count + 1,
        last_error: formatUploadError(input.error),
        next_retry_at: input.nextRetryAt ?? null,
        claimed_by: null,
        lease_expires_at: null,
        last_heartbeat_at: null,
        completed_at: null,
        failed_at: failedAt,
        updated_at: failedAt,
      });
      return {
        batchId: batch.batchId(),
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

export function hashDesignerTraceCanonical(value: unknown): string {
  return `sha256:${sha256Hex(stableJson(value))}`;
}

export function hashDesignerTraceContent(bytes: Uint8Array): string {
  return `sha256:${sha256Hex(bytes)}`;
}

function assertObjectRefInput(input: DesignerObjectRefInput): void {
  if (input.provider.trim() === "") {
    throw new DesignerTraceUploadError("object ref provider is required");
  }
  if (input.key.trim() === "") {
    throw new DesignerTraceUploadError("object ref key is required");
  }
  if ((input.provider === "s3" || input.provider === "oci") && !input.bucket) {
    throw new DesignerTraceUploadError(`${input.provider} object refs require a bucket`);
  }
  if (input.sizeBytes !== null && input.sizeBytes !== undefined && input.sizeBytes < 0) {
    throw new DesignerTraceUploadError("object ref sizeBytes must be non-negative");
  }
}

function assertAccessPolicy(
  policy: DesignerAccessPolicy,
  session: DesignerTraceSessionContext,
  proof: DesignerAccessProof | undefined,
  currentTime: Date,
  workspaceId: string,
): void {
  if (policy.replication_scope !== session.replicationScope) {
    throw new DesignerTraceAccessPolicyError(
      `replication scope ${String(policy.replication_scope)} does not match session scope ${session.replicationScope}`,
    );
  }
  if (policy.privacy_mode !== session.privacyMode) {
    throw new DesignerTraceAccessPolicyError(
      `privacy mode ${String(policy.privacy_mode)} does not match session privacy mode ${session.privacyMode}`,
    );
  }
  if (policy.allowed_workspace_ids !== undefined) {
    if (!Array.isArray(policy.allowed_workspace_ids)) {
      throw new DesignerTraceAccessPolicyError("allowed_workspace_ids must be an array");
    }
    if (!policy.allowed_workspace_ids.includes(workspaceId)) {
      throw new DesignerTraceAccessPolicyError(
        `workspace ${workspaceId} is not allowed by the access policy`,
      );
    }
  }
  if (policy.allowed_writer_ids !== undefined) {
    if (!Array.isArray(policy.allowed_writer_ids)) {
      throw new DesignerTraceAccessPolicyError("allowed_writer_ids must be an array");
    }
    if (!policy.allowed_writer_ids.includes(session.writerId)) {
      throw new DesignerTraceAccessPolicyError(
        `writer ${session.writerId} is not allowed by the access policy`,
      );
    }
  }
  if (!policy.explicit_access_proof_required) {
    return;
  }
  if (!proof) {
    throw new DesignerTraceAccessPolicyError("explicit access proof is required");
  }
  if (policy.proof_id && policy.proof_id !== proof.proofId) {
    throw new DesignerTraceAccessPolicyError("access proof id does not match policy");
  }
  if (policy.proof_kind && policy.proof_kind !== proof.kind) {
    throw new DesignerTraceAccessPolicyError("access proof kind does not match policy");
  }
  if (proof.workspaceId && proof.workspaceId !== workspaceId) {
    throw new DesignerTraceAccessPolicyError(
      "access proof workspace does not match object workspace",
    );
  }
  if (proof.writerId && proof.writerId !== session.writerId) {
    throw new DesignerTraceAccessPolicyError("access proof writer does not match session writer");
  }
  if (proof.issuedAt && proof.issuedAt.getTime() > currentTime.getTime()) {
    throw new DesignerTraceAccessPolicyError("access proof was issued in the future");
  }
  if (proof.expiresAt && proof.expiresAt.getTime() <= currentTime.getTime()) {
    throw new DesignerTraceAccessPolicyError("access proof has expired");
  }
}

function assertFutureLease(currentTime: Date, leaseExpiresAt: Date): void {
  if (leaseExpiresAt.getTime() <= currentTime.getTime()) {
    throw new DesignerTraceUploadError("upload job lease must expire in the future");
  }
}

function normalizeObjectContent(content: DesignerTraceObjectContent): Uint8Array {
  if (typeof content === "string") {
    return new TextEncoder().encode(content);
  }
  if (content instanceof Uint8Array) {
    return content;
  }
  return new Uint8Array(content);
}

function formatUploadError(error: unknown): string {
  if (error instanceof Error) {
    return error.message.slice(0, 4096);
  }
  return String(error).slice(0, 4096);
}

const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const SHA256_INITIAL = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

function sha256Hex(input: string | Uint8Array): string {
  const inputBytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  const bitLength = inputBytes.length * 8;
  const paddedLength = Math.ceil((inputBytes.length + 1 + 8) / 64) * 64;
  const padded = new Uint8Array(paddedLength);
  padded.set(inputBytes);
  padded[inputBytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLength - 8, Math.floor(bitLength / 0x100000000));
  view.setUint32(paddedLength - 4, bitLength >>> 0);

  const hash = new Uint32Array(SHA256_INITIAL);
  const words = new Uint32Array(64);
  for (let offset = 0; offset < paddedLength; offset += 64) {
    for (let index = 0; index < 16; index += 1) {
      words[index] = view.getUint32(offset + index * 4);
    }
    for (let index = 16; index < 64; index += 1) {
      const s0 =
        rotateRight(words[index - 15], 7) ^
        rotateRight(words[index - 15], 18) ^
        (words[index - 15] >>> 3);
      const s1 =
        rotateRight(words[index - 2], 17) ^
        rotateRight(words[index - 2], 19) ^
        (words[index - 2] >>> 10);
      words[index] = (words[index - 16] + s0 + words[index - 7] + s1) >>> 0;
    }

    let a = hash[0];
    let b = hash[1];
    let c = hash[2];
    let d = hash[3];
    let e = hash[4];
    let f = hash[5];
    let g = hash[6];
    let h = hash[7];

    for (let index = 0; index < 64; index += 1) {
      const s1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + ch + SHA256_K[index] + words[index]) >>> 0;
      const s0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + maj) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    hash[0] = (hash[0] + a) >>> 0;
    hash[1] = (hash[1] + b) >>> 0;
    hash[2] = (hash[2] + c) >>> 0;
    hash[3] = (hash[3] + d) >>> 0;
    hash[4] = (hash[4] + e) >>> 0;
    hash[5] = (hash[5] + f) >>> 0;
    hash[6] = (hash[6] + g) >>> 0;
    hash[7] = (hash[7] + h) >>> 0;
  }
  return Array.from(hash)
    .map((word) => word.toString(16).padStart(8, "0"))
    .join("");
}

function rotateRight(value: number, bits: number): number {
  return (value >>> bits) | (value << (32 - bits));
}

function stableJson(value: unknown, seen = new WeakSet<object>()): string {
  if (value === undefined) {
    return "undefined";
  }
  if (typeof value === "bigint") {
    return JSON.stringify(value.toString());
  }
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? "undefined";
  }
  if (value instanceof Date) {
    return JSON.stringify(value.toISOString());
  }
  if (seen.has(value)) {
    throw new DesignerTraceControlPlaneError("cannot hash circular JSON values");
  }
  seen.add(value);
  if (Array.isArray(value)) {
    const json = `[${value.map((entry) => stableJson(entry, seen)).join(",")}]`;
    seen.delete(value);
    return json;
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  const json = `{${keys
    .filter((key) => record[key] !== undefined)
    .map((key) => `${JSON.stringify(key)}:${stableJson(record[key], seen)}`)
    .join(",")}}`;
  seen.delete(value);
  return json;
}
