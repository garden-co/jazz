import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdir, stat } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import readline from "node:readline";
import {
  type AgentRun,
  type AgentRunSummary,
  type AgentStateSnapshot,
  type AgentClaimRecord,
  type Artifact,
  type BranchFileReviewStateRecord,
  type CommitTurnOperationRecord,
  type CommitTurnResultRecord,
  type ContextDigestRecord,
  type CursorReviewOperationRecord,
  type CursorReviewOperationResultRecord,
  type DaemonLogCheckpoint,
  type DaemonLogChunk,
  type DaemonLogEvent,
  type DaemonLogSource,
  type DaemonLogSummary,
  type JobEventRecord,
  type JobRecord,
  createAgentDataStore,
  type CancelJobInput,
  type ClaimJobInput,
  type ListAgentClaimsInput,
  type ListBranchFileReviewStatesInput,
  type ListCommitTurnOperationsInput,
  type ListContextDigestsInput,
  type ListCursorReviewOperationsInput,
  type ListDaemonLogEventsInput,
  type ListDaemonLogSourcesInput,
  type ListDaemonLogSummariesInput,
  type ListJobsInput,
  type ListTaskRecordsInput,
  type RecordAgentClaimInput,
  type RecordBranchFileReviewStateInput,
  type RecordCommitTurnOperationInput,
  type RecordCommitTurnResultInput,
  type RecordContextDigestInput,
  type RecordCursorReviewOperationInput,
  type RecordCursorReviewResultInput,
  type RecordDaemonLogCheckpointInput,
  type RecordDaemonLogChunkInput,
  type RecordDaemonLogEventInput,
  type RecordDaemonLogSourceInput,
  type RecordDaemonLogSummaryInput,
  type RecordJobInput,
  type MemoryLink,
  type RecordArtifactInput,
  type RecordItemCompletedInput,
  type RecordItemStartedInput,
  type RecordRunCompletedInput,
  type RecordRunStartedInput,
  type RecordWorkspaceSnapshotInput,
  type ReleaseAgentClaimInput,
  type RunItem,
  type RenewAgentClaimInput,
  type SemanticEvent,
  type SourceFile,
  type TaskRecord,
  type UpdateJobInput,
  type WireEvent,
  type WorkspaceSnapshot,
  type UpsertTaskRecordInput,
} from "./index.js";
import { projectDoDesignerTasks, syncDoDesignerTasks } from "./task_records.js";

interface SerializedTaskRecord {
  taskId: string;
  context: string;
  title: string;
  status: string;
  priority: string;
  placement: string;
  focusRank: number | null;
  project: string;
  issue: string | null;
  branch: string | null;
  workspace: string | null;
  plan: string | null;
  pr: string | null;
  tagsJson: unknown | null;
  nextText: string | null;
  contextText: string | null;
  notesText: string | null;
  annotationsJson: unknown | null;
  sourceKind: string | null;
  sourcePath: string | null;
  metadataJson: unknown | null;
  createdAt: string;
  updatedAt: string;
}

interface SerializedAgentRun {
  runId: string;
  agentId: string;
  threadId: string | null;
  turnId: string | null;
  cwd: string | null;
  repoRoot: string | null;
  requestSummary: string | null;
  status: string;
  startedAt: string;
  endedAt: string | null;
  contextJson: unknown | null;
  sourceTracePath: string | null;
}

interface SerializedRunItem {
  runId: string;
  itemId: string;
  itemKind: string;
  sequence: number;
  phase: string | null;
  status: string;
  summaryJson: unknown | null;
  startedAt: string;
  completedAt: string | null;
}

interface SerializedSemanticEvent {
  eventId: string;
  runId: string;
  itemId: string | null;
  eventType: string;
  summaryText: string | null;
  payloadJson: unknown | null;
  occurredAt: string;
}

interface SerializedWireEvent {
  eventId: string;
  runId: string | null;
  connectionId: number | null;
  sessionId: number | null;
  direction: string;
  method: string | null;
  requestId: string | null;
  payloadJson: unknown | null;
  occurredAt: string;
}

interface SerializedArtifact {
  artifactId: string;
  runId: string;
  artifactKind: string;
  absolutePath: string;
  title: string | null;
  checksum: string | null;
  createdAt: string;
}

interface SerializedWorkspaceSnapshot {
  snapshotId: string;
  runId: string;
  repoRoot: string;
  branch: string | null;
  headCommit: string | null;
  dirtyPathCount: number | null;
  snapshotJson: unknown | null;
  capturedAt: string;
}

interface SerializedMemoryLink {
  linkId: string;
  runId: string | null;
  itemId: string | null;
  memoryScope: string;
  memoryRef: string | null;
  queryText: string | null;
  linkJson: unknown | null;
  createdAt: string;
}

interface SerializedSourceFile {
  sourceFileId: string;
  runId: string | null;
  fileKind: string;
  absolutePath: string;
  checksum: string | null;
  createdAt: string;
}

interface SerializedDaemonLogSource {
  sourceId: string;
  manager: string;
  daemonName: string;
  stream: string;
  hostId: string | null;
  logPath: string;
  configPath: string | null;
  repoRoot: string | null;
  workspaceRoot: string | null;
  ownerAgent: string | null;
  flowDaemonName: string | null;
  launchdLabel: string | null;
  retentionClass: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}

interface SerializedDaemonLogChunk {
  chunkId: string;
  sourceId: string;
  daemonName: string;
  stream: string;
  hostId: string | null;
  logPath: string;
  fileFingerprint: string;
  startOffset: number;
  endOffset: number;
  firstLineNo: number;
  lastLineNo: number;
  lineCount: number;
  byteCount: number;
  firstAt: string | null;
  lastAt: string | null;
  sha256: string;
  bodyRef: string | null;
  bodyPreview: string | null;
  compression: string;
  ingestedAt: string;
}

interface SerializedDaemonLogEvent {
  eventId: string;
  sourceId: string;
  chunkId: string;
  daemonName: string;
  stream: string;
  seq: number;
  lineNo: number;
  at: string | null;
  level: string;
  message: string;
  fieldsJson: unknown | null;
  repoRoot: string | null;
  workspaceRoot: string | null;
  conversation: string | null;
  conversationHash: string | null;
  runId: string | null;
  jobId: string | null;
  traceId: string | null;
  spanId: string | null;
  errorKind: string | null;
  createdAt: string;
}

interface SerializedDaemonLogCheckpoint {
  checkpointId: string;
  sourceId: string;
  hostId: string | null;
  logPath: string;
  fileFingerprint: string;
  inode: string | null;
  device: string | null;
  offset: number;
  lineNo: number;
  lastChunkId: string | null;
  lastEventId: string | null;
  lastSeenAt: string | null;
  updatedAt: string;
}

interface SerializedDaemonLogSummary {
  summaryId: string;
  sourceId: string;
  daemonName: string;
  windowStart: string;
  windowEnd: string;
  levelCountsJson: unknown | null;
  errorCount: number;
  warningCount: number;
  firstErrorEventId: string | null;
  lastErrorEventId: string | null;
  topErrorKindsJson: unknown | null;
  summaryText: string | null;
  createdAt: string;
}

interface RecordDaemonLogBatchInput {
  source?: RecordDaemonLogSourceInput;
  chunk: RecordDaemonLogChunkInput;
  events: RecordDaemonLogEventInput[];
}

interface SerializedAgentStateSnapshot {
  snapshotId: string;
  agentId: string;
  stateVersion: number | null;
  status: string | null;
  stateJson: unknown | null;
  capturedAt: string;
}

interface SerializedCursorReviewOperationResult {
  eventId: string;
  operationId: string;
  status: string;
  clientId: string | null;
  repoRoot: string | null;
  message: string | null;
  processedAt: string;
}

interface SerializedCursorReviewOperation {
  eventId: string;
  operationId: string;
  operationType: string;
  repoRoot: string | null;
  workspaceRoot: string | null;
  bookmark: string | null;
  relPath: string | null;
  note: string | null;
  sourceSessionId: string | null;
  sourceChatKind: string | null;
  createdAt: string;
  latestResult: SerializedCursorReviewOperationResult | null;
}

interface SerializedBranchFileReviewState {
  eventId: string;
  repoRoot: string;
  workspaceRoot: string | null;
  bookmark: string;
  relPath: string;
  status: string;
  note: string | null;
  sourceSessionId: string | null;
  sourceChatKind: string | null;
  createdAt: string;
}

interface SerializedCommitTurnResult {
  eventId: string;
  operationId: string;
  status: string;
  agentId: string | null;
  runId: string | null;
  threadId: string | null;
  repoRoot: string | null;
  model: string | null;
  effort: string | null;
  traceRef: string | null;
  message: string | null;
  classification: string | null;
  title: string | null;
  description: string | null;
  commitMessage: string | null;
  todoItems: string[] | null;
  notes: string | null;
  group: string | null;
  groupReason: string | null;
  groupIsNew: boolean;
  snapshotCommitId: string | null;
  reviewJobId: string | null;
  conversationHash: string | null;
  processedAt: string;
}

interface SerializedCommitTurnOperation {
  eventId: string;
  operationId: string;
  provider: string;
  sessionId: string;
  conversation: string;
  conversationHash: string;
  trigger: string;
  turnOrdinal: number;
  sessionEventId: string;
  repoRoot: string | null;
  repoRoots: string[] | null;
  cwd: string | null;
  artifactPath: string | null;
  promptPreview: string | null;
  sourceChatKind: string | null;
  createdAt: string;
  latestResult: SerializedCommitTurnResult | null;
}

interface SerializedAgentClaim {
  eventId: string;
  claimId: string;
  scope: string;
  owner: string;
  ownerSession: string | null;
  mode: string | null;
  note: string | null;
  repoRoot: string | null;
  workspaceRoot: string | null;
  startedAt: string;
  expiresAt: string;
  heartbeatAt: string;
  releasedAt: string | null;
  status: string;
}

interface SerializedContextDigest {
  eventId: string;
  digestId: string;
  targetProvider: string;
  targetSession: string;
  targetTurnOrdinal: number;
  targetConversation: string;
  targetConversationHash: string;
  sourceSession: string;
  sourceWatermarkKind: string;
  sourceWatermarkValue: string;
  sourceConversationHash: string | null;
  kind: string;
  digestText: string;
  modelUsed: string | null;
  score: number | null;
  confidence: string | null;
  reason: string | null;
  generatedAt: string;
  expiresAt: string | null;
  status: string;
}

interface SerializedJob {
  eventId: string;
  jobId: string;
  kind: string;
  status: string;
  createdAt: string;
  updatedAt: string;
  claimedBy: string | null;
  leaseExpiresAt: string | null;
  attempt: number;
  payloadJson: unknown | null;
  resultJson: unknown | null;
  repoRoot: string | null;
  workspaceRoot: string | null;
  sourceChatKind: string | null;
  dedupeKey: string | null;
  targetSession: string | null;
  targetTurnWatermark: string | null;
  sourceSession: string | null;
  sourceWatermark: string | null;
  note: string | null;
}

interface SerializedJobEvent {
  eventId: string;
  jobId: string;
  eventType: string;
  status: string | null;
  claimedBy: string | null;
  leaseExpiresAt: string | null;
  attempt: number | null;
  note: string | null;
  payloadJson: unknown | null;
  resultJson: unknown | null;
  occurredAt: string;
}

interface SerializedAgentRunSummary {
  run: SerializedAgentRun;
  items: SerializedRunItem[];
  semanticEvents: SerializedSemanticEvent[];
  wireEvents: SerializedWireEvent[];
  artifacts: SerializedArtifact[];
  workspaceSnapshots: SerializedWorkspaceSnapshot[];
  memoryLinks: SerializedMemoryLink[];
  sourceFiles: SerializedSourceFile[];
  latestAgentState: SerializedAgentStateSnapshot | null;
}

function expandHomePath(value: string): string {
  if (value === "~") {
    return homedir();
  }
  if (value.startsWith("~/")) {
    return path.join(homedir(), value.slice(2));
  }
  return path.resolve(value);
}

async function maybeStat(pathname: string) {
  return stat(pathname).catch((error: unknown) => {
    const code =
      typeof error === "object" && error !== null && "code" in error
        ? String((error as { code?: unknown }).code ?? "")
        : "";
    if (code === "ENOENT") {
      return null;
    }
    throw error;
  });
}

function legacyDirectoryStoreFilename(dataPath: string): string {
  const baseName = path.basename(dataPath);
  if (baseName.endsWith(".db")) {
    return `${baseName.slice(0, -3)}.sqlite`;
  }
  return `${baseName}.sqlite`;
}

async function resolvePersistentDataPath(dataPath: string): Promise<string> {
  const normalizedPath = expandHomePath(dataPath);
  const currentStat = await maybeStat(normalizedPath);
  if (!currentStat?.isDirectory()) {
    return normalizedPath;
  }

  const storeFilename = legacyDirectoryStoreFilename(normalizedPath);
  const directoryCandidate = path.join(normalizedPath, storeFilename);
  const siblingCandidate = path.join(path.dirname(normalizedPath), storeFilename);
  for (const candidate of [directoryCandidate, siblingCandidate]) {
    const candidateStat = await maybeStat(candidate);
    if (candidateStat?.isFile()) {
      return candidate;
    }
    if (candidateStat?.isDirectory()) {
      throw new Error(
        `Jazz data path ${normalizedPath} is a directory, and fallback path ${candidate} is also a directory`,
      );
    }
  }
  return directoryCandidate;
}

function readFlag(flag: string): string | undefined {
  const index = process.argv.indexOf(flag);
  if (index === -1) {
    return undefined;
  }
  return process.argv[index + 1];
}

function hasFlag(flag: string): boolean {
  return process.argv.includes(flag);
}

function readWriteTierFlag(): "local" | "edge" | "global" | undefined {
  const tier = readFlag("--tier");
  if (!tier) {
    return undefined;
  }
  if (tier === "local" || tier === "edge" || tier === "global") {
    return tier;
  }
  throw new Error(`invalid --tier ${tier}; expected local, edge, or global`);
}

function requireCommand(): string {
  const command = process.argv[2];
  if (!command) {
    throw new Error("missing command");
  }
  return command;
}

function readJsonInput<T>(command: string): T {
  const inlineJson = readFlag("--input-json");
  const inputFile = readFlag("--input-file");

  if (inlineJson && inputFile) {
    throw new Error(
      `${command} accepts only one of --input-json or --input-file`,
    );
  }

  const text = inputFile
    ? readFileSync(expandHomePath(inputFile), "utf8")
    : inlineJson
      ? inlineJson
      : !process.stdin.isTTY
        ? readFileSync(0, "utf8")
        : null;

  if (!text) {
    throw new Error(
      `${command} requires --input-json, --input-file, or stdin JSON`,
    );
  }

  return JSON.parse(text) as T;
}

function parseCsvFlag(flag: string): string[] | undefined {
  const raw = readFlag(flag);
  if (!raw) {
    return undefined;
  }
  const values = raw
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return values.length > 0 ? values : undefined;
}

function serializeTaskRecord(record: TaskRecord): SerializedTaskRecord {
  return {
    taskId: record.task_id,
    context: record.context,
    title: record.title,
    status: record.status,
    priority: record.priority,
    placement: record.placement,
    focusRank: record.focus_rank ?? null,
    project: record.project,
    issue: record.issue ?? null,
    branch: record.branch ?? null,
    workspace: record.workspace ?? null,
    plan: record.plan ?? null,
    pr: record.pr ?? null,
    tagsJson: record.tags_json ?? null,
    nextText: record.next_text ?? null,
    contextText: record.context_text ?? null,
    notesText: record.notes_text ?? null,
    annotationsJson: record.annotations_json ?? null,
    sourceKind: record.source_kind ?? null,
    sourcePath: record.source_path ?? null,
    metadataJson: record.metadata_json ?? null,
    createdAt: record.created_at.toISOString(),
    updatedAt: record.updated_at.toISOString(),
  };
}

function serializeNullableDate(value: Date | undefined): string | null {
  return value ? value.toISOString() : null;
}

function serializeAgentRun(run: AgentRun): SerializedAgentRun {
  return {
    runId: run.run_id,
    agentId: run.agent_id,
    threadId: run.thread_id ?? null,
    turnId: run.turn_id ?? null,
    cwd: run.cwd ?? null,
    repoRoot: run.repo_root ?? null,
    requestSummary: run.request_summary ?? null,
    status: run.status,
    startedAt: run.started_at.toISOString(),
    endedAt: serializeNullableDate(run.ended_at),
    contextJson: run.context_json ?? null,
    sourceTracePath: run.source_trace_path ?? null,
  };
}

function serializeRunItem(item: RunItem): SerializedRunItem {
  return {
    runId: item.run_id,
    itemId: item.item_id,
    itemKind: item.item_kind,
    sequence: item.sequence,
    phase: item.phase ?? null,
    status: item.status,
    summaryJson: item.summary_json ?? null,
    startedAt: item.started_at.toISOString(),
    completedAt: serializeNullableDate(item.completed_at),
  };
}

function serializeSemanticEvent(event: SemanticEvent): SerializedSemanticEvent {
  return {
    eventId: event.event_id,
    runId: event.run_id,
    itemId: event.item_id ?? null,
    eventType: event.event_type,
    summaryText: event.summary_text ?? null,
    payloadJson: event.payload_json ?? null,
    occurredAt: event.occurred_at.toISOString(),
  };
}

function serializeWireEvent(event: WireEvent): SerializedWireEvent {
  return {
    eventId: event.event_id,
    runId: event.run_id ?? null,
    connectionId: event.connection_id ?? null,
    sessionId: event.session_id ?? null,
    direction: event.direction,
    method: event.method ?? null,
    requestId: event.request_id ?? null,
    payloadJson: event.payload_json ?? null,
    occurredAt: event.occurred_at.toISOString(),
  };
}

function serializeArtifact(artifact: Artifact): SerializedArtifact {
  return {
    artifactId: artifact.artifact_id,
    runId: artifact.run_id,
    artifactKind: artifact.artifact_kind,
    absolutePath: artifact.absolute_path,
    title: artifact.title ?? null,
    checksum: artifact.checksum ?? null,
    createdAt: artifact.created_at.toISOString(),
  };
}

function serializeWorkspaceSnapshot(
  snapshot: WorkspaceSnapshot,
): SerializedWorkspaceSnapshot {
  return {
    snapshotId: snapshot.snapshot_id,
    runId: snapshot.run_id,
    repoRoot: snapshot.repo_root,
    branch: snapshot.branch ?? null,
    headCommit: snapshot.head_commit ?? null,
    dirtyPathCount: snapshot.dirty_path_count ?? null,
    snapshotJson: snapshot.snapshot_json ?? null,
    capturedAt: snapshot.captured_at.toISOString(),
  };
}

function serializeMemoryLink(link: MemoryLink): SerializedMemoryLink {
  return {
    linkId: link.link_id,
    runId: link.run_id ?? null,
    itemId: link.item_id ?? null,
    memoryScope: link.memory_scope,
    memoryRef: link.memory_ref ?? null,
    queryText: link.query_text ?? null,
    linkJson: link.link_json ?? null,
    createdAt: link.created_at.toISOString(),
  };
}

function serializeSourceFile(sourceFile: SourceFile): SerializedSourceFile {
  return {
    sourceFileId: sourceFile.source_file_id,
    runId: sourceFile.run_id ?? null,
    fileKind: sourceFile.file_kind,
    absolutePath: sourceFile.absolute_path,
    checksum: sourceFile.checksum ?? null,
    createdAt: sourceFile.created_at.toISOString(),
  };
}

function serializeDaemonLogSource(
  source: DaemonLogSource,
): SerializedDaemonLogSource {
  return {
    sourceId: source.source_id,
    manager: source.manager,
    daemonName: source.daemon_name,
    stream: source.stream,
    hostId: source.host_id ?? null,
    logPath: source.log_path,
    configPath: source.config_path ?? null,
    repoRoot: source.repo_root ?? null,
    workspaceRoot: source.workspace_root ?? null,
    ownerAgent: source.owner_agent ?? null,
    flowDaemonName: source.flow_daemon_name ?? null,
    launchdLabel: source.launchd_label ?? null,
    retentionClass: source.retention_class,
    status: source.status,
    createdAt: source.created_at.toISOString(),
    updatedAt: source.updated_at.toISOString(),
  };
}

function serializeDaemonLogChunk(
  chunk: DaemonLogChunk,
): SerializedDaemonLogChunk {
  return {
    chunkId: chunk.chunk_id,
    sourceId: chunk.source_id,
    daemonName: chunk.daemon_name,
    stream: chunk.stream,
    hostId: chunk.host_id ?? null,
    logPath: chunk.log_path,
    fileFingerprint: chunk.file_fingerprint,
    startOffset: chunk.start_offset,
    endOffset: chunk.end_offset,
    firstLineNo: chunk.first_line_no,
    lastLineNo: chunk.last_line_no,
    lineCount: chunk.line_count,
    byteCount: chunk.byte_count,
    firstAt: serializeNullableDate(chunk.first_at),
    lastAt: serializeNullableDate(chunk.last_at),
    sha256: chunk.sha256,
    bodyRef: chunk.body_ref ?? null,
    bodyPreview: chunk.body_preview ?? null,
    compression: chunk.compression,
    ingestedAt: chunk.ingested_at.toISOString(),
  };
}

function serializeDaemonLogEvent(
  event: DaemonLogEvent,
): SerializedDaemonLogEvent {
  return {
    eventId: event.event_id,
    sourceId: event.source_id,
    chunkId: event.chunk_id,
    daemonName: event.daemon_name,
    stream: event.stream,
    seq: event.seq,
    lineNo: event.line_no,
    at: serializeNullableDate(event.at),
    level: event.level,
    message: event.message,
    fieldsJson: event.fields_json ?? null,
    repoRoot: event.repo_root ?? null,
    workspaceRoot: event.workspace_root ?? null,
    conversation: event.conversation ?? null,
    conversationHash: event.conversation_hash ?? null,
    runId: event.run_id ?? null,
    jobId: event.job_id ?? null,
    traceId: event.trace_id ?? null,
    spanId: event.span_id ?? null,
    errorKind: event.error_kind ?? null,
    createdAt: event.created_at.toISOString(),
  };
}

function serializeDaemonLogCheckpoint(
  checkpoint: DaemonLogCheckpoint,
): SerializedDaemonLogCheckpoint {
  return {
    checkpointId: checkpoint.checkpoint_id,
    sourceId: checkpoint.source_id,
    hostId: checkpoint.host_id ?? null,
    logPath: checkpoint.log_path,
    fileFingerprint: checkpoint.file_fingerprint,
    inode: checkpoint.inode ?? null,
    device: checkpoint.device ?? null,
    offset: checkpoint.offset,
    lineNo: checkpoint.line_no,
    lastChunkId: checkpoint.last_chunk_id ?? null,
    lastEventId: checkpoint.last_event_id ?? null,
    lastSeenAt: serializeNullableDate(checkpoint.last_seen_at),
    updatedAt: checkpoint.updated_at.toISOString(),
  };
}

function serializeDaemonLogSummary(
  summary: DaemonLogSummary,
): SerializedDaemonLogSummary {
  return {
    summaryId: summary.summary_id,
    sourceId: summary.source_id,
    daemonName: summary.daemon_name,
    windowStart: summary.window_start.toISOString(),
    windowEnd: summary.window_end.toISOString(),
    levelCountsJson: summary.level_counts_json ?? null,
    errorCount: summary.error_count,
    warningCount: summary.warning_count,
    firstErrorEventId: summary.first_error_event_id ?? null,
    lastErrorEventId: summary.last_error_event_id ?? null,
    topErrorKindsJson: summary.top_error_kinds_json ?? null,
    summaryText: summary.summary_text ?? null,
    createdAt: summary.created_at.toISOString(),
  };
}

function serializeAgentStateSnapshot(
  snapshot: AgentStateSnapshot,
): SerializedAgentStateSnapshot {
  return {
    snapshotId: snapshot.snapshot_id,
    agentId: snapshot.agent_id,
    stateVersion: snapshot.state_version ?? null,
    status: snapshot.status ?? null,
    stateJson: snapshot.state_json ?? null,
    capturedAt: snapshot.captured_at.toISOString(),
  };
}

function serializeCursorReviewOperationResult(
  result: CursorReviewOperationResultRecord,
): SerializedCursorReviewOperationResult {
  return {
    eventId: result.eventId,
    operationId: result.operationId,
    status: result.status,
    clientId: result.clientId ?? null,
    repoRoot: result.repoRoot ?? null,
    message: result.message ?? null,
    processedAt: result.processedAt.toISOString(),
  };
}

function serializeCursorReviewOperation(
  operation: CursorReviewOperationRecord,
): SerializedCursorReviewOperation {
  return {
    eventId: operation.eventId,
    operationId: operation.operationId,
    operationType: operation.operationType,
    repoRoot: operation.repoRoot ?? null,
    workspaceRoot: operation.workspaceRoot ?? null,
    bookmark: operation.bookmark ?? null,
    relPath: operation.relPath ?? null,
    note: operation.note ?? null,
    sourceSessionId: operation.sourceSessionId ?? null,
    sourceChatKind: operation.sourceChatKind ?? null,
    createdAt: operation.createdAt.toISOString(),
    latestResult: operation.latestResult
      ? serializeCursorReviewOperationResult(operation.latestResult)
      : null,
  };
}

function serializeBranchFileReviewState(
  state: BranchFileReviewStateRecord,
): SerializedBranchFileReviewState {
  return {
    eventId: state.eventId,
    repoRoot: state.repoRoot,
    workspaceRoot: state.workspaceRoot ?? null,
    bookmark: state.bookmark,
    relPath: state.relPath,
    status: state.status,
    note: state.note ?? null,
    sourceSessionId: state.sourceSessionId ?? null,
    sourceChatKind: state.sourceChatKind ?? null,
    createdAt: state.createdAt.toISOString(),
  };
}

function serializeCommitTurnResult(
  result: CommitTurnResultRecord,
): SerializedCommitTurnResult {
  return {
    eventId: result.eventId,
    operationId: result.operationId,
    status: result.status,
    agentId: result.agentId ?? null,
    runId: result.runId ?? null,
    threadId: result.threadId ?? null,
    repoRoot: result.repoRoot ?? null,
    model: result.model ?? null,
    effort: result.effort ?? null,
    traceRef: result.traceRef ?? null,
    message: result.message ?? null,
    classification: result.classification ?? null,
    title: result.title ?? null,
    description: result.description ?? null,
    commitMessage: result.commitMessage ?? null,
    todoItems: result.todoItems ?? null,
    notes: result.notes ?? null,
    group: result.group ?? null,
    groupReason: result.groupReason ?? null,
    groupIsNew: result.groupIsNew ?? false,
    snapshotCommitId: result.snapshotCommitId ?? null,
    reviewJobId: result.reviewJobId ?? null,
    conversationHash: result.conversationHash ?? null,
    processedAt: result.processedAt.toISOString(),
  };
}

function serializeCommitTurnOperation(
  operation: CommitTurnOperationRecord,
): SerializedCommitTurnOperation {
  return {
    eventId: operation.eventId,
    operationId: operation.operationId,
    provider: operation.provider,
    sessionId: operation.sessionId,
    conversation: operation.conversation,
    conversationHash: operation.conversationHash,
    trigger: operation.trigger,
    turnOrdinal: operation.turnOrdinal,
    sessionEventId: operation.sessionEventId,
    repoRoot: operation.repoRoot ?? null,
    repoRoots: operation.repoRoots ?? null,
    cwd: operation.cwd ?? null,
    artifactPath: operation.artifactPath ?? null,
    promptPreview: operation.promptPreview ?? null,
    sourceChatKind: operation.sourceChatKind ?? null,
    createdAt: operation.createdAt.toISOString(),
    latestResult: operation.latestResult
      ? serializeCommitTurnResult(operation.latestResult)
      : null,
  };
}

function serializeAgentClaim(claim: AgentClaimRecord): SerializedAgentClaim {
  return {
    eventId: claim.eventId,
    claimId: claim.claimId,
    scope: claim.scope,
    owner: claim.owner,
    ownerSession: claim.ownerSession ?? null,
    mode: claim.mode ?? null,
    note: claim.note ?? null,
    repoRoot: claim.repoRoot ?? null,
    workspaceRoot: claim.workspaceRoot ?? null,
    startedAt: claim.startedAt.toISOString(),
    expiresAt: claim.expiresAt.toISOString(),
    heartbeatAt: claim.heartbeatAt.toISOString(),
    releasedAt: serializeNullableDate(claim.releasedAt),
    status: claim.status,
  };
}

function serializeContextDigest(
  digest: ContextDigestRecord,
): SerializedContextDigest {
  return {
    eventId: digest.eventId,
    digestId: digest.digestId,
    targetProvider: digest.targetProvider,
    targetSession: digest.targetSession,
    targetTurnOrdinal: digest.targetTurnOrdinal,
    targetConversation: digest.targetConversation,
    targetConversationHash: digest.targetConversationHash,
    sourceSession: digest.sourceSession,
    sourceWatermarkKind: digest.sourceWatermarkKind,
    sourceWatermarkValue: digest.sourceWatermarkValue,
    sourceConversationHash: digest.sourceConversationHash ?? null,
    kind: digest.kind,
    digestText: digest.digestText,
    modelUsed: digest.modelUsed ?? null,
    score: digest.score ?? null,
    confidence: digest.confidence ?? null,
    reason: digest.reason ?? null,
    generatedAt: digest.generatedAt.toISOString(),
    expiresAt: serializeNullableDate(digest.expiresAt),
    status: digest.status,
  };
}

function serializeJob(job: JobRecord): SerializedJob {
  return {
    eventId: job.eventId,
    jobId: job.jobId,
    kind: job.kind,
    status: job.status,
    createdAt: job.createdAt.toISOString(),
    updatedAt: job.updatedAt.toISOString(),
    claimedBy: job.claimedBy ?? null,
    leaseExpiresAt: serializeNullableDate(job.leaseExpiresAt),
    attempt: job.attempt,
    payloadJson: job.payloadJson ?? null,
    resultJson: job.resultJson ?? null,
    repoRoot: job.repoRoot ?? null,
    workspaceRoot: job.workspaceRoot ?? null,
    sourceChatKind: job.sourceChatKind ?? null,
    dedupeKey: job.dedupeKey ?? null,
    targetSession: job.targetSession ?? null,
    targetTurnWatermark: job.targetTurnWatermark ?? null,
    sourceSession: job.sourceSession ?? null,
    sourceWatermark: job.sourceWatermark ?? null,
    note: job.note ?? null,
  };
}

function serializeJobEvent(event: JobEventRecord): SerializedJobEvent {
  return {
    eventId: event.eventId,
    jobId: event.jobId,
    eventType: event.eventType,
    status: event.status ?? null,
    claimedBy: event.claimedBy ?? null,
    leaseExpiresAt: serializeNullableDate(event.leaseExpiresAt),
    attempt: event.attempt ?? null,
    note: event.note ?? null,
    payloadJson: event.payloadJson ?? null,
    resultJson: event.resultJson ?? null,
    occurredAt: event.occurredAt.toISOString(),
  };
}

function serializeRunSummary(
  summary: AgentRunSummary,
): SerializedAgentRunSummary {
  return {
    run: serializeAgentRun(summary.run),
    items: summary.items.map(serializeRunItem),
    semanticEvents: summary.semanticEvents.map(serializeSemanticEvent),
    wireEvents: summary.wireEvents.map(serializeWireEvent),
    artifacts: summary.artifacts.map(serializeArtifact),
    workspaceSnapshots: summary.workspaceSnapshots.map(
      serializeWorkspaceSnapshot,
    ),
    memoryLinks: summary.memoryLinks.map(serializeMemoryLink),
    sourceFiles: summary.sourceFiles.map(serializeSourceFile),
    latestAgentState: summary.latestAgentState
      ? serializeAgentStateSnapshot(summary.latestAgentState)
      : null,
  };
}

function renderJson(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

interface ServeJsonRequest {
  command?: string;
  args?: string[];
  input?: unknown;
}

async function runServeJsonCommand(
  request: ServeJsonRequest,
  dataPath: string,
): Promise<unknown> {
  const command = request.command?.trim();
  if (!command || command === "serve-json") {
    throw new Error("serve-json request requires a non-recursive command");
  }
  const args = Array.isArray(request.args) ? request.args.map(String) : [];
  const child = spawn(
    process.execPath,
    [path.resolve(process.argv[1]), command, ...args, "--data-path", dataPath],
    {
      cwd: process.cwd(),
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
  if (Object.prototype.hasOwnProperty.call(request, "input")) {
    child.stdin.end(JSON.stringify(request.input));
  } else {
    child.stdin.end();
  }
  const exitCode = await new Promise<number | null>((resolve, reject) => {
    child.on("error", reject);
    child.on("close", resolve);
  });
  const stdoutText = Buffer.concat(stdout).toString("utf8");
  const stderrText = Buffer.concat(stderr).toString("utf8").trim();
  if (exitCode !== 0) {
    throw new Error(stderrText || stdoutText.trim() || `command exited ${exitCode}`);
  }
  return stdoutText.trim() ? JSON.parse(stdoutText) : null;
}

async function serveJson(dataPath: string): Promise<void> {
  const lines = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });
  for await (const line of lines) {
    if (!line.trim()) {
      continue;
    }
    try {
      const request = JSON.parse(line) as ServeJsonRequest;
      const result = await runServeJsonCommand(request, dataPath);
      process.stdout.write(`${JSON.stringify({ ok: true, result })}\n`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      process.stdout.write(`${JSON.stringify({ ok: false, error: message })}\n`);
    }
  }
}

async function main(): Promise<void> {
  const command = requireCommand();
  const dataPath = await resolvePersistentDataPath(
    readFlag("--data-path") ?? "~/.jazz2/agent-infra.db",
  );
  const tier = readWriteTierFlag();
  await mkdir(path.dirname(dataPath), { recursive: true });

  if (command === "serve-json") {
    await serveJson(dataPath);
    return;
  }

  const store = createAgentDataStore({
    appId: "run-agent-infra",
    dataPath,
    ...(tier ? { tier } : {}),
  });

  try {
    switch (command) {
      case "sync-do-designer": {
        const result = await syncDoDesignerTasks({
          store,
          tasksRoot: readFlag("--tasks-root"),
          nowPath: readFlag("--now-path"),
          nextPath: readFlag("--next-path"),
          context: readFlag("--context") ?? "designer",
        });
        renderJson({
          dataPath,
          syncedCount: result.syncedCount,
          taskIds: result.taskIds,
          records: result.records.map(serializeTaskRecord),
        });
        break;
      }
      case "project-do-designer": {
        const result = await projectDoDesignerTasks({
          store,
          tasksRoot: readFlag("--tasks-root"),
          nowPath: readFlag("--now-path"),
          nextPath: readFlag("--next-path"),
          designerPath: readFlag("--designer-path"),
          context: readFlag("--context") ?? "designer",
        });
        renderJson({
          dataPath,
          projectedCount: result.projectedCount,
          taskIds: result.taskIds,
          records: result.records.map(serializeTaskRecord),
        });
        break;
      }
      case "upsert-task": {
        const input = readJsonInput<UpsertTaskRecordInput>("upsert-task");
        const record = await store.upsertTaskRecord(input);
        if (hasFlag("--project-do-designer")) {
          await projectDoDesignerTasks({
            store,
            tasksRoot: readFlag("--tasks-root"),
            nowPath: readFlag("--now-path"),
            nextPath: readFlag("--next-path"),
            designerPath: readFlag("--designer-path"),
            context: readFlag("--context") ?? input.context,
          });
        }
        renderJson(serializeTaskRecord(record));
        break;
      }
      case "list-tasks": {
        const limitRaw = readFlag("--limit");
        const query: ListTaskRecordsInput = {
          context: readFlag("--context"),
          statuses: parseCsvFlag("--status"),
          priorities: parseCsvFlag("--priority"),
          placements: parseCsvFlag("--placement"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const records = await store.listTaskRecords(query);
        renderJson(records.map(serializeTaskRecord));
        break;
      }
      case "get-task": {
        const taskId = process.argv[3];
        if (!taskId) {
          throw new Error("get-task requires <task-id>");
        }
        const record = await store.getTaskRecord(taskId);
        if (!record) {
          throw new Error(`task ${taskId} not found`);
        }
        renderJson(serializeTaskRecord(record));
        break;
      }
      case "record-run-started": {
        const input =
          readJsonInput<RecordRunStartedInput>("record-run-started");
        const run = await store.recordRunStarted(input);
        renderJson(serializeAgentRun(run));
        break;
      }
      case "record-run-completed": {
        const input = readJsonInput<RecordRunCompletedInput>(
          "record-run-completed",
        );
        const run = await store.recordRunCompleted(input);
        renderJson(serializeAgentRun(run));
        break;
      }
      case "record-item-started": {
        const input = readJsonInput<RecordItemStartedInput>(
          "record-item-started",
        );
        const item = await store.recordItemStarted(input);
        renderJson(serializeRunItem(item));
        break;
      }
      case "record-item-completed": {
        const input = readJsonInput<RecordItemCompletedInput>(
          "record-item-completed",
        );
        const item = await store.recordItemCompleted(input);
        renderJson(serializeRunItem(item));
        break;
      }
      case "record-artifact": {
        const input = readJsonInput<RecordArtifactInput>("record-artifact");
        const artifact = await store.recordArtifact(input);
        renderJson(serializeArtifact(artifact));
        break;
      }
      case "record-workspace-snapshot": {
        const input = readJsonInput<RecordWorkspaceSnapshotInput>(
          "record-workspace-snapshot",
        );
        const snapshot = await store.recordWorkspaceSnapshot(input);
        renderJson(serializeWorkspaceSnapshot(snapshot));
        break;
      }
      case "record-daemon-log-source": {
        const input = readJsonInput<RecordDaemonLogSourceInput>(
          "record-daemon-log-source",
        );
        const source = await store.recordDaemonLogSource(input);
        renderJson(serializeDaemonLogSource(source));
        break;
      }
      case "list-daemon-log-sources": {
        const limitRaw = readFlag("--limit");
        const query: ListDaemonLogSourcesInput = {
          manager: readFlag("--manager"),
          daemonName: readFlag("--daemon"),
          stream: readFlag("--stream"),
          status: readFlag("--status"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const sources = await store.listDaemonLogSources(query);
        renderJson(sources.map(serializeDaemonLogSource));
        break;
      }
      case "record-daemon-log-chunk": {
        const input = readJsonInput<RecordDaemonLogChunkInput>(
          "record-daemon-log-chunk",
        );
        const chunk = await store.recordDaemonLogChunk(input);
        renderJson(serializeDaemonLogChunk(chunk));
        break;
      }
      case "get-daemon-log-chunk": {
        const chunkId = readFlag("--chunk-id");
        if (!chunkId) {
          throw new Error("get-daemon-log-chunk requires --chunk-id");
        }
        const chunk = await store.getDaemonLogChunk(chunkId);
        if (!chunk) {
          throw new Error(`daemon log chunk ${chunkId} not found`);
        }
        renderJson(serializeDaemonLogChunk(chunk));
        break;
      }
      case "record-daemon-log-event": {
        const input = readJsonInput<RecordDaemonLogEventInput>(
          "record-daemon-log-event",
        );
        const event = await store.recordDaemonLogEvent(input);
        renderJson(serializeDaemonLogEvent(event));
        break;
      }
      case "record-daemon-log-batch": {
        const input = readJsonInput<RecordDaemonLogBatchInput>(
          "record-daemon-log-batch",
        );
        const source = input.source
          ? await store.recordDaemonLogSource(input.source)
          : null;
        const chunk = await store.recordDaemonLogChunk(input.chunk);
        const events: DaemonLogEvent[] = [];
        for (const eventInput of input.events) {
          events.push(await store.recordDaemonLogEvent(eventInput));
        }
        renderJson({
          source: source ? serializeDaemonLogSource(source) : null,
          chunk: serializeDaemonLogChunk(chunk),
          events: events.map(serializeDaemonLogEvent),
        });
        break;
      }
      case "list-daemon-log-events": {
        const limitRaw = readFlag("--limit");
        const query: ListDaemonLogEventsInput = {
          sourceId: readFlag("--source-id"),
          daemonName: readFlag("--daemon"),
          level: readFlag("--level"),
          conversation: readFlag("--conversation"),
          conversationHash: readFlag("--conversation-hash"),
          runId: readFlag("--run-id"),
          jobId: readFlag("--job-id"),
          traceId: readFlag("--trace-id"),
          since: readFlag("--since"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const events = await store.listDaemonLogEvents(query);
        renderJson(events.map(serializeDaemonLogEvent));
        break;
      }
      case "record-daemon-log-checkpoint": {
        const input = readJsonInput<RecordDaemonLogCheckpointInput>(
          "record-daemon-log-checkpoint",
        );
        const checkpoint = await store.recordDaemonLogCheckpoint(input);
        renderJson(serializeDaemonLogCheckpoint(checkpoint));
        break;
      }
      case "record-daemon-log-summary": {
        const input = readJsonInput<RecordDaemonLogSummaryInput>(
          "record-daemon-log-summary",
        );
        const summary = await store.recordDaemonLogSummary(input);
        renderJson(serializeDaemonLogSummary(summary));
        break;
      }
      case "list-daemon-log-summaries": {
        const limitRaw = readFlag("--limit");
        const query: ListDaemonLogSummariesInput = {
          sourceId: readFlag("--source-id"),
          daemonName: readFlag("--daemon"),
          since: readFlag("--since"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const summaries = await store.listDaemonLogSummaries(query);
        renderJson(summaries.map(serializeDaemonLogSummary));
        break;
      }
      case "list-recent-runs": {
        const limitRaw = readFlag("--limit");
        const limit = limitRaw ? Number.parseInt(limitRaw, 10) : undefined;
        const runs = await store.listRecentRuns(limit);
        renderJson(runs.map(serializeAgentRun));
        break;
      }
      case "list-active-runs": {
        const limitRaw = readFlag("--limit");
        const limit = limitRaw ? Number.parseInt(limitRaw, 10) : undefined;
        const runs = await store.listActiveRuns(limit);
        renderJson(runs.map(serializeAgentRun));
        break;
      }
      case "get-run-summary": {
        const runId = readFlag("--run-id");
        if (!runId) {
          throw new Error("get-run-summary requires --run-id");
        }
        const summary = await store.getRunSummary(runId);
        if (!summary) {
          throw new Error(`run ${runId} not found`);
        }
        renderJson(serializeRunSummary(summary));
        break;
      }
      case "record-cursor-review-op": {
        const input = readJsonInput<RecordCursorReviewOperationInput>(
          "record-cursor-review-op",
        );
        const operation = await store.recordCursorReviewOperation(input);
        renderJson(serializeCursorReviewOperation(operation));
        break;
      }
      case "record-cursor-review-result": {
        const input = readJsonInput<RecordCursorReviewResultInput>(
          "record-cursor-review-result",
        );
        const result = await store.recordCursorReviewResult(input);
        renderJson(serializeCursorReviewOperationResult(result));
        break;
      }
      case "record-branch-file-review-state": {
        const input = readJsonInput<RecordBranchFileReviewStateInput>(
          "record-branch-file-review-state",
        );
        const state = await store.recordBranchFileReviewState(input);
        renderJson(serializeBranchFileReviewState(state));
        break;
      }
      case "record-commit-turn-op": {
        const input = readJsonInput<RecordCommitTurnOperationInput>(
          "record-commit-turn-op",
        );
        const operation = await store.recordCommitTurnOperation(input);
        renderJson(serializeCommitTurnOperation(operation));
        break;
      }
      case "record-commit-turn-result": {
        const input = readJsonInput<RecordCommitTurnResultInput>(
          "record-commit-turn-result",
        );
        const result = await store.recordCommitTurnResult(input);
        renderJson(serializeCommitTurnResult(result));
        break;
      }
      case "record-agent-claim": {
        const input =
          readJsonInput<RecordAgentClaimInput>("record-agent-claim");
        const claim = await store.recordAgentClaim(input);
        renderJson(serializeAgentClaim(claim));
        break;
      }
      case "renew-agent-claim": {
        const input = readJsonInput<RenewAgentClaimInput>("renew-agent-claim");
        const claim = await store.renewAgentClaim(input);
        renderJson(serializeAgentClaim(claim));
        break;
      }
      case "release-agent-claim": {
        const input = readJsonInput<ReleaseAgentClaimInput>(
          "release-agent-claim",
        );
        const claim = await store.releaseAgentClaim(input);
        renderJson(serializeAgentClaim(claim));
        break;
      }
      case "list-agent-claims": {
        const limitRaw = readFlag("--limit");
        const query: ListAgentClaimsInput = {
          scopePrefix: readFlag("--scope-prefix"),
          ownerSession: readFlag("--owner-session"),
          includeReleased: hasFlag("--include-released"),
          includeExpired: hasFlag("--include-expired"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const claims = await store.listAgentClaims(query);
        renderJson(claims.map(serializeAgentClaim));
        break;
      }
      case "record-context-digest": {
        const input = readJsonInput<RecordContextDigestInput>(
          "record-context-digest",
        );
        const digest = await store.recordContextDigest(input);
        renderJson(serializeContextDigest(digest));
        break;
      }
      case "list-context-digests": {
        const limitRaw = readFlag("--limit");
        const turnOrdinalRaw = readFlag("--target-turn-ordinal");
        const query: ListContextDigestsInput = {
          targetSession: readFlag("--target-session"),
          targetConversation: readFlag("--target-conversation"),
          targetConversationHash: readFlag("--target-conversation-hash"),
          targetTurnOrdinal: turnOrdinalRaw
            ? Number.parseInt(turnOrdinalRaw, 10)
            : undefined,
          sourceSession: readFlag("--source-session"),
          kind: readFlag("--kind"),
          includeExpired: hasFlag("--include-expired"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const digests = await store.listContextDigests(query);
        renderJson(digests.map(serializeContextDigest));
        break;
      }
      case "record-job": {
        const input = readJsonInput<RecordJobInput>("record-job");
        const job = await store.recordJob(input);
        renderJson(serializeJob(job));
        break;
      }
      case "claim-job": {
        const input = readJsonInput<ClaimJobInput>("claim-job");
        const job = await store.claimJob(input);
        renderJson(serializeJob(job));
        break;
      }
      case "update-job": {
        const input = readJsonInput<UpdateJobInput>("update-job");
        const job = await store.updateJob(input);
        renderJson(serializeJob(job));
        break;
      }
      case "cancel-job": {
        const input = readJsonInput<CancelJobInput>("cancel-job");
        const job = await store.cancelJob(input);
        renderJson(serializeJob(job));
        break;
      }
      case "get-job": {
        const jobId = readFlag("--job-id");
        if (!jobId) {
          throw new Error("get-job requires --job-id");
        }
        const job = await store.getJob(jobId);
        if (!job) {
          throw new Error(`job ${jobId} not found`);
        }
        renderJson(serializeJob(job));
        break;
      }
      case "list-jobs": {
        const limitRaw = readFlag("--limit");
        const query: ListJobsInput = {
          kind: readFlag("--kind"),
          status: readFlag("--status") as ListJobsInput["status"],
          claimedBy: readFlag("--claimed-by"),
          repoRoot: readFlag("--repo-root"),
          targetSession: readFlag("--target-session"),
          includeFinished: hasFlag("--include-finished"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const jobs = await store.listJobs(query);
        renderJson(jobs.map(serializeJob));
        break;
      }
      case "list-job-events": {
        const jobId = readFlag("--job-id");
        if (!jobId) {
          throw new Error("list-job-events requires --job-id");
        }
        const limitRaw = readFlag("--limit");
        const events = await store.listJobEvents(
          jobId,
          limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        );
        renderJson(events.map(serializeJobEvent));
        break;
      }
      case "list-commit-turn-ops": {
        const limitRaw = readFlag("--limit");
        const query: ListCommitTurnOperationsInput = {
          repoRoot: readFlag("--repo-root"),
          conversationHash: readFlag("--conversation-hash"),
          includeProcessed: hasFlag("--include-processed"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const operations = await store.listCommitTurnOperations(query);
        renderJson(operations.map(serializeCommitTurnOperation));
        break;
      }
      case "list-cursor-review-ops": {
        const limitRaw = readFlag("--limit");
        const query: ListCursorReviewOperationsInput = {
          repoRoot: readFlag("--repo-root"),
          workspaceRoot: readFlag("--workspace-root"),
          includeProcessed: hasFlag("--include-processed"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const operations = await store.listCursorReviewOperations(query);
        renderJson(operations.map(serializeCursorReviewOperation));
        break;
      }
      case "list-branch-file-review-states": {
        const limitRaw = readFlag("--limit");
        const query: ListBranchFileReviewStatesInput = {
          repoRoot: readFlag("--repo-root"),
          workspaceRoot: readFlag("--workspace-root"),
          bookmark: readFlag("--bookmark"),
          relPath: readFlag("--rel-path"),
          includeCleared: hasFlag("--include-cleared"),
          limit: limitRaw ? Number.parseInt(limitRaw, 10) : 20,
        };
        const states = await store.listBranchFileReviewStates(query);
        renderJson(states.map(serializeBranchFileReviewState));
        break;
      }
      default:
        throw new Error(`unknown command ${command}`);
    }
  } finally {
    await store.shutdown();
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
