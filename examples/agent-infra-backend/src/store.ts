import { randomUUID } from "node:crypto";
import {
  createJazzContext,
  type Db,
  type JazzContext,
  type QueryBuilder,
  type Session,
  type TableProxy,
} from "jazz-tools/backend";
import {
  app,
  type Agent,
  type AgentInit,
  type AgentRun,
  type AgentRunInit,
  type AgentStateSnapshot,
  type AgentStateSnapshotInit,
  type Artifact,
  type ArtifactInit,
  type DaemonLogCheckpoint,
  type DaemonLogCheckpointInit,
  type DaemonLogChunk,
  type DaemonLogChunkInit,
  type DaemonLogEvent,
  type DaemonLogEventInit,
  type DaemonLogSource,
  type DaemonLogSourceInit,
  type DaemonLogSummary,
  type DaemonLogSummaryInit,
  type DesignerAgent,
  type DesignerAgentContext,
  type DesignerAgentTool,
  type DesignerCadDocument,
  type DesignerCadEvent,
  type DesignerCadOperation,
  type DesignerCadPreviewHandle,
  type DesignerCadPreviewUpdate,
  type DesignerCadSceneNode,
  type DesignerCadSelection,
  type DesignerCadSession,
  type DesignerCadSourceEdit,
  type DesignerCadSteer,
  type DesignerCadToolSession,
  type DesignerCadWidget,
  type DesignerCadWorkspace,
  type DesignerCodexConversation,
  type DesignerCodexTurn,
  type DesignerLiveCommit,
  type DesignerObjectRef,
  type DesignerTelemetryEvent,
  type JsonValue,
  type MemoryLink,
  type MemoryLinkInit,
  type RunItem,
  type RunItemInit,
  type SemanticEvent,
  type SemanticEventInit,
  type SourceFile,
  type SourceFileInit,
  type TaskRecord,
  type WireEvent,
  type WireEventInit,
  type WorkspaceSnapshot,
  type WorkspaceSnapshotInit,
} from "../schema/app.js";

const DEFAULT_APP_ID = "run-agent-infra";
const DEFAULT_STATUS = "started";
const CURSOR_REVIEW_AGENT_ID = "cursor-review";
const CURSOR_REVIEW_CONTROL_RUN_ID = "cursor-review-control";
const CURSOR_REVIEW_OPERATION_EVENT_TYPE = "cursor_review_operation";
const CURSOR_REVIEW_RESULT_EVENT_TYPE = "cursor_review_result";
const BRANCH_FILE_REVIEW_AGENT_ID = "branch-file-review";
const BRANCH_FILE_REVIEW_CONTROL_RUN_ID = "branch-file-review-control";
const BRANCH_FILE_REVIEW_STATE_EVENT_TYPE = "branch_file_review_state";
const COMMIT_TURN_AGENT_ID = "commit";
const COMMIT_TURN_CONTROL_RUN_ID = "commit-turn-control";
const COMMIT_TURN_OPERATION_EVENT_TYPE = "commit_turn_operation";
const COMMIT_TURN_RESULT_EVENT_TYPE = "commit_turn_result";
const AGENT_CLAIM_AGENT_ID = "ops-claim";
const AGENT_CLAIM_CONTROL_RUN_ID = "ops-claim-control";
const AGENT_CLAIM_STATE_EVENT_TYPE = "agent_claim_state";
const JOB_AGENT_ID = "job";
const JOB_CONTROL_RUN_ID = "job-control";
const JOB_STATE_EVENT_TYPE = "job_state";
const JOB_EVENT_EVENT_TYPE = "job_event";
const CONTEXT_DIGEST_AGENT_ID = "context-distill";
const CONTEXT_DIGEST_CONTROL_RUN_ID = "context-digest-control";
const CONTEXT_DIGEST_EVENT_TYPE = "context_digest";
const TERMINAL_RUN_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "error",
]);
const TERMINAL_CURSOR_REVIEW_RESULT_STATUSES = new Set([
  "completed",
  "failed",
  "ignored",
]);
const TERMINAL_COMMIT_TURN_RESULT_STATUSES = new Set([
  "completed",
  "failed",
  "ignored",
]);
const TERMINAL_JOB_STATUSES = new Set([
  "completed",
  "failed",
  "cancelled",
  "cancelled-superseded",
]);
const TASK_PLACEMENT_ORDER: Record<string, number> = {
  now: 0,
  next: 1,
  backlog: 2,
};
const TASK_PRIORITY_ORDER: Record<string, number> = {
  P0: 0,
  P1: 1,
  P2: 2,
  P3: 3,
};

type DurabilityTier = "local" | "edge" | "global";
type TimestampInput = Date | string | number;
export type TaskPlacement = "now" | "next" | "backlog" | string;

export interface AgentDataStoreConfig {
  dataPath: string;
  appId?: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  serverPathPrefix?: string;
  backendSecret?: string;
  adminSecret?: string;
  tier?: DurabilityTier;
}

export interface UpsertAgentInput {
  agentId: string;
  lane?: string;
  specPath?: string;
  promptSurface?: string;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordRunStartedInput {
  runId: string;
  agentId: string;
  threadId?: string;
  turnId?: string;
  cwd?: string;
  repoRoot?: string;
  requestSummary?: string;
  status?: string;
  startedAt?: TimestampInput;
  contextJson?: JsonValue;
  sourceTracePath?: string;
  agent?: Omit<UpsertAgentInput, "agentId">;
}

export interface RecordRunCompletedInput {
  runId: string;
  status?: string;
  endedAt?: TimestampInput;
}

export interface RecordItemStartedInput {
  runId: string;
  itemId: string;
  itemKind: string;
  sequence: number;
  phase?: string;
  status?: string;
  summaryJson?: JsonValue;
  startedAt?: TimestampInput;
}

export interface RecordItemCompletedInput {
  runId: string;
  itemId: string;
  status?: string;
  summaryJson?: JsonValue;
  completedAt?: TimestampInput;
}

export interface AppendSemanticEventInput {
  runId: string;
  eventType: string;
  eventId?: string;
  itemId?: string;
  summaryText?: string;
  payloadJson?: JsonValue;
  occurredAt?: TimestampInput;
}

export interface AppendWireEventInput {
  direction: string;
  eventId?: string;
  runId?: string;
  connectionId?: number;
  sessionId?: number;
  method?: string;
  requestId?: string;
  payloadJson?: JsonValue;
  occurredAt?: TimestampInput;
}

export interface RecordArtifactInput {
  runId: string;
  artifactKind: string;
  absolutePath: string;
  artifactId?: string;
  title?: string;
  checksum?: string;
  createdAt?: TimestampInput;
}

export interface RecordWorkspaceSnapshotInput {
  runId: string;
  repoRoot: string;
  snapshotId?: string;
  branch?: string;
  headCommit?: string;
  dirtyPathCount?: number;
  snapshotJson?: JsonValue;
  capturedAt?: TimestampInput;
}

export interface UpdateAgentStateInput {
  agentId: string;
  stateJson: JsonValue;
  snapshotId?: string;
  stateVersion?: number;
  status?: string;
  capturedAt?: TimestampInput;
}

export interface RecordMemoryLinkInput {
  memoryScope: string;
  linkId?: string;
  runId?: string;
  itemId?: string;
  memoryRef?: string;
  queryText?: string;
  linkJson?: JsonValue;
  createdAt?: TimestampInput;
}

export interface RecordSourceFileInput {
  fileKind: string;
  absolutePath: string;
  sourceFileId?: string;
  runId?: string;
  checksum?: string;
  createdAt?: TimestampInput;
}

export interface RecordDesignerObjectRefInput {
  objectRefId?: string;
  provider: string;
  uri: string;
  bucket?: string;
  key?: string;
  region?: string;
  digestSha256?: string;
  byteSize?: number;
  contentType?: string;
  objectKind: string;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerCodexConversationInput {
  conversationId?: string;
  provider: string;
  providerSessionId: string;
  threadId?: string;
  workspaceId?: string;
  workspaceKey?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  branch?: string;
  model?: string;
  status?: string;
  transcriptObjectRefId: string;
  latestEventSequence?: number;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
  endedAt?: TimestampInput;
}

export interface RecordDesignerCodexTurnInput {
  turnId?: string;
  conversationId: string;
  sequence: number;
  turnKind: string;
  role: string;
  actorKind: string;
  actorId?: string;
  summaryText?: string;
  payloadObjectRefId: string;
  promptObjectRefId?: string;
  responseObjectRefId?: string;
  tokenCountsJson?: JsonValue;
  status?: string;
  startedAt?: TimestampInput;
  completedAt?: TimestampInput;
}

export interface RecordDesignerTelemetryEventInput {
  telemetryEventId?: string;
  sessionId?: string;
  workspaceId?: string;
  conversationId?: string;
  eventType: string;
  pane?: string;
  sequence?: number;
  summaryText?: string;
  payloadObjectRefId: string;
  propertiesJson?: JsonValue;
  occurredAt?: TimestampInput;
  ingestedAt?: TimestampInput;
}

export interface RecordDesignerAgentInput {
  agentId: string;
  agentKind: string;
  provider: string;
  displayName: string;
  model?: string;
  defaultContextJson?: JsonValue;
  toolContractJson?: JsonValue;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerAgentToolInput {
  toolId?: string;
  agentId: string;
  toolName: string;
  toolKind: string;
  inputSchemaJson?: JsonValue;
  outputSchemaJson?: JsonValue;
  scopeJson?: JsonValue;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerAgentContextInput {
  contextId?: string;
  agentId: string;
  contextKind: string;
  sourceKind: string;
  objectRefId?: string;
  inlineContextJson?: JsonValue;
  priority?: number;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerLiveCommitInput {
  commitId: string;
  repoRoot: string;
  workspaceRoot?: string;
  branch: string;
  bookmark?: string;
  liveRef?: string;
  treeId?: string;
  parentCommitIdsJson?: JsonValue;
  subject: string;
  body?: string;
  authorName?: string;
  authorEmail?: string;
  committerName?: string;
  committerEmail?: string;
  traceRef?: string;
  sourceSessionId?: string;
  sourceTurnOrdinal?: number;
  sourceConversationId?: string;
  sourceTurnId?: string;
  agentId?: string;
  courierRunId?: string;
  liveSnapshotRef?: string;
  changedPathsJson?: JsonValue;
  patchObjectRefId?: string;
  manifestObjectRefId?: string;
  status?: string;
  committedAt?: TimestampInput;
  reflectedAt?: TimestampInput;
  ingestedAt?: TimestampInput;
}

export interface ListDesignerCodexTurnsInput {
  conversationId?: string;
  role?: string;
  status?: string;
  afterSequence?: number;
  limit?: number;
}

export interface ListDesignerTelemetryEventsInput {
  conversationId?: string;
  sessionId?: string;
  workspaceId?: string;
  eventType?: string;
  afterSequence?: number;
  limit?: number;
}

export interface ListDesignerAgentToolsInput {
  agentId?: string;
  toolKind?: string;
  status?: string;
  limit?: number;
}

export interface ListDesignerAgentContextsInput {
  agentId?: string;
  contextKind?: string;
  sourceKind?: string;
  status?: string;
  limit?: number;
}

export interface ListDesignerLiveCommitsInput {
  repoRoot?: string;
  branch?: string;
  sourceSessionId?: string;
  agentId?: string;
  status?: string;
  limit?: number;
}

export interface DesignerCodexConversationSummary {
  conversation: DesignerCodexConversation;
  transcriptObject: DesignerObjectRef;
  turns: DesignerCodexTurn[];
  telemetryEvents: DesignerTelemetryEvent[];
}

export interface DesignerLiveCommitSummary {
  commit: DesignerLiveCommit;
  agent?: DesignerAgent;
  patchObject?: DesignerObjectRef;
  manifestObject?: DesignerObjectRef;
  sourceConversation?: DesignerCodexConversation;
  sourceTurn?: DesignerCodexTurn;
}

export interface RecordDesignerCadWorkspaceInput {
  workspaceId: string;
  workspaceKey?: string;
  title?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerCadDocumentInput {
  workspaceId: string;
  documentId?: string;
  filePath: string;
  language?: string;
  sourceKind?: string;
  sourceHash?: string;
  status?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerCadSessionInput {
  cadSessionId?: string;
  workspaceId: string;
  documentId: string;
  codexSessionId?: string;
  agentRunId?: string;
  status?: string;
  activeToolSessionId?: string;
  latestProjectionId?: string;
  openedBy?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
  closedAt?: TimestampInput;
}

export interface RecordDesignerCadEventInput {
  eventId?: string;
  cadSessionId: string;
  sequence: number;
  eventKind: string;
  actorKind: string;
  actorId?: string;
  toolSessionId?: string;
  operationId?: string;
  previewId?: string;
  sourceEventId?: string;
  payloadJson?: JsonValue;
  occurredAt?: TimestampInput;
  observedAt?: TimestampInput;
}

export interface UpsertDesignerCadSceneNodeInput {
  nodeId: string;
  cadSessionId: string;
  documentId?: string;
  projectionId: string;
  kind: string;
  label?: string;
  path?: string;
  parentNodeId?: string;
  stableRef?: string;
  visibility?: string;
  sourceSpanJson?: JsonValue;
  geometryRefJson?: JsonValue;
  metadataJson?: JsonValue;
  updatedAt?: TimestampInput;
}

export interface UpsertDesignerCadSelectionInput {
  selectionId?: string;
  cadSessionId: string;
  actorKind: string;
  actorId?: string;
  targetKind: string;
  targetId: string;
  nodeId?: string;
  selectionJson?: JsonValue;
  status?: string;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerCadToolSessionInput {
  toolSessionId?: string;
  cadSessionId: string;
  toolKind: string;
  actorKind: string;
  actorId?: string;
  status?: string;
  inputJson?: JsonValue;
  stateJson?: JsonValue;
  startedAt?: TimestampInput;
  updatedAt?: TimestampInput;
  completedAt?: TimestampInput;
}

export interface RecordDesignerCadOperationInput {
  operationId?: string;
  cadSessionId: string;
  toolSessionId?: string;
  actorKind: string;
  actorId?: string;
  operationKind: string;
  status?: string;
  operationJson: JsonValue;
  validationJson?: JsonValue;
  resultJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
  appliedAt?: TimestampInput;
}

export interface RecordDesignerCadSourceEditInput {
  editId?: string;
  operationId: string;
  sequence: number;
  filePath: string;
  rangeJson: JsonValue;
  textPreview?: string;
  textSha256?: string;
  status?: string;
  createdAt?: TimestampInput;
}

export interface RecordDesignerCadPreviewHandleInput {
  previewId?: string;
  cadSessionId: string;
  toolSessionId?: string;
  operationId?: string;
  previewKind: string;
  targetJson?: JsonValue;
  status?: string;
  handleRef?: string;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
  disposedAt?: TimestampInput;
}

export interface RecordDesignerCadPreviewUpdateInput {
  updateId?: string;
  previewId: string;
  sequence: number;
  paramsJson?: JsonValue;
  meshRefJson?: JsonValue;
  status?: string;
  errorText?: string;
  requestedAt?: TimestampInput;
  completedAt?: TimestampInput;
}

export interface RecordDesignerCadWidgetInput {
  widgetId?: string;
  workspaceId: string;
  widgetKey: string;
  title?: string;
  sourceKind?: string;
  sourcePath?: string;
  version?: string;
  status?: string;
  manifestJson?: JsonValue;
  stateJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface RecordDesignerCadSteerInput {
  steerId?: string;
  cadSessionId: string;
  actorKind: string;
  actorId?: string;
  targetAgentId?: string;
  targetRunId?: string;
  messageText: string;
  contextJson?: JsonValue;
  status?: string;
  createdAt?: TimestampInput;
}

export interface ListDesignerCadEventsInput {
  cadSessionId?: string;
  eventKind?: string;
  afterSequence?: number;
  limit?: number;
}

export interface ListDesignerCadOperationsInput {
  cadSessionId?: string;
  toolSessionId?: string;
  status?: string;
  limit?: number;
}

export interface DesignerCadSessionSummary {
  workspace: DesignerCadWorkspace;
  document: DesignerCadDocument;
  session: DesignerCadSession;
  events: DesignerCadEvent[];
  sceneNodes: DesignerCadSceneNode[];
  selections: DesignerCadSelection[];
  toolSessions: DesignerCadToolSession[];
  operations: DesignerCadOperation[];
  sourceEdits: DesignerCadSourceEdit[];
  previewHandles: DesignerCadPreviewHandle[];
  previewUpdates: DesignerCadPreviewUpdate[];
  widgets: DesignerCadWidget[];
  steers: DesignerCadSteer[];
}

export type DaemonLogManager = "flow" | "launchd" | "manual" | "systemd" | string;
export type DaemonLogStream =
  | "stdout"
  | "stderr"
  | "combined"
  | "health"
  | "supervisor"
  | string;
export type DaemonLogRetentionClass = "debug" | "normal" | "audit" | string;
export type DaemonLogSourceStatus =
  | "active"
  | "paused"
  | "missing"
  | "rotated"
  | "retired"
  | string;
export type DaemonLogLevel =
  | "trace"
  | "debug"
  | "info"
  | "warn"
  | "error"
  | "fatal"
  | "unknown"
  | string;

export interface RecordDaemonLogSourceInput {
  sourceId?: string;
  manager: DaemonLogManager;
  daemonName: string;
  stream: DaemonLogStream;
  hostId?: string;
  logPath: string;
  configPath?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  ownerAgent?: string;
  flowDaemonName?: string;
  launchdLabel?: string;
  retentionClass?: DaemonLogRetentionClass;
  status?: DaemonLogSourceStatus;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface ListDaemonLogSourcesInput {
  manager?: string;
  daemonName?: string;
  stream?: string;
  status?: string;
  limit?: number;
}

export interface DaemonLogSourceRecord {
  sourceId: string;
  manager: string;
  daemonName: string;
  stream: string;
  hostId?: string;
  logPath: string;
  configPath?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  ownerAgent?: string;
  flowDaemonName?: string;
  launchdLabel?: string;
  retentionClass: string;
  status: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface RecordDaemonLogChunkInput {
  chunkId?: string;
  sourceId: string;
  daemonName?: string;
  stream?: string;
  hostId?: string;
  logPath?: string;
  fileFingerprint: string;
  startOffset: number;
  endOffset: number;
  firstLineNo: number;
  lastLineNo: number;
  lineCount: number;
  byteCount: number;
  firstAt?: TimestampInput;
  lastAt?: TimestampInput;
  sha256: string;
  bodyRef?: string;
  bodyPreview?: string;
  compression?: string;
  ingestedAt?: TimestampInput;
}

export interface DaemonLogChunkRecord {
  chunkId: string;
  sourceId: string;
  daemonName: string;
  stream: string;
  hostId?: string;
  logPath: string;
  fileFingerprint: string;
  startOffset: number;
  endOffset: number;
  firstLineNo: number;
  lastLineNo: number;
  lineCount: number;
  byteCount: number;
  firstAt?: Date;
  lastAt?: Date;
  sha256: string;
  bodyRef?: string;
  bodyPreview?: string;
  compression: string;
  ingestedAt: Date;
}

export interface RecordDaemonLogEventInput {
  eventId?: string;
  sourceId: string;
  chunkId: string;
  daemonName?: string;
  stream?: string;
  seq: number;
  lineNo: number;
  at?: TimestampInput;
  level?: DaemonLogLevel;
  message: string;
  fieldsJson?: JsonValue;
  repoRoot?: string;
  workspaceRoot?: string;
  conversation?: string;
  conversationHash?: string;
  runId?: string;
  jobId?: string;
  traceId?: string;
  spanId?: string;
  errorKind?: string;
  createdAt?: TimestampInput;
}

export interface ListDaemonLogEventsInput {
  sourceId?: string;
  daemonName?: string;
  level?: string;
  conversation?: string;
  conversationHash?: string;
  runId?: string;
  jobId?: string;
  traceId?: string;
  since?: TimestampInput;
  limit?: number;
}

export interface DaemonLogEventRecord {
  eventId: string;
  sourceId: string;
  chunkId: string;
  daemonName: string;
  stream: string;
  seq: number;
  lineNo: number;
  at?: Date;
  level: string;
  message: string;
  fieldsJson?: JsonValue;
  repoRoot?: string;
  workspaceRoot?: string;
  conversation?: string;
  conversationHash?: string;
  runId?: string;
  jobId?: string;
  traceId?: string;
  spanId?: string;
  errorKind?: string;
  createdAt: Date;
}

export interface RecordDaemonLogCheckpointInput {
  checkpointId?: string;
  sourceId: string;
  hostId?: string;
  logPath?: string;
  fileFingerprint: string;
  inode?: string;
  device?: string;
  offset: number;
  lineNo: number;
  lastChunkId?: string;
  lastEventId?: string;
  lastSeenAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface DaemonLogCheckpointRecord {
  checkpointId: string;
  sourceId: string;
  hostId?: string;
  logPath: string;
  fileFingerprint: string;
  inode?: string;
  device?: string;
  offset: number;
  lineNo: number;
  lastChunkId?: string;
  lastEventId?: string;
  lastSeenAt?: Date;
  updatedAt: Date;
}

export interface RecordDaemonLogSummaryInput {
  summaryId?: string;
  sourceId: string;
  daemonName?: string;
  windowStart: TimestampInput;
  windowEnd: TimestampInput;
  levelCountsJson: JsonValue;
  errorCount: number;
  warningCount: number;
  firstErrorEventId?: string;
  lastErrorEventId?: string;
  topErrorKindsJson?: JsonValue;
  summaryText?: string;
  createdAt?: TimestampInput;
}

export interface ListDaemonLogSummariesInput {
  sourceId?: string;
  daemonName?: string;
  since?: TimestampInput;
  limit?: number;
}

export interface DaemonLogSummaryRecord {
  summaryId: string;
  sourceId: string;
  daemonName: string;
  windowStart: Date;
  windowEnd: Date;
  levelCountsJson: JsonValue;
  errorCount: number;
  warningCount: number;
  firstErrorEventId?: string;
  lastErrorEventId?: string;
  topErrorKindsJson?: JsonValue;
  summaryText?: string;
  createdAt: Date;
}

export interface UpsertTaskRecordInput {
  taskId: string;
  context: string;
  title: string;
  status: string;
  priority: string;
  placement: TaskPlacement;
  focusRank?: number;
  project: string;
  issue?: string;
  branch?: string;
  workspace?: string;
  plan?: string;
  pr?: string;
  tagsJson?: JsonValue;
  nextText?: string;
  contextText?: string;
  notesText?: string;
  annotationsJson?: JsonValue;
  sourceKind?: string;
  sourcePath?: string;
  metadataJson?: JsonValue;
  createdAt?: TimestampInput;
  updatedAt?: TimestampInput;
}

export interface ListTaskRecordsInput {
  context?: string;
  statuses?: string[];
  priorities?: string[];
  placements?: string[];
  limit?: number;
}

export const CURSOR_REVIEW_OPERATION_TYPES = [
  "prepare-branch-review",
  "focus-branch-review",
  "refresh-branch-review",
  "copy-branch-review-prompt",
  "open-branch-review-chat",
  "open-branch-workspace",
  "open-branch-commit-chat",
  "show-branch-diff",
  "delete-branch-path",
  "open-branch-file",
  "open-branch-file-diff",
  "open-review-context-file",
  "paste-review-context-file",
  "open-review-context-chat",
  "tail-review-sessions",
  "summarize-codex-session",
  "sticky-cursor-explain",
  "mark-branch-file-happy",
  "mark-branch-file-needs-work",
  "clear-branch-file-review-state",
] as const;

export type CursorReviewOperationType =
  (typeof CURSOR_REVIEW_OPERATION_TYPES)[number];

const CURSOR_REVIEW_OPERATION_TYPE_SET: ReadonlySet<string> = new Set(
  CURSOR_REVIEW_OPERATION_TYPES,
);

function isCursorReviewOperationType(
  value: string | undefined,
): value is CursorReviewOperationType {
  return (
    value !== undefined && CURSOR_REVIEW_OPERATION_TYPE_SET.has(value)
  );
}

export type CursorReviewOperationResultStatus =
  | "completed"
  | "failed"
  | "ignored";

export interface RecordCursorReviewOperationInput {
  operationId?: string;
  operationType: CursorReviewOperationType;
  repoRoot?: string;
  workspaceRoot?: string;
  bookmark?: string;
  relPath?: string;
  note?: string;
  sourceSessionId?: string;
  sourceChatKind?: string;
  createdAt?: TimestampInput;
}

export interface RecordCursorReviewResultInput {
  operationId: string;
  status: CursorReviewOperationResultStatus;
  clientId?: string;
  repoRoot?: string;
  message?: string;
  processedAt?: TimestampInput;
}

export interface ListCursorReviewOperationsInput {
  repoRoot?: string;
  workspaceRoot?: string;
  includeProcessed?: boolean;
  limit?: number;
}

export interface CursorReviewOperationResultRecord {
  eventId: string;
  operationId: string;
  status: CursorReviewOperationResultStatus;
  clientId?: string;
  repoRoot?: string;
  message?: string;
  processedAt: Date;
}

export interface CursorReviewOperationRecord {
  eventId: string;
  operationId: string;
  operationType: CursorReviewOperationType;
  repoRoot?: string;
  workspaceRoot?: string;
  bookmark?: string;
  relPath?: string;
  note?: string;
  sourceSessionId?: string;
  sourceChatKind?: string;
  createdAt: Date;
  latestResult?: CursorReviewOperationResultRecord;
}

export type BranchFileReviewStatus = "happy" | "needs-work" | "cleared";

export interface RecordBranchFileReviewStateInput {
  eventId?: string;
  repoRoot: string;
  workspaceRoot?: string;
  bookmark: string;
  relPath: string;
  status: BranchFileReviewStatus;
  note?: string;
  sourceSessionId?: string;
  sourceChatKind?: string;
  createdAt?: TimestampInput;
}

export interface ListBranchFileReviewStatesInput {
  repoRoot?: string;
  workspaceRoot?: string;
  bookmark?: string;
  relPath?: string;
  includeCleared?: boolean;
  limit?: number;
}

export interface BranchFileReviewStateRecord {
  eventId: string;
  repoRoot: string;
  workspaceRoot?: string;
  bookmark: string;
  relPath: string;
  status: BranchFileReviewStatus;
  note?: string;
  sourceSessionId?: string;
  sourceChatKind?: string;
  createdAt: Date;
}

export type CommitTurnResultStatus = "completed" | "failed" | "ignored";

export interface RecordCommitTurnOperationInput {
  operationId?: string;
  provider: string;
  sessionId: string;
  conversation: string;
  conversationHash: string;
  trigger: string;
  turnOrdinal: number;
  sessionEventId: string;
  repoRoot?: string;
  repoRoots?: string[];
  cwd?: string;
  artifactPath?: string;
  promptPreview?: string;
  sourceChatKind?: string;
  createdAt?: TimestampInput;
}

export interface RecordCommitTurnResultInput {
  operationId: string;
  status: CommitTurnResultStatus;
  agentId?: string;
  runId?: string;
  threadId?: string;
  repoRoot?: string;
  model?: string;
  effort?: string;
  traceRef?: string;
  message?: string;
  classification?: string;
  title?: string;
  description?: string;
  commitMessage?: string;
  todoItems?: string[];
  notes?: string;
  group?: string;
  groupReason?: string;
  groupIsNew?: boolean;
  snapshotCommitId?: string;
  reviewJobId?: string;
  conversationHash?: string;
  processedAt?: TimestampInput;
}

export interface ListCommitTurnOperationsInput {
  repoRoot?: string;
  conversationHash?: string;
  includeProcessed?: boolean;
  limit?: number;
}

export interface CommitTurnResultRecord {
  eventId: string;
  operationId: string;
  status: CommitTurnResultStatus;
  agentId?: string;
  runId?: string;
  threadId?: string;
  repoRoot?: string;
  model?: string;
  effort?: string;
  traceRef?: string;
  message?: string;
  classification?: string;
  title?: string;
  description?: string;
  commitMessage?: string;
  todoItems?: string[];
  notes?: string;
  group?: string;
  groupReason?: string;
  groupIsNew?: boolean;
  snapshotCommitId?: string;
  reviewJobId?: string;
  conversationHash?: string;
  processedAt: Date;
}

export interface CommitTurnOperationRecord {
  eventId: string;
  operationId: string;
  provider: string;
  sessionId: string;
  conversation: string;
  conversationHash: string;
  trigger: string;
  turnOrdinal: number;
  sessionEventId: string;
  repoRoot?: string;
  repoRoots?: string[];
  cwd?: string;
  artifactPath?: string;
  promptPreview?: string;
  sourceChatKind?: string;
  createdAt: Date;
  latestResult?: CommitTurnResultRecord;
}

export type AgentClaimStatus = "active" | "released" | "expired";

export interface RecordAgentClaimInput {
  claimId?: string;
  scope: string;
  owner: string;
  ownerSession?: string;
  mode?: string;
  note?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  startedAt?: TimestampInput;
  expiresAt?: TimestampInput;
  heartbeatAt?: TimestampInput;
  releasedAt?: TimestampInput;
  status?: AgentClaimStatus;
}

export interface RenewAgentClaimInput {
  claimId: string;
  expiresAt?: TimestampInput;
  heartbeatAt?: TimestampInput;
}

export interface ReleaseAgentClaimInput {
  claimId: string;
  releasedAt?: TimestampInput;
}

export interface ListAgentClaimsInput {
  scopePrefix?: string;
  ownerSession?: string;
  includeReleased?: boolean;
  includeExpired?: boolean;
  limit?: number;
}

export interface AgentClaimRecord {
  eventId: string;
  claimId: string;
  scope: string;
  owner: string;
  ownerSession?: string;
  mode?: string;
  note?: string;
  repoRoot?: string;
  workspaceRoot?: string;
  startedAt: Date;
  expiresAt: Date;
  heartbeatAt: Date;
  releasedAt?: Date;
  status: AgentClaimStatus;
}

export type JobStatus =
  | "queued"
  | "claimed"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "cancelled-superseded";

export type JobEventType =
  | "created"
  | "claimed"
  | "renewed"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "cancelled-superseded";

export interface RecordJobInput {
  jobId?: string;
  kind: string;
  repoRoot?: string;
  workspaceRoot?: string;
  sourceChatKind?: string;
  dedupeKey?: string;
  targetSession?: string;
  targetTurnWatermark?: string;
  sourceSession?: string;
  sourceWatermark?: string;
  payloadJson?: JsonValue;
  resultJson?: JsonValue;
  note?: string;
  createdAt?: TimestampInput;
}

export interface ClaimJobInput {
  jobId: string;
  claimedBy: string;
  leaseExpiresAt?: TimestampInput;
  claimedAt?: TimestampInput;
  attempt?: number;
  note?: string;
}

export interface UpdateJobInput {
  jobId: string;
  status: JobStatus;
  claimedBy?: string;
  leaseExpiresAt?: TimestampInput;
  attempt?: number;
  resultJson?: JsonValue;
  note?: string;
  updatedAt?: TimestampInput;
}

export interface CancelJobInput {
  jobId: string;
  reason?: string;
  cancelledAt?: TimestampInput;
  status?: Extract<JobStatus, "cancelled" | "cancelled-superseded">;
}

export interface ListJobsInput {
  kind?: string;
  status?: JobStatus;
  claimedBy?: string;
  repoRoot?: string;
  targetSession?: string;
  includeFinished?: boolean;
  limit?: number;
}

export interface JobRecord {
  eventId: string;
  jobId: string;
  kind: string;
  status: JobStatus;
  createdAt: Date;
  updatedAt: Date;
  claimedBy?: string;
  leaseExpiresAt?: Date;
  attempt: number;
  payloadJson?: JsonValue;
  resultJson?: JsonValue;
  repoRoot?: string;
  workspaceRoot?: string;
  sourceChatKind?: string;
  dedupeKey?: string;
  targetSession?: string;
  targetTurnWatermark?: string;
  sourceSession?: string;
  sourceWatermark?: string;
  note?: string;
}

export interface JobEventRecord {
  eventId: string;
  jobId: string;
  eventType: JobEventType;
  status?: JobStatus;
  claimedBy?: string;
  leaseExpiresAt?: Date;
  attempt?: number;
  note?: string;
  payloadJson?: JsonValue;
  resultJson?: JsonValue;
  occurredAt: Date;
}

export type ContextDigestStatus = "ready" | "superseded" | "expired" | "error";

export interface RecordContextDigestInput {
  digestId?: string;
  targetProvider: string;
  targetSession: string;
  targetTurnOrdinal: number;
  targetConversation: string;
  targetConversationHash: string;
  sourceSession: string;
  sourceWatermarkKind: string;
  sourceWatermarkValue: string;
  sourceConversationHash?: string;
  kind: string;
  digestText: string;
  modelUsed?: string;
  score?: number;
  confidence?: string;
  reason?: string;
  generatedAt?: TimestampInput;
  expiresAt?: TimestampInput;
  status?: ContextDigestStatus;
}

export interface ListContextDigestsInput {
  targetSession?: string;
  targetConversation?: string;
  targetConversationHash?: string;
  targetTurnOrdinal?: number;
  sourceSession?: string;
  kind?: string;
  includeExpired?: boolean;
  limit?: number;
}

export interface ContextDigestRecord {
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
  sourceConversationHash?: string;
  kind: string;
  digestText: string;
  modelUsed?: string;
  score?: number;
  confidence?: string;
  reason?: string;
  generatedAt: Date;
  expiresAt?: Date;
  status: ContextDigestStatus;
}

function jobEventTypeForStatus(status: JobStatus): JobEventType {
  switch (status) {
    case "queued":
      return "created";
    case "claimed":
      return "claimed";
    case "running":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    case "cancelled-superseded":
      return "cancelled-superseded";
  }
}

export interface AgentRunSummary {
  run: AgentRun;
  items: RunItem[];
  semanticEvents: SemanticEvent[];
  wireEvents: WireEvent[];
  artifacts: Artifact[];
  workspaceSnapshots: WorkspaceSnapshot[];
  memoryLinks: MemoryLink[];
  sourceFiles: SourceFile[];
  latestAgentState: AgentStateSnapshot | null;
}

function taskPlacementRank(value: string | undefined): number {
  if (!value) return Number.MAX_SAFE_INTEGER;
  return TASK_PLACEMENT_ORDER[value] ?? Number.MAX_SAFE_INTEGER - 1;
}

function taskPriorityRank(value: string | undefined): number {
  if (!value) return Number.MAX_SAFE_INTEGER;
  return TASK_PRIORITY_ORDER[value] ?? Number.MAX_SAFE_INTEGER - 1;
}

function compareTaskRecords(left: TaskRecord, right: TaskRecord): number {
  const placementCompare =
    taskPlacementRank(left.placement) - taskPlacementRank(right.placement);
  if (placementCompare !== 0) return placementCompare;

  const leftFocusRank = left.focus_rank ?? Number.MAX_SAFE_INTEGER;
  const rightFocusRank = right.focus_rank ?? Number.MAX_SAFE_INTEGER;
  if (leftFocusRank !== rightFocusRank) {
    return leftFocusRank - rightFocusRank;
  }

  const priorityCompare =
    taskPriorityRank(left.priority) - taskPriorityRank(right.priority);
  if (priorityCompare !== 0) return priorityCompare;

  const updatedCompare = right.updated_at.getTime() - left.updated_at.getTime();
  if (updatedCompare !== 0) return updatedCompare;

  return left.task_id.localeCompare(right.task_id);
}

function asObjectRecord(
  value: JsonValue | undefined,
): Record<string, JsonValue> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, JsonValue>;
}

function readObjectString(
  value: Record<string, JsonValue> | null,
  key: string,
): string | undefined {
  const raw = value?.[key];
  return typeof raw === "string" ? raw : undefined;
}

function readObjectStringArray(
  value: Record<string, JsonValue> | null,
  key: string,
): string[] | undefined {
  const raw = value?.[key];
  if (!Array.isArray(raw)) return undefined;
  const items = raw.filter(
    (entry): entry is string => typeof entry === "string",
  );
  return items.length > 0 ? items : undefined;
}

function readObjectNumber(
  value: Record<string, JsonValue> | null,
  key: string,
): number | undefined {
  const raw = value?.[key];
  return typeof raw === "number" ? raw : undefined;
}

function readObjectBoolean(
  value: Record<string, JsonValue> | null,
  key: string,
): boolean | undefined {
  const raw = value?.[key];
  return typeof raw === "boolean" ? raw : undefined;
}

function readObjectDate(
  value: Record<string, JsonValue> | null,
  key: string,
): Date | undefined {
  const raw = value?.[key];
  if (typeof raw !== "string" && typeof raw !== "number") {
    return undefined;
  }
  const date = new Date(raw);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

function asDate(value?: TimestampInput): Date {
  if (value instanceof Date) return value;
  if (typeof value === "string" || typeof value === "number") {
    return new Date(value);
  }
  return new Date();
}

function pruneUndefined<T extends Record<string, unknown>>(
  input: T,
): Partial<T> {
  const entries = Object.entries(input).filter(
    ([, value]) => value !== undefined,
  );
  return Object.fromEntries(entries) as Partial<T>;
}

function clampLimit(limit: number | undefined, fallback = 20): number {
  return Math.max(1, Math.min(limit ?? fallback, 200));
}

function isExpired(expiresAt: Date | undefined, now: Date): boolean {
  return Boolean(expiresAt && expiresAt.getTime() <= now.getTime());
}

export class AgentDataStore {
  constructor(
    private readonly context: JazzContext,
    private readonly writeTier: DurabilityTier,
    private readonly useBackendSync = false,
  ) {}

  flush(): void {
    this.context.flush();
  }

  async shutdown(): Promise<void> {
    await this.context.shutdown();
  }

  async upsertAgent(
    input: UpsertAgentInput,
    session?: Session,
  ): Promise<Agent> {
    const db = this.getDb(session);
    const now = asDate(input.updatedAt);
    const existing = await this.getAgentByExternalId(db, input.agentId);

    if (existing) {
      await this.updateRow(db, app.agents, existing.id, {
        lane: input.lane,
        spec_path: input.specPath,
        prompt_surface: input.promptSurface,
        status: input.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireByQuery(
        db,
        app.agents.where({ agent_id: input.agentId }),
        "agent",
      );
    }

    return await db.insert(
      app.agents,
      {
        agent_id: input.agentId,
        lane: input.lane,
        spec_path: input.specPath,
        prompt_surface: input.promptSurface,
        status: input.status,
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
    ).wait({ tier: this.writeTier });
  }

  async recordRunStarted(
    input: RecordRunStartedInput,
    session?: Session,
  ): Promise<AgentRun> {
    const db = this.getDb(session);
    const agent = await this.upsertAgent(
      {
        agentId: input.agentId,
        status: input.status,
        lane: input.agent?.lane,
        specPath: input.agent?.specPath,
        promptSurface: input.agent?.promptSurface,
        metadataJson: input.agent?.metadataJson,
      },
      session,
    );
    const existing = await this.getRunByExternalId(db, input.runId);

    if (existing) {
      await this.updateRow(db, app.agent_runs, existing.id, {
        agent_id: input.agentId,
        agent_row_id: agent.id,
        thread_id: input.threadId,
        turn_id: input.turnId,
        cwd: input.cwd,
        repo_root: input.repoRoot,
        request_summary: input.requestSummary,
        status: input.status ?? existing.status ?? DEFAULT_STATUS,
        started_at: input.startedAt ? asDate(input.startedAt) : undefined,
        context_json: input.contextJson,
        source_trace_path: input.sourceTracePath,
      });
      return this.requireByQuery(
        db,
        app.agent_runs.where({ run_id: input.runId }),
        "agent run",
      );
    }

    return await db.insert(
      app.agent_runs,
      {
        run_id: input.runId,
        agent_id: input.agentId,
        agent_row_id: agent.id,
        thread_id: input.threadId,
        turn_id: input.turnId,
        cwd: input.cwd,
        repo_root: input.repoRoot,
        request_summary: input.requestSummary,
        status: input.status ?? DEFAULT_STATUS,
        started_at: asDate(input.startedAt),
        context_json: input.contextJson,
        source_trace_path: input.sourceTracePath,
      },
    ).wait({ tier: this.writeTier });
  }

  async recordRunCompleted(
    input: RecordRunCompletedInput,
    session?: Session,
  ): Promise<AgentRun> {
    const db = this.getDb(session);
    const existing = await this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
    await this.updateRow(db, app.agent_runs, existing.id, {
      status: input.status ?? "completed",
      ended_at: asDate(input.endedAt),
    });
    return this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
  }

  async recordItemStarted(
    input: RecordItemStartedInput,
    session?: Session,
  ): Promise<RunItem> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
    const existing = await this.getItemByExternalId(
      db,
      input.runId,
      input.itemId,
    );

    if (existing) {
      await this.updateRow(db, app.run_items, existing.id, {
        item_kind: input.itemKind,
        phase: input.phase,
        sequence: input.sequence,
        status: input.status ?? existing.status,
        summary_json: input.summaryJson,
        started_at: input.startedAt ? asDate(input.startedAt) : undefined,
      });
      return this.requireItemByExternalId(db, input.runId, input.itemId);
    }

    return (db as any).insertDurable(
      app.run_items,
      {
        item_id: input.itemId,
        run_id: input.runId,
        run_row_id: run.id,
        item_kind: input.itemKind,
        phase: input.phase,
        sequence: input.sequence,
        status: input.status ?? "started",
        summary_json: input.summaryJson,
        started_at: asDate(input.startedAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordItemCompleted(
    input: RecordItemCompletedInput,
    session?: Session,
  ): Promise<RunItem> {
    const db = this.getDb(session);
    const existing = await this.requireItemByExternalId(
      db,
      input.runId,
      input.itemId,
    );
    await this.updateRow(db, app.run_items, existing.id, {
      status: input.status ?? "completed",
      summary_json: input.summaryJson,
      completed_at: asDate(input.completedAt),
    });
    return this.requireItemByExternalId(db, input.runId, input.itemId);
  }

  async appendSemanticEvent(
    input: AppendSemanticEventInput,
    session?: Session,
  ): Promise<SemanticEvent> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
    const item = input.itemId
      ? await this.getItemByExternalId(db, input.runId, input.itemId)
      : null;
    const eventId = input.eventId ?? randomUUID();
    const existing = await db.one(
      app.semantic_events.where({ event_id: eventId }),
    );

    if (existing) {
      await this.updateRow(db, app.semantic_events, existing.id, {
        item_id: input.itemId,
        item_row_id: item?.id,
        event_type: input.eventType,
        summary_text: input.summaryText,
        payload_json: input.payloadJson,
        occurred_at: input.occurredAt ? asDate(input.occurredAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.semantic_events.where({ event_id: eventId }),
        "semantic event",
      );
    }

    return await db.insert(
      app.semantic_events,
      {
        event_id: eventId,
        run_id: input.runId,
        run_row_id: run.id,
        item_id: input.itemId,
        item_row_id: item?.id,
        event_type: input.eventType,
        summary_text: input.summaryText,
        payload_json: input.payloadJson,
        occurred_at: asDate(input.occurredAt),
      },
    ).wait({ tier: this.writeTier });
  }

  async appendWireEvent(
    input: AppendWireEventInput,
    session?: Session,
  ): Promise<WireEvent> {
    const db = this.getDb(session);
    const run = input.runId
      ? await this.getRunByExternalId(db, input.runId)
      : null;
    const eventId = input.eventId ?? randomUUID();
    const existing = await db.one(app.wire_events.where({ event_id: eventId }));

    if (existing) {
      await this.updateRow(db, app.wire_events, existing.id, {
        run_id: input.runId,
        run_row_id: run?.id,
        connection_id: input.connectionId,
        session_id: input.sessionId,
        direction: input.direction,
        method: input.method,
        request_id: input.requestId,
        payload_json: input.payloadJson,
        occurred_at: input.occurredAt ? asDate(input.occurredAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.wire_events.where({ event_id: eventId }),
        "wire event",
      );
    }

    return (db as any).insertDurable(
      app.wire_events,
      {
        event_id: eventId,
        run_id: input.runId,
        run_row_id: run?.id,
        connection_id: input.connectionId,
        session_id: input.sessionId,
        direction: input.direction,
        method: input.method,
        request_id: input.requestId,
        payload_json: input.payloadJson,
        occurred_at: asDate(input.occurredAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordArtifact(
    input: RecordArtifactInput,
    session?: Session,
  ): Promise<Artifact> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
    const artifactId = input.artifactId ?? randomUUID();
    const existing = await db.one(
      app.artifacts.where({ artifact_id: artifactId }),
    );

    if (existing) {
      await this.updateRow(db, app.artifacts, existing.id, {
        artifact_kind: input.artifactKind,
        title: input.title,
        absolute_path: input.absolutePath,
        checksum: input.checksum,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.artifacts.where({ artifact_id: artifactId }),
        "artifact",
      );
    }

    return (db as any).insertDurable(
      app.artifacts,
      {
        artifact_id: artifactId,
        run_id: input.runId,
        run_row_id: run.id,
        artifact_kind: input.artifactKind,
        title: input.title,
        absolute_path: input.absolutePath,
        checksum: input.checksum,
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordWorkspaceSnapshot(
    input: RecordWorkspaceSnapshotInput,
    session?: Session,
  ): Promise<WorkspaceSnapshot> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(
      db,
      app.agent_runs.where({ run_id: input.runId }),
      "agent run",
    );
    const snapshotId = input.snapshotId ?? randomUUID();
    const existing = await db.one(
      app.workspace_snapshots.where({ snapshot_id: snapshotId }),
    );

    if (existing) {
      await this.updateRow(db, app.workspace_snapshots, existing.id, {
        repo_root: input.repoRoot,
        branch: input.branch,
        head_commit: input.headCommit,
        dirty_path_count: input.dirtyPathCount,
        snapshot_json: input.snapshotJson,
        captured_at: input.capturedAt ? asDate(input.capturedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.workspace_snapshots.where({ snapshot_id: snapshotId }),
        "workspace snapshot",
      );
    }

    return (db as any).insertDurable(
      app.workspace_snapshots,
      {
        snapshot_id: snapshotId,
        run_id: input.runId,
        run_row_id: run.id,
        repo_root: input.repoRoot,
        branch: input.branch,
        head_commit: input.headCommit,
        dirty_path_count: input.dirtyPathCount,
        snapshot_json: input.snapshotJson,
        captured_at: asDate(input.capturedAt),
      },
      { tier: this.writeTier },
    );
  }

  async updateAgentState(
    input: UpdateAgentStateInput,
    session?: Session,
  ): Promise<AgentStateSnapshot> {
    const db = this.getDb(session);
    const agent = await this.upsertAgent(
      {
        agentId: input.agentId,
        status: input.status,
        updatedAt: input.capturedAt,
      },
      session,
    );
    const snapshotId = input.snapshotId ?? randomUUID();
    const existing = await db.one(
      app.agent_state_snapshots.where({ snapshot_id: snapshotId }),
    );

    if (existing) {
      await this.updateRow(db, app.agent_state_snapshots, existing.id, {
        state_version: input.stateVersion,
        status: input.status,
        state_json: input.stateJson,
        captured_at: input.capturedAt ? asDate(input.capturedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.agent_state_snapshots.where({ snapshot_id: snapshotId }),
        "agent state snapshot",
      );
    }

    return await db.insert(
      app.agent_state_snapshots,
      {
        snapshot_id: snapshotId,
        agent_id: input.agentId,
        agent_row_id: agent.id,
        state_version: input.stateVersion,
        status: input.status,
        state_json: input.stateJson,
        captured_at: asDate(input.capturedAt),
      },
    ).wait({ tier: this.writeTier });
  }

  async recordMemoryLink(
    input: RecordMemoryLinkInput,
    session?: Session,
  ): Promise<MemoryLink> {
    const db = this.getDb(session);
    const run = input.runId
      ? await this.getRunByExternalId(db, input.runId)
      : null;
    const item =
      input.runId && input.itemId
        ? await this.getItemByExternalId(db, input.runId, input.itemId)
        : null;
    const linkId = input.linkId ?? randomUUID();
    const existing = await db.one(app.memory_links.where({ link_id: linkId }));

    if (existing) {
      await this.updateRow(db, app.memory_links, existing.id, {
        run_id: input.runId,
        run_row_id: run?.id,
        item_id: input.itemId,
        item_row_id: item?.id,
        memory_scope: input.memoryScope,
        memory_ref: input.memoryRef,
        query_text: input.queryText,
        link_json: input.linkJson,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.memory_links.where({ link_id: linkId }),
        "memory link",
      );
    }

    return (db as any).insertDurable(
      app.memory_links,
      {
        link_id: linkId,
        run_id: input.runId,
        run_row_id: run?.id,
        item_id: input.itemId,
        item_row_id: item?.id,
        memory_scope: input.memoryScope,
        memory_ref: input.memoryRef,
        query_text: input.queryText,
        link_json: input.linkJson,
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordSourceFile(
    input: RecordSourceFileInput,
    session?: Session,
  ): Promise<SourceFile> {
    const db = this.getDb(session);
    const run = input.runId
      ? await this.getRunByExternalId(db, input.runId)
      : null;
    const sourceFileId = input.sourceFileId ?? randomUUID();
    const existing = await db.one(
      app.source_files.where({ source_file_id: sourceFileId }),
    );

    if (existing) {
      await this.updateRow(db, app.source_files, existing.id, {
        run_id: input.runId,
        run_row_id: run?.id,
        file_kind: input.fileKind,
        absolute_path: input.absolutePath,
        checksum: input.checksum,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.source_files.where({ source_file_id: sourceFileId }),
        "source file",
      );
    }

    return (db as any).insertDurable(
      app.source_files,
      {
        source_file_id: sourceFileId,
        run_id: input.runId,
        run_row_id: run?.id,
        file_kind: input.fileKind,
        absolute_path: input.absolutePath,
        checksum: input.checksum,
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerObjectRef(
    input: RecordDesignerObjectRefInput,
    session?: Session,
  ): Promise<DesignerObjectRef> {
    const db = this.getDb(session);
    const objectRefId = input.objectRefId ?? input.digestSha256 ?? input.uri;
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerObjectRefByExternalId(
      db,
      objectRefId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_object_refs, existing.id, {
        provider: input.provider,
        uri: input.uri,
        bucket: input.bucket,
        key: input.key,
        region: input.region,
        digest_sha256: input.digestSha256,
        byte_size: input.byteSize,
        content_type: input.contentType,
        object_kind: input.objectKind,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireDesignerObjectRefByExternalId(db, objectRefId);
    }

    return (db as any).insertDurable(
      app.designer_object_refs,
      {
        object_ref_id: objectRefId,
        provider: input.provider,
        uri: input.uri,
        bucket: input.bucket,
        key: input.key,
        region: input.region,
        digest_sha256: input.digestSha256,
        byte_size: input.byteSize,
        content_type: input.contentType,
        object_kind: input.objectKind,
        status: input.status ?? "available",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCodexConversation(
    input: RecordDesignerCodexConversationInput,
    session?: Session,
  ): Promise<DesignerCodexConversation> {
    const db = this.getDb(session);
    const transcriptObject = await this.requireDesignerObjectRefByExternalId(
      db,
      input.transcriptObjectRefId,
    );
    const conversationId =
      input.conversationId ?? `${input.provider}:${input.providerSessionId}`;
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCodexConversationByExternalId(
      db,
      conversationId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_codex_conversations, existing.id, {
        provider: input.provider,
        provider_session_id: input.providerSessionId,
        thread_id: input.threadId,
        workspace_id: input.workspaceId,
        workspace_key: input.workspaceKey,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        branch: input.branch,
        model: input.model,
        status: input.status ?? existing.status,
        transcript_object_ref_id: input.transcriptObjectRefId,
        transcript_object_row_id: transcriptObject.id,
        latest_event_sequence: input.latestEventSequence,
        metadata_json: input.metadataJson,
        updated_at: now,
        ended_at: input.endedAt ? asDate(input.endedAt) : undefined,
      });
      return this.requireDesignerCodexConversationByExternalId(
        db,
        conversationId,
      );
    }

    return (db as any).insertDurable(
      app.designer_codex_conversations,
      {
        conversation_id: conversationId,
        provider: input.provider,
        provider_session_id: input.providerSessionId,
        thread_id: input.threadId,
        workspace_id: input.workspaceId,
        workspace_key: input.workspaceKey,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        branch: input.branch,
        model: input.model,
        status: input.status ?? "running",
        transcript_object_ref_id: input.transcriptObjectRefId,
        transcript_object_row_id: transcriptObject.id,
        latest_event_sequence: input.latestEventSequence,
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
        ended_at: input.endedAt ? asDate(input.endedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCodexTurn(
    input: RecordDesignerCodexTurnInput,
    session?: Session,
  ): Promise<DesignerCodexTurn> {
    const db = this.getDb(session);
    const conversation = await this.requireDesignerCodexConversationByExternalId(
      db,
      input.conversationId,
    );
    const payloadObject = await this.requireDesignerObjectRefByExternalId(
      db,
      input.payloadObjectRefId,
    );
    const promptObject = input.promptObjectRefId
      ? await this.requireDesignerObjectRefByExternalId(
          db,
          input.promptObjectRefId,
        )
      : null;
    const responseObject = input.responseObjectRefId
      ? await this.requireDesignerObjectRefByExternalId(
          db,
          input.responseObjectRefId,
        )
      : null;
    const turnId = input.turnId ?? `${input.conversationId}:${input.sequence}`;
    const existing = await db.one(
      app.designer_codex_turns.where({ turn_id: turnId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_codex_turns, existing.id, {
        conversation_id: input.conversationId,
        conversation_row_id: conversation.id,
        sequence: input.sequence,
        turn_kind: input.turnKind,
        role: input.role,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        summary_text: input.summaryText,
        payload_object_ref_id: input.payloadObjectRefId,
        payload_object_row_id: payloadObject.id,
        prompt_object_ref_id: input.promptObjectRefId,
        prompt_object_row_id: promptObject?.id,
        response_object_ref_id: input.responseObjectRefId,
        response_object_row_id: responseObject?.id,
        token_counts_json: input.tokenCountsJson,
        status: input.status ?? existing.status,
        started_at: input.startedAt ? asDate(input.startedAt) : undefined,
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_codex_turns.where({ turn_id: turnId }),
        "designer codex turn",
      );
    }

    return (db as any).insertDurable(
      app.designer_codex_turns,
      {
        turn_id: turnId,
        conversation_id: input.conversationId,
        conversation_row_id: conversation.id,
        sequence: input.sequence,
        turn_kind: input.turnKind,
        role: input.role,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        summary_text: input.summaryText,
        payload_object_ref_id: input.payloadObjectRefId,
        payload_object_row_id: payloadObject.id,
        prompt_object_ref_id: input.promptObjectRefId,
        prompt_object_row_id: promptObject?.id,
        response_object_ref_id: input.responseObjectRefId,
        response_object_row_id: responseObject?.id,
        token_counts_json: input.tokenCountsJson,
        status: input.status ?? "completed",
        started_at: asDate(input.startedAt),
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerTelemetryEvent(
    input: RecordDesignerTelemetryEventInput,
    session?: Session,
  ): Promise<DesignerTelemetryEvent> {
    const db = this.getDb(session);
    const conversation = input.conversationId
      ? await this.requireDesignerCodexConversationByExternalId(
          db,
          input.conversationId,
        )
      : null;
    const payloadObject = await this.requireDesignerObjectRefByExternalId(
      db,
      input.payloadObjectRefId,
    );
    const telemetryEventId = input.telemetryEventId ?? randomUUID();
    const existing = await db.one(
      app.designer_telemetry_events.where({
        telemetry_event_id: telemetryEventId,
      }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_telemetry_events, existing.id, {
        session_id: input.sessionId,
        workspace_id: input.workspaceId,
        conversation_id: input.conversationId,
        conversation_row_id: conversation?.id,
        event_type: input.eventType,
        pane: input.pane,
        sequence: input.sequence,
        summary_text: input.summaryText,
        payload_object_ref_id: input.payloadObjectRefId,
        payload_object_row_id: payloadObject.id,
        properties_json: input.propertiesJson,
        occurred_at: input.occurredAt
          ? asDate(input.occurredAt)
          : undefined,
        ingested_at: input.ingestedAt ? asDate(input.ingestedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_telemetry_events.where({
          telemetry_event_id: telemetryEventId,
        }),
        "designer telemetry event",
      );
    }

    return (db as any).insertDurable(
      app.designer_telemetry_events,
      {
        telemetry_event_id: telemetryEventId,
        session_id: input.sessionId,
        workspace_id: input.workspaceId,
        conversation_id: input.conversationId,
        conversation_row_id: conversation?.id,
        event_type: input.eventType,
        pane: input.pane,
        sequence: input.sequence,
        summary_text: input.summaryText,
        payload_object_ref_id: input.payloadObjectRefId,
        payload_object_row_id: payloadObject.id,
        properties_json: input.propertiesJson,
        occurred_at: asDate(input.occurredAt),
        ingested_at: asDate(input.ingestedAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerAgent(
    input: RecordDesignerAgentInput,
    session?: Session,
  ): Promise<DesignerAgent> {
    const db = this.getDb(session);
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerAgentByExternalId(
      db,
      input.agentId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_agents, existing.id, {
        agent_kind: input.agentKind,
        provider: input.provider,
        display_name: input.displayName,
        model: input.model,
        default_context_json: input.defaultContextJson,
        tool_contract_json: input.toolContractJson,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireDesignerAgentByExternalId(db, input.agentId);
    }

    return (db as any).insertDurable(
      app.designer_agents,
      {
        agent_id: input.agentId,
        agent_kind: input.agentKind,
        provider: input.provider,
        display_name: input.displayName,
        model: input.model,
        default_context_json: input.defaultContextJson,
        tool_contract_json: input.toolContractJson,
        status: input.status ?? "active",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerAgentTool(
    input: RecordDesignerAgentToolInput,
    session?: Session,
  ): Promise<DesignerAgentTool> {
    const db = this.getDb(session);
    const agent = await this.requireDesignerAgentByExternalId(
      db,
      input.agentId,
    );
    const toolId = input.toolId ?? `${input.agentId}:tool:${input.toolName}`;
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerAgentToolByExternalId(db, toolId);

    if (existing) {
      await this.updateRow(db, app.designer_agent_tools, existing.id, {
        agent_id: input.agentId,
        agent_row_id: agent.id,
        tool_name: input.toolName,
        tool_kind: input.toolKind,
        input_schema_json: input.inputSchemaJson,
        output_schema_json: input.outputSchemaJson,
        scope_json: input.scopeJson,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireDesignerAgentToolByExternalId(db, toolId);
    }

    return (db as any).insertDurable(
      app.designer_agent_tools,
      {
        tool_id: toolId,
        agent_id: input.agentId,
        agent_row_id: agent.id,
        tool_name: input.toolName,
        tool_kind: input.toolKind,
        input_schema_json: input.inputSchemaJson,
        output_schema_json: input.outputSchemaJson,
        scope_json: input.scopeJson,
        status: input.status ?? "active",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerAgentContext(
    input: RecordDesignerAgentContextInput,
    session?: Session,
  ): Promise<DesignerAgentContext> {
    const db = this.getDb(session);
    const agent = await this.requireDesignerAgentByExternalId(
      db,
      input.agentId,
    );
    const objectRef = input.objectRefId
      ? await this.requireDesignerObjectRefByExternalId(db, input.objectRefId)
      : null;
    const contextId =
      input.contextId ??
      `${input.agentId}:context:${input.contextKind}:${input.sourceKind}`;
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerAgentContextByExternalId(
      db,
      contextId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_agent_contexts, existing.id, {
        agent_id: input.agentId,
        agent_row_id: agent.id,
        context_kind: input.contextKind,
        source_kind: input.sourceKind,
        object_ref_id: input.objectRefId,
        object_ref_row_id: objectRef?.id,
        inline_context_json: input.inlineContextJson,
        priority: input.priority ?? existing.priority,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireDesignerAgentContextByExternalId(db, contextId);
    }

    return (db as any).insertDurable(
      app.designer_agent_contexts,
      {
        context_id: contextId,
        agent_id: input.agentId,
        agent_row_id: agent.id,
        context_kind: input.contextKind,
        source_kind: input.sourceKind,
        object_ref_id: input.objectRefId,
        object_ref_row_id: objectRef?.id,
        inline_context_json: input.inlineContextJson,
        priority: input.priority ?? 0,
        status: input.status ?? "active",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerLiveCommit(
    input: RecordDesignerLiveCommitInput,
    session?: Session,
  ): Promise<DesignerLiveCommit> {
    const db = this.getDb(session);
    const sourceConversation = input.sourceConversationId
      ? await this.requireDesignerCodexConversationByExternalId(
          db,
          input.sourceConversationId,
        )
      : null;
    const sourceTurn = await this.resolveDesignerCodexTurn(db, {
      sourceConversationId: input.sourceConversationId,
      sourceTurnId: input.sourceTurnId,
      sourceTurnOrdinal: input.sourceTurnOrdinal,
    });
    const agent = input.agentId
      ? await this.requireDesignerAgentByExternalId(db, input.agentId)
      : null;
    const patchObject = input.patchObjectRefId
      ? await this.requireDesignerObjectRefByExternalId(
          db,
          input.patchObjectRefId,
        )
      : null;
    const manifestObject = input.manifestObjectRefId
      ? await this.requireDesignerObjectRefByExternalId(
          db,
          input.manifestObjectRefId,
        )
      : null;
    const existing = await this.getDesignerLiveCommitByExternalId(
      db,
      input.commitId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_live_commits, existing.id, {
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        branch: input.branch,
        bookmark: input.bookmark,
        live_ref: input.liveRef,
        tree_id: input.treeId,
        parent_commit_ids_json: input.parentCommitIdsJson,
        subject: input.subject,
        body: input.body,
        author_name: input.authorName,
        author_email: input.authorEmail,
        committer_name: input.committerName,
        committer_email: input.committerEmail,
        trace_ref: input.traceRef,
        source_session_id: input.sourceSessionId,
        source_turn_ordinal: input.sourceTurnOrdinal,
        source_conversation_id: input.sourceConversationId,
        source_conversation_row_id: sourceConversation?.id,
        source_turn_id: input.sourceTurnId,
        source_turn_row_id: sourceTurn?.id,
        agent_id: input.agentId,
        agent_row_id: agent?.id,
        courier_run_id: input.courierRunId,
        live_snapshot_ref: input.liveSnapshotRef,
        changed_paths_json: input.changedPathsJson,
        patch_object_ref_id: input.patchObjectRefId,
        patch_object_row_id: patchObject?.id,
        manifest_object_ref_id: input.manifestObjectRefId,
        manifest_object_row_id: manifestObject?.id,
        status: input.status ?? existing.status,
        committed_at: input.committedAt
          ? asDate(input.committedAt)
          : undefined,
        reflected_at: input.reflectedAt
          ? asDate(input.reflectedAt)
          : undefined,
        ingested_at: input.ingestedAt ? asDate(input.ingestedAt) : undefined,
      });
      return this.requireDesignerLiveCommitByExternalId(db, input.commitId);
    }

    return (db as any).insertDurable(
      app.designer_live_commits,
      {
        commit_id: input.commitId,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        branch: input.branch,
        bookmark: input.bookmark,
        live_ref: input.liveRef,
        tree_id: input.treeId,
        parent_commit_ids_json: input.parentCommitIdsJson,
        subject: input.subject,
        body: input.body,
        author_name: input.authorName,
        author_email: input.authorEmail,
        committer_name: input.committerName,
        committer_email: input.committerEmail,
        trace_ref: input.traceRef,
        source_session_id: input.sourceSessionId,
        source_turn_ordinal: input.sourceTurnOrdinal,
        source_conversation_id: input.sourceConversationId,
        source_conversation_row_id: sourceConversation?.id,
        source_turn_id: input.sourceTurnId,
        source_turn_row_id: sourceTurn?.id,
        agent_id: input.agentId,
        agent_row_id: agent?.id,
        courier_run_id: input.courierRunId,
        live_snapshot_ref: input.liveSnapshotRef,
        changed_paths_json: input.changedPathsJson,
        patch_object_ref_id: input.patchObjectRefId,
        patch_object_row_id: patchObject?.id,
        manifest_object_ref_id: input.manifestObjectRefId,
        manifest_object_row_id: manifestObject?.id,
        status: input.status ?? "reflected",
        committed_at: input.committedAt
          ? asDate(input.committedAt)
          : undefined,
        reflected_at: input.reflectedAt
          ? asDate(input.reflectedAt)
          : undefined,
        ingested_at: asDate(input.ingestedAt),
      },
      { tier: this.writeTier },
    );
  }

  async listDesignerCodexTurns(
    input: ListDesignerCodexTurnsInput = {},
    session?: Session,
  ): Promise<DesignerCodexTurn[]> {
    const db = this.getDb(session);
    const rows = input.conversationId
      ? await db.all(
          app.designer_codex_turns
            .where({ conversation_id: input.conversationId })
            .orderBy("sequence", "asc"),
        )
      : await db.all(
          app.designer_codex_turns
            .orderBy("sequence", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((turn) => {
        if (input.conversationId && turn.conversation_id !== input.conversationId)
          return false;
        if (input.role && turn.role !== input.role) return false;
        if (input.status && turn.status !== input.status) return false;
        if (
          input.afterSequence !== undefined &&
          turn.sequence <= input.afterSequence
        ) {
          return false;
        }
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async listDesignerTelemetryEvents(
    input: ListDesignerTelemetryEventsInput = {},
    session?: Session,
  ): Promise<DesignerTelemetryEvent[]> {
    const db = this.getDb(session);
    const rows = input.conversationId
      ? await db.all(
          app.designer_telemetry_events
            .where({ conversation_id: input.conversationId })
            .orderBy("ingested_at", "asc"),
        )
      : await db.all(
          app.designer_telemetry_events
            .orderBy("ingested_at", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((event) => {
        if (input.conversationId && event.conversation_id !== input.conversationId)
          return false;
        if (input.sessionId && event.session_id !== input.sessionId)
          return false;
        if (input.workspaceId && event.workspace_id !== input.workspaceId)
          return false;
        if (input.eventType && event.event_type !== input.eventType)
          return false;
        if (
          input.afterSequence !== undefined &&
          (event.sequence ?? 0) <= input.afterSequence
        ) {
          return false;
        }
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async listDesignerAgentTools(
    input: ListDesignerAgentToolsInput = {},
    session?: Session,
  ): Promise<DesignerAgentTool[]> {
    const db = this.getDb(session);
    const rows = input.agentId
      ? await db.all(
          app.designer_agent_tools
            .where({ agent_id: input.agentId })
            .orderBy("tool_name", "asc"),
        )
      : await db.all(
          app.designer_agent_tools
            .orderBy("tool_name", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((tool) => {
        if (input.agentId && tool.agent_id !== input.agentId) return false;
        if (input.toolKind && tool.tool_kind !== input.toolKind) return false;
        if (input.status && tool.status !== input.status) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async listDesignerAgentContexts(
    input: ListDesignerAgentContextsInput = {},
    session?: Session,
  ): Promise<DesignerAgentContext[]> {
    const db = this.getDb(session);
    const rows = input.agentId
      ? await db.all(
          app.designer_agent_contexts
            .where({ agent_id: input.agentId })
            .orderBy("priority", "asc"),
        )
      : await db.all(
          app.designer_agent_contexts
            .orderBy("priority", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((context) => {
        if (input.agentId && context.agent_id !== input.agentId) return false;
        if (input.contextKind && context.context_kind !== input.contextKind)
          return false;
        if (input.sourceKind && context.source_kind !== input.sourceKind)
          return false;
        if (input.status && context.status !== input.status) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async listDesignerLiveCommits(
    input: ListDesignerLiveCommitsInput = {},
    session?: Session,
  ): Promise<DesignerLiveCommit[]> {
    const db = this.getDb(session);
    const rows = input.repoRoot
      ? await db.all(
          app.designer_live_commits
            .where({ repo_root: input.repoRoot })
            .orderBy("ingested_at", "asc"),
        )
      : await db.all(
          app.designer_live_commits
            .orderBy("ingested_at", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((commit) => {
        if (input.repoRoot && commit.repo_root !== input.repoRoot) return false;
        if (input.branch && commit.branch !== input.branch) return false;
        if (
          input.sourceSessionId &&
          commit.source_session_id !== input.sourceSessionId
        ) {
          return false;
        }
        if (input.agentId && commit.agent_id !== input.agentId) return false;
        if (input.status && commit.status !== input.status) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async getDesignerCodexConversationSummary(
    conversationId: string,
    session?: Session,
  ): Promise<DesignerCodexConversationSummary | null> {
    const db = this.getDb(session);
    const conversation = await this.getDesignerCodexConversationByExternalId(
      db,
      conversationId,
    );
    if (!conversation) return null;

    const [transcriptObject, turns, telemetryEvents] = await Promise.all([
      this.requireDesignerObjectRefByExternalId(
        db,
        conversation.transcript_object_ref_id,
      ),
      this.listDesignerCodexTurns({ conversationId }, session),
      this.listDesignerTelemetryEvents({ conversationId }, session),
    ]);

    return {
      conversation,
      transcriptObject,
      turns,
      telemetryEvents,
    };
  }

  async getDesignerLiveCommitSummary(
    commitId: string,
    session?: Session,
  ): Promise<DesignerLiveCommitSummary | null> {
    const db = this.getDb(session);
    const commit = await this.getDesignerLiveCommitByExternalId(db, commitId);
    if (!commit) return null;

    const [agent, patchObject, manifestObject, sourceConversation, sourceTurn] =
      await Promise.all([
        commit.agent_id
          ? this.getDesignerAgentByExternalId(db, commit.agent_id)
          : Promise.resolve(null),
        commit.patch_object_ref_id
          ? this.getDesignerObjectRefByExternalId(db, commit.patch_object_ref_id)
          : Promise.resolve(null),
        commit.manifest_object_ref_id
          ? this.getDesignerObjectRefByExternalId(
              db,
              commit.manifest_object_ref_id,
            )
          : Promise.resolve(null),
        commit.source_conversation_id
          ? this.getDesignerCodexConversationByExternalId(
              db,
              commit.source_conversation_id,
            )
          : Promise.resolve(null),
        commit.source_turn_id
          ? this.getDesignerCodexTurnByExternalId(db, commit.source_turn_id)
          : Promise.resolve(null),
      ]);

    return {
      commit,
      agent: agent ?? undefined,
      patchObject: patchObject ?? undefined,
      manifestObject: manifestObject ?? undefined,
      sourceConversation: sourceConversation ?? undefined,
      sourceTurn: sourceTurn ?? undefined,
    };
  }

  async recordDesignerCadWorkspace(
    input: RecordDesignerCadWorkspaceInput,
    session?: Session,
  ): Promise<DesignerCadWorkspace> {
    const db = this.getDb(session);
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadWorkspaceByExternalId(
      db,
      input.workspaceId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_workspaces, existing.id, {
        workspace_key: input.workspaceKey ?? existing.workspace_key,
        title: input.title,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_workspaces.where({
          workspace_id: input.workspaceId,
        }),
        "designer cad workspace",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_workspaces,
      {
        workspace_id: input.workspaceId,
        workspace_key: input.workspaceKey ?? input.workspaceId,
        title: input.title,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        status: input.status ?? "active",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadDocument(
    input: RecordDesignerCadDocumentInput,
    session?: Session,
  ): Promise<DesignerCadDocument> {
    const db = this.getDb(session);
    const workspace = await this.requireDesignerCadWorkspaceByExternalId(
      db,
      input.workspaceId,
    );
    const documentId = input.documentId ?? `${input.workspaceId}:${input.filePath}`;
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadDocumentByExternalId(
      db,
      documentId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_documents, existing.id, {
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        file_path: input.filePath,
        language: input.language ?? existing.language,
        source_kind: input.sourceKind ?? existing.source_kind,
        source_hash: input.sourceHash,
        status: input.status ?? existing.status,
        metadata_json: input.metadataJson,
        updated_at: now,
      });
      return this.requireDesignerCadDocumentByExternalId(db, documentId);
    }

    return (db as any).insertDurable(
      app.designer_cad_documents,
      {
        document_id: documentId,
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        file_path: input.filePath,
        language: input.language ?? "build123d-python",
        source_kind: input.sourceKind ?? "workspace-file",
        source_hash: input.sourceHash,
        status: input.status ?? "active",
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadSession(
    input: RecordDesignerCadSessionInput,
    session?: Session,
  ): Promise<DesignerCadSession> {
    const db = this.getDb(session);
    const workspace = await this.requireDesignerCadWorkspaceByExternalId(
      db,
      input.workspaceId,
    );
    const document = await this.requireDesignerCadDocumentByExternalId(
      db,
      input.documentId,
    );
    if (document.workspace_id !== input.workspaceId) {
      throw new Error(
        `designer cad document ${input.documentId} is not in workspace ${input.workspaceId}`,
      );
    }
    const cadSessionId = input.cadSessionId ?? randomUUID();
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadSessionByExternalId(
      db,
      cadSessionId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_sessions, existing.id, {
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        document_id: input.documentId,
        document_row_id: document.id,
        codex_session_id: input.codexSessionId,
        agent_run_id: input.agentRunId,
        status: input.status ?? existing.status,
        active_tool_session_id: input.activeToolSessionId,
        latest_projection_id: input.latestProjectionId,
        opened_by: input.openedBy,
        metadata_json: input.metadataJson,
        updated_at: now,
        closed_at: input.closedAt ? asDate(input.closedAt) : undefined,
      });
      return this.requireDesignerCadSessionByExternalId(db, cadSessionId);
    }

    return (db as any).insertDurable(
      app.designer_cad_sessions,
      {
        cad_session_id: cadSessionId,
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        document_id: input.documentId,
        document_row_id: document.id,
        codex_session_id: input.codexSessionId,
        agent_run_id: input.agentRunId,
        status: input.status ?? "active",
        active_tool_session_id: input.activeToolSessionId,
        latest_projection_id: input.latestProjectionId,
        opened_by: input.openedBy,
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
        closed_at: input.closedAt ? asDate(input.closedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadEvent(
    input: RecordDesignerCadEventInput,
    session?: Session,
  ): Promise<DesignerCadEvent> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const deterministicEventId =
      input.eventId ??
      (input.sourceEventId
        ? `${input.cadSessionId}:${input.sourceEventId}`
        : `${input.cadSessionId}:${input.sequence}`);
    const existingById = await db.one(
      app.designer_cad_events.where({ event_id: deterministicEventId }),
    );
    const existingBySequence = existingById
      ? null
      : await db.one(
          app.designer_cad_events.where({
            cad_session_id: input.cadSessionId,
            sequence: input.sequence,
          }),
        );
    const existing = existingById ?? existingBySequence;
    const effectiveEventId = existing?.event_id ?? deterministicEventId;

    if (existing) {
      await this.updateRow(db, app.designer_cad_events, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        event_kind: input.eventKind,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        tool_session_id: input.toolSessionId,
        operation_id: input.operationId,
        preview_id: input.previewId,
        source_event_id: input.sourceEventId,
        payload_json: input.payloadJson,
        occurred_at: input.occurredAt ? asDate(input.occurredAt) : undefined,
        observed_at: input.observedAt ? asDate(input.observedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_events.where({ event_id: effectiveEventId }),
        "designer cad event",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_events,
      {
        event_id: effectiveEventId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        event_kind: input.eventKind,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        tool_session_id: input.toolSessionId,
        operation_id: input.operationId,
        preview_id: input.previewId,
        source_event_id: input.sourceEventId,
        payload_json: input.payloadJson,
        occurred_at: asDate(input.occurredAt),
        observed_at: asDate(input.observedAt),
      },
      { tier: this.writeTier },
    );
  }

  async upsertDesignerCadSceneNode(
    input: UpsertDesignerCadSceneNodeInput,
    session?: Session,
  ): Promise<DesignerCadSceneNode> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const document = await this.requireDesignerCadDocumentByExternalId(
      db,
      input.documentId ?? cadSession.document_id,
    );
    if (document.document_id !== cadSession.document_id) {
      throw new Error(
        `designer cad scene node document ${document.document_id} does not match session document ${cadSession.document_id}`,
      );
    }
    const existing = await db.one(
      app.designer_cad_scene_nodes.where({
        cad_session_id: input.cadSessionId,
        node_id: input.nodeId,
      }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_scene_nodes, existing.id, {
        document_id: document.document_id,
        document_row_id: document.id,
        projection_id: input.projectionId,
        kind: input.kind,
        label: input.label,
        path: input.path,
        parent_node_id: input.parentNodeId,
        stable_ref: input.stableRef,
        visibility: input.visibility,
        source_span_json: input.sourceSpanJson,
        geometry_ref_json: input.geometryRefJson,
        metadata_json: input.metadataJson,
        updated_at: asDate(input.updatedAt),
      });
      return this.requireDesignerCadSceneNodeByExternalId(
        db,
        input.cadSessionId,
        input.nodeId,
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_scene_nodes,
      {
        node_id: input.nodeId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        document_id: document.document_id,
        document_row_id: document.id,
        projection_id: input.projectionId,
        kind: input.kind,
        label: input.label,
        path: input.path,
        parent_node_id: input.parentNodeId,
        stable_ref: input.stableRef,
        visibility: input.visibility,
        source_span_json: input.sourceSpanJson,
        geometry_ref_json: input.geometryRefJson,
        metadata_json: input.metadataJson,
        updated_at: asDate(input.updatedAt),
      },
      { tier: this.writeTier },
    );
  }

  async upsertDesignerCadSelection(
    input: UpsertDesignerCadSelectionInput,
    session?: Session,
  ): Promise<DesignerCadSelection> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const selectionId =
      input.selectionId ??
      `${input.cadSessionId}:${input.actorKind}:${input.actorId ?? "anonymous"}`;
    if (input.nodeId) {
      await this.requireDesignerCadSceneNodeByExternalId(
        db,
        input.cadSessionId,
        input.nodeId,
      );
    }
    const existing = await db.one(
      app.designer_cad_selections.where({ selection_id: selectionId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_selections, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        target_kind: input.targetKind,
        target_id: input.targetId,
        node_id: input.nodeId,
        selection_json: input.selectionJson,
        status: input.status ?? existing.status,
        updated_at: asDate(input.updatedAt),
      });
      return this.requireByQuery(
        db,
        app.designer_cad_selections.where({ selection_id: selectionId }),
        "designer cad selection",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_selections,
      {
        selection_id: selectionId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        target_kind: input.targetKind,
        target_id: input.targetId,
        node_id: input.nodeId,
        selection_json: input.selectionJson,
        status: input.status ?? "active",
        updated_at: asDate(input.updatedAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadToolSession(
    input: RecordDesignerCadToolSessionInput,
    session?: Session,
  ): Promise<DesignerCadToolSession> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const toolSessionId = input.toolSessionId ?? randomUUID();
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadToolSessionByExternalId(
      db,
      toolSessionId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_tool_sessions, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_kind: input.toolKind,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        status: input.status ?? existing.status,
        input_json: input.inputJson,
        state_json: input.stateJson,
        started_at: input.startedAt ? asDate(input.startedAt) : undefined,
        updated_at: now,
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      });
      return this.requireDesignerCadToolSessionByExternalId(db, toolSessionId);
    }

    return (db as any).insertDurable(
      app.designer_cad_tool_sessions,
      {
        tool_session_id: toolSessionId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_kind: input.toolKind,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        status: input.status ?? "active",
        input_json: input.inputJson,
        state_json: input.stateJson,
        started_at: asDate(input.startedAt),
        updated_at: now,
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadOperation(
    input: RecordDesignerCadOperationInput,
    session?: Session,
  ): Promise<DesignerCadOperation> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const toolSession = input.toolSessionId
      ? await this.requireDesignerCadToolSessionByExternalId(
          db,
          input.toolSessionId,
        )
      : null;
    if (toolSession && toolSession.cad_session_id !== input.cadSessionId) {
      throw new Error(
        `designer cad tool session ${toolSession.tool_session_id} is not in session ${input.cadSessionId}`,
      );
    }
    const operationId = input.operationId ?? randomUUID();
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadOperationByExternalId(
      db,
      operationId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_operations, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_session_id: input.toolSessionId,
        tool_session_row_id: toolSession?.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        operation_kind: input.operationKind,
        status: input.status ?? existing.status,
        operation_json: input.operationJson,
        validation_json: input.validationJson,
        result_json: input.resultJson,
        updated_at: now,
        applied_at: input.appliedAt ? asDate(input.appliedAt) : undefined,
      });
      return this.requireDesignerCadOperationByExternalId(db, operationId);
    }

    return (db as any).insertDurable(
      app.designer_cad_operations,
      {
        operation_id: operationId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_session_id: input.toolSessionId,
        tool_session_row_id: toolSession?.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        operation_kind: input.operationKind,
        status: input.status ?? "queued",
        operation_json: input.operationJson,
        validation_json: input.validationJson,
        result_json: input.resultJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
        applied_at: input.appliedAt ? asDate(input.appliedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadSourceEdit(
    input: RecordDesignerCadSourceEditInput,
    session?: Session,
  ): Promise<DesignerCadSourceEdit> {
    const db = this.getDb(session);
    const operation = await this.requireDesignerCadOperationByExternalId(
      db,
      input.operationId,
    );
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      operation.cad_session_id,
    );
    const editId = input.editId ?? randomUUID();
    const existing = await db.one(
      app.designer_cad_source_edits.where({ edit_id: editId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_source_edits, existing.id, {
        operation_id: input.operationId,
        operation_row_id: operation.id,
        cad_session_id: operation.cad_session_id,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        file_path: input.filePath,
        range_json: input.rangeJson,
        text_preview: input.textPreview,
        text_sha256: input.textSha256,
        status: input.status ?? existing.status,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_source_edits.where({ edit_id: editId }),
        "designer cad source edit",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_source_edits,
      {
        edit_id: editId,
        operation_id: input.operationId,
        operation_row_id: operation.id,
        cad_session_id: operation.cad_session_id,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        file_path: input.filePath,
        range_json: input.rangeJson,
        text_preview: input.textPreview,
        text_sha256: input.textSha256,
        status: input.status ?? "planned",
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadPreviewHandle(
    input: RecordDesignerCadPreviewHandleInput,
    session?: Session,
  ): Promise<DesignerCadPreviewHandle> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const toolSession = input.toolSessionId
      ? await this.requireDesignerCadToolSessionByExternalId(
          db,
          input.toolSessionId,
        )
      : null;
    const operation = input.operationId
      ? await this.requireDesignerCadOperationByExternalId(
          db,
          input.operationId,
        )
      : null;
    if (toolSession && toolSession.cad_session_id !== input.cadSessionId) {
      throw new Error(
        `designer cad tool session ${toolSession.tool_session_id} is not in session ${input.cadSessionId}`,
      );
    }
    if (operation && operation.cad_session_id !== input.cadSessionId) {
      throw new Error(
        `designer cad operation ${operation.operation_id} is not in session ${input.cadSessionId}`,
      );
    }
    const previewId = input.previewId ?? randomUUID();
    const now = asDate(input.updatedAt);
    const existing = await this.getDesignerCadPreviewHandleByExternalId(
      db,
      previewId,
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_preview_handles, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_session_id: input.toolSessionId,
        tool_session_row_id: toolSession?.id,
        operation_id: input.operationId,
        operation_row_id: operation?.id,
        preview_kind: input.previewKind,
        target_json: input.targetJson,
        status: input.status ?? existing.status,
        handle_ref: input.handleRef,
        updated_at: now,
        disposed_at: input.disposedAt ? asDate(input.disposedAt) : undefined,
      });
      return this.requireDesignerCadPreviewHandleByExternalId(db, previewId);
    }

    return (db as any).insertDurable(
      app.designer_cad_preview_handles,
      {
        preview_id: previewId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        tool_session_id: input.toolSessionId,
        tool_session_row_id: toolSession?.id,
        operation_id: input.operationId,
        operation_row_id: operation?.id,
        preview_kind: input.previewKind,
        target_json: input.targetJson,
        status: input.status ?? "prepared",
        handle_ref: input.handleRef,
        created_at: asDate(input.createdAt),
        updated_at: now,
        disposed_at: input.disposedAt ? asDate(input.disposedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadPreviewUpdate(
    input: RecordDesignerCadPreviewUpdateInput,
    session?: Session,
  ): Promise<DesignerCadPreviewUpdate> {
    const db = this.getDb(session);
    const preview = await this.requireDesignerCadPreviewHandleByExternalId(
      db,
      input.previewId,
    );
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      preview.cad_session_id,
    );
    const updateId = input.updateId ?? randomUUID();
    const existing = await db.one(
      app.designer_cad_preview_updates.where({ update_id: updateId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_preview_updates, existing.id, {
        preview_id: input.previewId,
        preview_row_id: preview.id,
        cad_session_id: preview.cad_session_id,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        params_json: input.paramsJson,
        mesh_ref_json: input.meshRefJson,
        status: input.status ?? existing.status,
        error_text: input.errorText,
        requested_at: input.requestedAt ? asDate(input.requestedAt) : undefined,
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_preview_updates.where({ update_id: updateId }),
        "designer cad preview update",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_preview_updates,
      {
        update_id: updateId,
        preview_id: input.previewId,
        preview_row_id: preview.id,
        cad_session_id: preview.cad_session_id,
        cad_session_row_id: cadSession.id,
        sequence: input.sequence,
        params_json: input.paramsJson,
        mesh_ref_json: input.meshRefJson,
        status: input.status ?? "completed",
        error_text: input.errorText,
        requested_at: asDate(input.requestedAt),
        completed_at: input.completedAt ? asDate(input.completedAt) : undefined,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadWidget(
    input: RecordDesignerCadWidgetInput,
    session?: Session,
  ): Promise<DesignerCadWidget> {
    const db = this.getDb(session);
    const workspace = await this.requireDesignerCadWorkspaceByExternalId(
      db,
      input.workspaceId,
    );
    const widgetId = input.widgetId ?? `${input.workspaceId}:${input.widgetKey}`;
    const now = asDate(input.updatedAt);
    const existing = await db.one(
      app.designer_cad_widgets.where({ widget_id: widgetId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_widgets, existing.id, {
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        widget_key: input.widgetKey,
        title: input.title,
        source_kind: input.sourceKind ?? existing.source_kind,
        source_path: input.sourcePath,
        version: input.version,
        status: input.status ?? existing.status,
        manifest_json: input.manifestJson,
        state_json: input.stateJson,
        updated_at: now,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_widgets.where({ widget_id: widgetId }),
        "designer cad widget",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_widgets,
      {
        widget_id: widgetId,
        workspace_id: input.workspaceId,
        workspace_row_id: workspace.id,
        widget_key: input.widgetKey,
        title: input.title,
        source_kind: input.sourceKind ?? "designer-widget",
        source_path: input.sourcePath,
        version: input.version,
        status: input.status ?? "active",
        manifest_json: input.manifestJson,
        state_json: input.stateJson,
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async recordDesignerCadSteer(
    input: RecordDesignerCadSteerInput,
    session?: Session,
  ): Promise<DesignerCadSteer> {
    const db = this.getDb(session);
    const cadSession = await this.requireDesignerCadSessionByExternalId(
      db,
      input.cadSessionId,
    );
    const steerId = input.steerId ?? randomUUID();
    const existing = await db.one(
      app.designer_cad_steers.where({ steer_id: steerId }),
    );

    if (existing) {
      await this.updateRow(db, app.designer_cad_steers, existing.id, {
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        target_agent_id: input.targetAgentId,
        target_run_id: input.targetRunId,
        message_text: input.messageText,
        context_json: input.contextJson,
        status: input.status ?? existing.status,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.designer_cad_steers.where({ steer_id: steerId }),
        "designer cad steer",
      );
    }

    return (db as any).insertDurable(
      app.designer_cad_steers,
      {
        steer_id: steerId,
        cad_session_id: input.cadSessionId,
        cad_session_row_id: cadSession.id,
        actor_kind: input.actorKind,
        actor_id: input.actorId,
        target_agent_id: input.targetAgentId,
        target_run_id: input.targetRunId,
        message_text: input.messageText,
        context_json: input.contextJson,
        status: input.status ?? "queued",
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async listDesignerCadEvents(
    input: ListDesignerCadEventsInput = {},
    session?: Session,
  ): Promise<DesignerCadEvent[]> {
    const db = this.getDb(session);
    const rows = input.cadSessionId
      ? await db.all(
          app.designer_cad_events
            .where({ cad_session_id: input.cadSessionId })
            .orderBy("sequence", "asc"),
        )
      : await db.all(
          app.designer_cad_events
            .orderBy("sequence", "asc")
            .limit(Math.max(clampLimit(input.limit), 50) * 8),
        );

    return rows
      .filter((event) => {
        if (input.cadSessionId && event.cad_session_id !== input.cadSessionId)
          return false;
        if (input.eventKind && event.event_kind !== input.eventKind)
          return false;
        if (
          input.afterSequence !== undefined &&
          event.sequence <= input.afterSequence
        ) {
          return false;
        }
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async listDesignerCadOperations(
    input: ListDesignerCadOperationsInput = {},
    session?: Session,
  ): Promise<DesignerCadOperation[]> {
    const db = this.getDb(session);
    const rows = input.cadSessionId
      ? await db.all(
          app.designer_cad_operations
            .where({ cad_session_id: input.cadSessionId })
            .orderBy("updated_at", "desc"),
        )
      : await db.all(
          app.designer_cad_operations
            .orderBy("updated_at", "desc")
            .limit(Math.max(clampLimit(input.limit), 50) * 4),
        );

    return rows
      .filter((operation) => {
        if (
          input.cadSessionId &&
          operation.cad_session_id !== input.cadSessionId
        ) {
          return false;
        }
        if (
          input.toolSessionId &&
          operation.tool_session_id !== input.toolSessionId
        ) {
          return false;
        }
        if (input.status && operation.status !== input.status) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async getDesignerCadSessionSummary(
    cadSessionId: string,
    session?: Session,
  ): Promise<DesignerCadSessionSummary | null> {
    const db = this.getDb(session);
    const cadSession = await this.getDesignerCadSessionByExternalId(
      db,
      cadSessionId,
    );
    if (!cadSession) return null;

    const [workspace, document] = await Promise.all([
      this.requireDesignerCadWorkspaceByExternalId(db, cadSession.workspace_id),
      this.requireDesignerCadDocumentByExternalId(db, cadSession.document_id),
    ]);
    const [
      events,
      sceneNodes,
      selections,
      toolSessions,
      operations,
      sourceEdits,
      previewHandles,
      previewUpdates,
      widgets,
      steers,
    ] = await Promise.all([
      db.all(
        app.designer_cad_events
          .where({ cad_session_id: cadSessionId })
          .orderBy("sequence", "asc"),
      ),
      db.all(
        app.designer_cad_scene_nodes
          .where({ cad_session_id: cadSessionId })
          .orderBy("updated_at", "asc"),
      ),
      db.all(
        app.designer_cad_selections
          .where({ cad_session_id: cadSessionId })
          .orderBy("updated_at", "desc"),
      ),
      db.all(
        app.designer_cad_tool_sessions
          .where({ cad_session_id: cadSessionId })
          .orderBy("started_at", "asc"),
      ),
      db.all(
        app.designer_cad_operations
          .where({ cad_session_id: cadSessionId })
          .orderBy("created_at", "asc"),
      ),
      db.all(
        app.designer_cad_source_edits
          .where({ cad_session_id: cadSessionId })
          .orderBy("sequence", "asc"),
      ),
      db.all(
        app.designer_cad_preview_handles
          .where({ cad_session_id: cadSessionId })
          .orderBy("created_at", "asc"),
      ),
      db.all(
        app.designer_cad_preview_updates
          .where({ cad_session_id: cadSessionId })
          .orderBy("sequence", "asc"),
      ),
      db.all(
        app.designer_cad_widgets
          .where({ workspace_id: cadSession.workspace_id })
          .orderBy("updated_at", "desc"),
      ),
      db.all(
        app.designer_cad_steers
          .where({ cad_session_id: cadSessionId })
          .orderBy("created_at", "asc"),
      ),
    ]);

    return {
      workspace,
      document,
      session: cadSession,
      events,
      sceneNodes,
      selections,
      toolSessions,
      operations,
      sourceEdits,
      previewHandles,
      previewUpdates,
      widgets,
      steers,
    };
  }

  async recordDaemonLogSource(
    input: RecordDaemonLogSourceInput,
    session?: Session,
  ): Promise<DaemonLogSource> {
    const db = this.getDb(session);
    const sourceId = input.sourceId ?? randomUUID();
    const now = asDate(input.updatedAt);
    const existing = await this.getDaemonLogSourceByExternalId(db, sourceId);

    if (existing) {
      await this.updateRow(db, app.daemon_log_sources, existing.id, {
        manager: input.manager,
        daemon_name: input.daemonName,
        stream: input.stream,
        host_id: input.hostId,
        log_path: input.logPath,
        config_path: input.configPath,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        owner_agent: input.ownerAgent,
        flow_daemon_name: input.flowDaemonName,
        launchd_label: input.launchdLabel,
        retention_class: input.retentionClass ?? existing.retention_class,
        status: input.status ?? existing.status,
        updated_at: now,
      });
      return this.requireByQuery(
        db,
        app.daemon_log_sources.where({ source_id: sourceId }),
        "daemon log source",
      );
    }

    return (db as any).insertDurable(
      app.daemon_log_sources,
      {
        source_id: sourceId,
        manager: input.manager,
        daemon_name: input.daemonName,
        stream: input.stream,
        host_id: input.hostId,
        log_path: input.logPath,
        config_path: input.configPath,
        repo_root: input.repoRoot,
        workspace_root: input.workspaceRoot,
        owner_agent: input.ownerAgent,
        flow_daemon_name: input.flowDaemonName,
        launchd_label: input.launchdLabel,
        retention_class: input.retentionClass ?? "normal",
        status: input.status ?? "active",
        created_at: asDate(input.createdAt),
        updated_at: now,
      },
      { tier: this.writeTier },
    );
  }

  async listDaemonLogSources(
    input: ListDaemonLogSourcesInput = {},
    session?: Session,
  ): Promise<DaemonLogSource[]> {
    const rows = await this.getDb(session).all(
      app.daemon_log_sources
        .orderBy("updated_at", "desc")
        .limit(Math.max(clampLimit(input.limit), 50) * 4),
    );

    return rows
      .filter((row) => {
        if (input.manager && row.manager !== input.manager) return false;
        if (input.daemonName && row.daemon_name !== input.daemonName)
          return false;
        if (input.stream && row.stream !== input.stream) return false;
        if (input.status && row.status !== input.status) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async recordDaemonLogChunk(
    input: RecordDaemonLogChunkInput,
    session?: Session,
  ): Promise<DaemonLogChunk> {
    const db = this.getDb(session);
    const source = await this.requireDaemonLogSourceByExternalId(
      db,
      input.sourceId,
    );
    const chunkId = input.chunkId ?? randomUUID();
    const existing = await this.getDaemonLogChunkByExternalId(db, chunkId);

    if (existing) {
      await this.updateRow(db, app.daemon_log_chunks, existing.id, {
        source_id: input.sourceId,
        source_row_id: source.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        stream: input.stream ?? source.stream,
        host_id: input.hostId ?? source.host_id,
        log_path: input.logPath ?? source.log_path,
        file_fingerprint: input.fileFingerprint,
        start_offset: input.startOffset,
        end_offset: input.endOffset,
        first_line_no: input.firstLineNo,
        last_line_no: input.lastLineNo,
        line_count: input.lineCount,
        byte_count: input.byteCount,
        first_at: input.firstAt ? asDate(input.firstAt) : undefined,
        last_at: input.lastAt ? asDate(input.lastAt) : undefined,
        sha256: input.sha256,
        body_ref: input.bodyRef,
        body_preview: input.bodyPreview,
        compression: input.compression ?? existing.compression,
        ingested_at: input.ingestedAt ? asDate(input.ingestedAt) : undefined,
      });
      return this.requireDaemonLogChunkByExternalId(db, chunkId);
    }

    return (db as any).insertDurable(
      app.daemon_log_chunks,
      {
        chunk_id: chunkId,
        source_id: input.sourceId,
        source_row_id: source.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        stream: input.stream ?? source.stream,
        host_id: input.hostId ?? source.host_id,
        log_path: input.logPath ?? source.log_path,
        file_fingerprint: input.fileFingerprint,
        start_offset: input.startOffset,
        end_offset: input.endOffset,
        first_line_no: input.firstLineNo,
        last_line_no: input.lastLineNo,
        line_count: input.lineCount,
        byte_count: input.byteCount,
        first_at: input.firstAt ? asDate(input.firstAt) : undefined,
        last_at: input.lastAt ? asDate(input.lastAt) : undefined,
        sha256: input.sha256,
        body_ref: input.bodyRef,
        body_preview: input.bodyPreview,
        compression: input.compression ?? "none",
        ingested_at: asDate(input.ingestedAt),
      },
      { tier: this.writeTier },
    );
  }

  async getDaemonLogChunk(
    chunkId: string,
    session?: Session,
  ): Promise<DaemonLogChunk | null> {
    return this.getDaemonLogChunkByExternalId(this.getDb(session), chunkId);
  }

  async recordDaemonLogEvent(
    input: RecordDaemonLogEventInput,
    session?: Session,
  ): Promise<DaemonLogEvent> {
    const db = this.getDb(session);
    const source = await this.requireDaemonLogSourceByExternalId(
      db,
      input.sourceId,
    );
    const chunk = await this.requireDaemonLogChunkByExternalId(
      db,
      input.chunkId,
    );
    const eventId = input.eventId ?? randomUUID();
    const existing = await db.one(
      app.daemon_log_events.where({ event_id: eventId }),
    );

    if (existing) {
      await this.updateRow(db, app.daemon_log_events, existing.id, {
        source_id: input.sourceId,
        source_row_id: source.id,
        chunk_id: input.chunkId,
        chunk_row_id: chunk.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        stream: input.stream ?? source.stream,
        seq: input.seq,
        line_no: input.lineNo,
        at: input.at ? asDate(input.at) : undefined,
        level: input.level ?? existing.level,
        message: input.message,
        fields_json: input.fieldsJson,
        repo_root: input.repoRoot ?? source.repo_root,
        workspace_root: input.workspaceRoot ?? source.workspace_root,
        conversation: input.conversation,
        conversation_hash: input.conversationHash,
        run_id: input.runId,
        job_id: input.jobId,
        trace_id: input.traceId,
        span_id: input.spanId,
        error_kind: input.errorKind,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.daemon_log_events.where({ event_id: eventId }),
        "daemon log event",
      );
    }

    return (db as any).insertDurable(
      app.daemon_log_events,
      {
        event_id: eventId,
        source_id: input.sourceId,
        source_row_id: source.id,
        chunk_id: input.chunkId,
        chunk_row_id: chunk.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        stream: input.stream ?? source.stream,
        seq: input.seq,
        line_no: input.lineNo,
        at: input.at ? asDate(input.at) : undefined,
        level: input.level ?? "unknown",
        message: input.message,
        fields_json: input.fieldsJson,
        repo_root: input.repoRoot ?? source.repo_root,
        workspace_root: input.workspaceRoot ?? source.workspace_root,
        conversation: input.conversation,
        conversation_hash: input.conversationHash,
        run_id: input.runId,
        job_id: input.jobId,
        trace_id: input.traceId,
        span_id: input.spanId,
        error_kind: input.errorKind,
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async listDaemonLogEvents(
    input: ListDaemonLogEventsInput = {},
    session?: Session,
  ): Promise<DaemonLogEvent[]> {
    const rows = await this.getDb(session).all(
      app.daemon_log_events
        .orderBy("created_at", "desc")
        .limit(Math.max(clampLimit(input.limit), 50) * 8),
    );
    const since = input.since ? asDate(input.since) : undefined;

    return rows
      .filter((row) => {
        if (input.sourceId && row.source_id !== input.sourceId) return false;
        if (input.daemonName && row.daemon_name !== input.daemonName)
          return false;
        if (input.level && row.level !== input.level) return false;
        if (input.conversation && row.conversation !== input.conversation)
          return false;
        if (
          input.conversationHash &&
          row.conversation_hash !== input.conversationHash
        ) {
          return false;
        }
        if (input.runId && row.run_id !== input.runId) return false;
        if (input.jobId && row.job_id !== input.jobId) return false;
        if (input.traceId && row.trace_id !== input.traceId) return false;
        if (since) {
          const eventTime = row.at ?? row.created_at;
          if (eventTime.getTime() < since.getTime()) return false;
        }
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async recordDaemonLogCheckpoint(
    input: RecordDaemonLogCheckpointInput,
    session?: Session,
  ): Promise<DaemonLogCheckpoint> {
    const db = this.getDb(session);
    const source = await this.requireDaemonLogSourceByExternalId(
      db,
      input.sourceId,
    );
    const checkpointId = input.checkpointId ?? input.sourceId;
    const existing = await db.one(
      app.daemon_log_checkpoints.where({ checkpoint_id: checkpointId }),
    );

    if (existing) {
      await this.updateRow(db, app.daemon_log_checkpoints, existing.id, {
        source_id: input.sourceId,
        source_row_id: source.id,
        host_id: input.hostId ?? source.host_id,
        log_path: input.logPath ?? source.log_path,
        file_fingerprint: input.fileFingerprint,
        inode: input.inode,
        device: input.device,
        offset: input.offset,
        line_no: input.lineNo,
        last_chunk_id: input.lastChunkId,
        last_event_id: input.lastEventId,
        last_seen_at: input.lastSeenAt
          ? asDate(input.lastSeenAt)
          : undefined,
        updated_at: asDate(input.updatedAt),
      });
      return this.requireByQuery(
        db,
        app.daemon_log_checkpoints.where({ checkpoint_id: checkpointId }),
        "daemon log checkpoint",
      );
    }

    return (db as any).insertDurable(
      app.daemon_log_checkpoints,
      {
        checkpoint_id: checkpointId,
        source_id: input.sourceId,
        source_row_id: source.id,
        host_id: input.hostId ?? source.host_id,
        log_path: input.logPath ?? source.log_path,
        file_fingerprint: input.fileFingerprint,
        inode: input.inode,
        device: input.device,
        offset: input.offset,
        line_no: input.lineNo,
        last_chunk_id: input.lastChunkId,
        last_event_id: input.lastEventId,
        last_seen_at: input.lastSeenAt ? asDate(input.lastSeenAt) : undefined,
        updated_at: asDate(input.updatedAt),
      },
      { tier: this.writeTier },
    );
  }

  async recordDaemonLogSummary(
    input: RecordDaemonLogSummaryInput,
    session?: Session,
  ): Promise<DaemonLogSummary> {
    const db = this.getDb(session);
    const source = await this.requireDaemonLogSourceByExternalId(
      db,
      input.sourceId,
    );
    const summaryId = input.summaryId ?? randomUUID();
    const existing = await db.one(
      app.daemon_log_summaries.where({ summary_id: summaryId }),
    );

    if (existing) {
      await this.updateRow(db, app.daemon_log_summaries, existing.id, {
        source_id: input.sourceId,
        source_row_id: source.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        window_start: asDate(input.windowStart),
        window_end: asDate(input.windowEnd),
        level_counts_json: input.levelCountsJson,
        error_count: input.errorCount,
        warning_count: input.warningCount,
        first_error_event_id: input.firstErrorEventId,
        last_error_event_id: input.lastErrorEventId,
        top_error_kinds_json: input.topErrorKindsJson,
        summary_text: input.summaryText,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(
        db,
        app.daemon_log_summaries.where({ summary_id: summaryId }),
        "daemon log summary",
      );
    }

    return (db as any).insertDurable(
      app.daemon_log_summaries,
      {
        summary_id: summaryId,
        source_id: input.sourceId,
        source_row_id: source.id,
        daemon_name: input.daemonName ?? source.daemon_name,
        window_start: asDate(input.windowStart),
        window_end: asDate(input.windowEnd),
        level_counts_json: input.levelCountsJson,
        error_count: input.errorCount,
        warning_count: input.warningCount,
        first_error_event_id: input.firstErrorEventId,
        last_error_event_id: input.lastErrorEventId,
        top_error_kinds_json: input.topErrorKindsJson,
        summary_text: input.summaryText,
        created_at: asDate(input.createdAt),
      },
      { tier: this.writeTier },
    );
  }

  async listDaemonLogSummaries(
    input: ListDaemonLogSummariesInput = {},
    session?: Session,
  ): Promise<DaemonLogSummary[]> {
    const rows = await this.getDb(session).all(
      app.daemon_log_summaries
        .orderBy("window_end", "desc")
        .limit(Math.max(clampLimit(input.limit), 50) * 4),
    );
    const since = input.since ? asDate(input.since) : undefined;

    return rows
      .filter((row) => {
        if (input.sourceId && row.source_id !== input.sourceId) return false;
        if (input.daemonName && row.daemon_name !== input.daemonName)
          return false;
        if (since && row.window_end.getTime() < since.getTime()) return false;
        return true;
      })
      .slice(0, clampLimit(input.limit));
  }

  async upsertTaskRecord(
    input: UpsertTaskRecordInput,
    session?: Session,
  ): Promise<TaskRecord> {
    const db = this.getDb(session);
    const existing = await this.getTaskByExternalId(db, input.taskId);
    const updatedAt = asDate(input.updatedAt);

    if (existing) {
      await this.updateRow(db, app.task_records, existing.id, {
        context: input.context,
        title: input.title,
        status: input.status,
        priority: input.priority,
        placement: input.placement,
        focus_rank: input.focusRank,
        project: input.project,
        issue: input.issue,
        branch: input.branch,
        workspace: input.workspace,
        plan: input.plan,
        pr: input.pr,
        tags_json: input.tagsJson,
        next_text: input.nextText,
        context_text: input.contextText,
        notes_text: input.notesText,
        annotations_json: input.annotationsJson,
        source_kind: input.sourceKind,
        source_path: input.sourcePath,
        metadata_json: input.metadataJson,
        updated_at: updatedAt,
      });
      return this.requireByQuery(
        db,
        app.task_records.where({ task_id: input.taskId }),
        "task record",
      );
    }

    return (db as any).insertDurable(
      app.task_records,
      {
        task_id: input.taskId,
        context: input.context,
        title: input.title,
        status: input.status,
        priority: input.priority,
        placement: input.placement,
        focus_rank: input.focusRank,
        project: input.project,
        issue: input.issue,
        branch: input.branch,
        workspace: input.workspace,
        plan: input.plan,
        pr: input.pr,
        tags_json: input.tagsJson,
        next_text: input.nextText,
        context_text: input.contextText,
        notes_text: input.notesText,
        annotations_json: input.annotationsJson,
        source_kind: input.sourceKind,
        source_path: input.sourcePath,
        metadata_json: input.metadataJson,
        created_at: asDate(input.createdAt),
        updated_at: updatedAt,
      },
      { tier: this.writeTier },
    );
  }

  async getTaskRecord(
    taskId: string,
    session?: Session,
  ): Promise<TaskRecord | null> {
    return this.getTaskByExternalId(this.getDb(session), taskId);
  }

  async listTaskRecords(
    input: ListTaskRecordsInput = {},
    session?: Session,
  ): Promise<TaskRecord[]> {
    const db = this.getDb(session);
    const rawRecords = input.context
      ? await db.all(
          app.task_records
            .where({ context: input.context })
            .orderBy("updated_at", "desc"),
        )
      : await db.all(app.task_records.orderBy("updated_at", "desc"));

    const statuses = input.statuses?.map((value) => value.toLowerCase());
    const priorities = input.priorities?.map((value) => value.toUpperCase());
    const placements = input.placements?.map((value) => value.toLowerCase());

    return rawRecords
      .filter((record) => {
        if (
          statuses &&
          statuses.length > 0 &&
          !statuses.includes(record.status.toLowerCase())
        ) {
          return false;
        }
        if (
          priorities &&
          priorities.length > 0 &&
          !priorities.includes(record.priority.toUpperCase())
        ) {
          return false;
        }
        if (
          placements &&
          placements.length > 0 &&
          !placements.includes(record.placement.toLowerCase())
        ) {
          return false;
        }
        return true;
      })
      .sort(compareTaskRecords)
      .slice(0, clampLimit(input.limit));
  }

  async recordCursorReviewOperation(
    input: RecordCursorReviewOperationInput,
    session?: Session,
  ): Promise<CursorReviewOperationRecord> {
    const db = this.getDb(session);
    await this.ensureCursorReviewControlRun(db);
    const operationId = input.operationId ?? randomUUID();
    const event = await this.appendSemanticEvent(
      {
        runId: CURSOR_REVIEW_CONTROL_RUN_ID,
        eventId: operationId,
        eventType: CURSOR_REVIEW_OPERATION_EVENT_TYPE,
        summaryText: input.note ?? input.operationType,
        payloadJson: pruneUndefined({
          operationId,
          operationType: input.operationType,
          repoRoot: input.repoRoot,
          workspaceRoot: input.workspaceRoot,
          bookmark: input.bookmark,
          relPath: input.relPath,
          note: input.note,
          sourceSessionId: input.sourceSessionId,
          sourceChatKind: input.sourceChatKind,
        }) as JsonValue,
        occurredAt: input.createdAt,
      },
      session,
    );
    const operation = this.cursorReviewOperationFromEvent(event);
    if (!operation) {
      throw new Error("cursor review operation event could not be decoded");
    }
    return operation;
  }

  async recordCursorReviewResult(
    input: RecordCursorReviewResultInput,
    session?: Session,
  ): Promise<CursorReviewOperationResultRecord> {
    const db = this.getDb(session);
    await this.ensureCursorReviewControlRun(db);
    const event = await this.appendSemanticEvent(
      {
        runId: CURSOR_REVIEW_CONTROL_RUN_ID,
        eventType: CURSOR_REVIEW_RESULT_EVENT_TYPE,
        summaryText: input.message ?? input.status,
        payloadJson: pruneUndefined({
          operationId: input.operationId,
          status: input.status,
          clientId: input.clientId,
          repoRoot: input.repoRoot,
          message: input.message,
        }) as JsonValue,
        occurredAt: input.processedAt,
      },
      session,
    );
    const result = this.cursorReviewResultFromEvent(event);
    if (!result) {
      throw new Error(`cursor review result ${event.event_id} failed to parse`);
    }
    return result;
  }

  async listCursorReviewOperations(
    input: ListCursorReviewOperationsInput = {},
    session?: Session,
  ): Promise<CursorReviewOperationRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: CURSOR_REVIEW_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 8),
    );

    const latestResults = new Map<string, CursorReviewOperationResultRecord>();
    for (const row of rows) {
      if (row.event_type !== CURSOR_REVIEW_RESULT_EVENT_TYPE) continue;
      const result = this.cursorReviewResultFromEvent(row);
      if (!result) continue;
      const existing = latestResults.get(result.operationId);
      if (
        !existing ||
        existing.processedAt.getTime() < result.processedAt.getTime()
      ) {
        latestResults.set(result.operationId, result);
      }
    }

    const operations = rows
      .filter((row) => row.event_type === CURSOR_REVIEW_OPERATION_EVENT_TYPE)
      .map((row) => this.cursorReviewOperationFromEvent(row))
      .filter((row): row is CursorReviewOperationRecord => Boolean(row))
      .filter((row) => {
        if (input.repoRoot && row.repoRoot && row.repoRoot !== input.repoRoot)
          return false;
        if (
          input.workspaceRoot &&
          row.workspaceRoot &&
          row.workspaceRoot !== input.workspaceRoot
        )
          return false;
        const latestResult = latestResults.get(row.operationId);
        if (
          !input.includeProcessed &&
          latestResult &&
          TERMINAL_CURSOR_REVIEW_RESULT_STATUSES.has(latestResult.status)
        ) {
          return false;
        }
        row.latestResult = latestResult;
        return true;
      })
      .sort(
        (left, right) => left.createdAt.getTime() - right.createdAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));

    return operations;
  }

  async recordBranchFileReviewState(
    input: RecordBranchFileReviewStateInput,
    session?: Session,
  ): Promise<BranchFileReviewStateRecord> {
    const db = this.getDb(session);
    await this.ensureBranchFileReviewControlRun(db);
    const event = await this.appendSemanticEvent(
      {
        runId: BRANCH_FILE_REVIEW_CONTROL_RUN_ID,
        eventId: input.eventId,
        eventType: BRANCH_FILE_REVIEW_STATE_EVENT_TYPE,
        summaryText: input.note ?? `${input.status} ${input.relPath}`,
        payloadJson: pruneUndefined({
          repoRoot: input.repoRoot,
          workspaceRoot: input.workspaceRoot,
          bookmark: input.bookmark,
          relPath: input.relPath,
          status: input.status,
          note: input.note,
          sourceSessionId: input.sourceSessionId,
          sourceChatKind: input.sourceChatKind,
        }) as JsonValue,
        occurredAt: input.createdAt,
      },
      session,
    );
    const state = this.branchFileReviewStateFromEvent(event);
    if (!state) {
      throw new Error("branch file review state event could not be decoded");
    }
    return state;
  }

  async listBranchFileReviewStates(
    input: ListBranchFileReviewStatesInput = {},
    session?: Session,
  ): Promise<BranchFileReviewStateRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: BRANCH_FILE_REVIEW_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 12),
    );

    const latestStates = new Map<string, BranchFileReviewStateRecord>();
    for (const row of rows) {
      if (row.event_type !== BRANCH_FILE_REVIEW_STATE_EVENT_TYPE) continue;
      const state = this.branchFileReviewStateFromEvent(row);
      if (!state) continue;
      if (input.repoRoot && state.repoRoot !== input.repoRoot) continue;
      if (input.workspaceRoot && state.workspaceRoot !== input.workspaceRoot)
        continue;
      if (input.bookmark && state.bookmark !== input.bookmark) continue;
      if (input.relPath && state.relPath !== input.relPath) continue;
      const key = `${state.repoRoot}\n${state.bookmark}\n${state.relPath}`;
      if (!latestStates.has(key)) {
        latestStates.set(key, state);
      }
    }

    return Array.from(latestStates.values())
      .filter((state) => input.includeCleared || state.status !== "cleared")
      .sort(
        (left, right) => right.createdAt.getTime() - left.createdAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));
  }

  async recordCommitTurnOperation(
    input: RecordCommitTurnOperationInput,
    session?: Session,
  ): Promise<CommitTurnOperationRecord> {
    const db = this.getDb(session);
    await this.ensureCommitTurnControlRun(db);
    const operationId = input.operationId ?? randomUUID();
    const event = await this.appendSemanticEvent(
      {
        runId: COMMIT_TURN_CONTROL_RUN_ID,
        eventId: operationId,
        eventType: COMMIT_TURN_OPERATION_EVENT_TYPE,
        summaryText: input.promptPreview ?? input.conversationHash,
        payloadJson: pruneUndefined({
          operationId,
          provider: input.provider,
          sessionId: input.sessionId,
          conversation: input.conversation,
          conversationHash: input.conversationHash,
          trigger: input.trigger,
          turnOrdinal: input.turnOrdinal,
          sessionEventId: input.sessionEventId,
          repoRoot: input.repoRoot,
          repoRoots: input.repoRoots,
          cwd: input.cwd,
          artifactPath: input.artifactPath,
          promptPreview: input.promptPreview,
          sourceChatKind: input.sourceChatKind,
        }) as JsonValue,
        occurredAt: input.createdAt,
      },
      session,
    );
    const operation = this.commitTurnOperationFromEvent(event);
    if (!operation) {
      throw new Error("commit turn operation event could not be decoded");
    }
    return operation;
  }

  async recordCommitTurnResult(
    input: RecordCommitTurnResultInput,
    session?: Session,
  ): Promise<CommitTurnResultRecord> {
    const db = this.getDb(session);
    await this.ensureCommitTurnControlRun(db);
    const event = await this.appendSemanticEvent(
      {
        runId: COMMIT_TURN_CONTROL_RUN_ID,
        eventType: COMMIT_TURN_RESULT_EVENT_TYPE,
        summaryText: input.message ?? input.commitMessage ?? input.status,
        payloadJson: pruneUndefined({
          operationId: input.operationId,
          status: input.status,
          agentId: input.agentId,
          runId: input.runId,
          threadId: input.threadId,
          repoRoot: input.repoRoot,
          model: input.model,
          effort: input.effort,
          traceRef: input.traceRef,
          message: input.message,
          classification: input.classification,
          title: input.title,
          description: input.description,
          commitMessage: input.commitMessage,
          todoItems: input.todoItems,
          notes: input.notes,
          group: input.group,
          groupReason: input.groupReason,
          groupIsNew: input.groupIsNew,
          snapshotCommitId: input.snapshotCommitId,
          reviewJobId: input.reviewJobId,
          conversationHash: input.conversationHash,
        }) as JsonValue,
        occurredAt: input.processedAt,
      },
      session,
    );
    const result = this.commitTurnResultFromEvent(event);
    if (!result) {
      throw new Error(`commit turn result ${event.event_id} failed to parse`);
    }
    return result;
  }

  async listCommitTurnOperations(
    input: ListCommitTurnOperationsInput = {},
    session?: Session,
  ): Promise<CommitTurnOperationRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: COMMIT_TURN_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 8),
    );

    const latestResults = new Map<string, CommitTurnResultRecord>();
    for (const row of rows) {
      if (row.event_type !== COMMIT_TURN_RESULT_EVENT_TYPE) continue;
      const result = this.commitTurnResultFromEvent(row);
      if (!result) continue;
      const existing = latestResults.get(result.operationId);
      if (
        !existing ||
        existing.processedAt.getTime() < result.processedAt.getTime()
      ) {
        latestResults.set(result.operationId, result);
      }
    }

    const operations = rows
      .filter((row) => row.event_type === COMMIT_TURN_OPERATION_EVENT_TYPE)
      .map((row) => this.commitTurnOperationFromEvent(row))
      .filter((row): row is CommitTurnOperationRecord => Boolean(row))
      .filter((row) => {
        if (input.repoRoot) {
          const matchesPrimary = row.repoRoot === input.repoRoot;
          const matchesAny = row.repoRoots?.includes(input.repoRoot) ?? false;
          if (!matchesPrimary && !matchesAny) return false;
        }
        if (
          input.conversationHash &&
          row.conversationHash !== input.conversationHash
        ) {
          return false;
        }
        const latestResult = latestResults.get(row.operationId);
        if (
          !input.includeProcessed &&
          latestResult &&
          TERMINAL_COMMIT_TURN_RESULT_STATUSES.has(latestResult.status)
        ) {
          return false;
        }
        row.latestResult = latestResult;
        return true;
      })
      .sort(
        (left, right) => left.createdAt.getTime() - right.createdAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));

    return operations;
  }

  async recordAgentClaim(
    input: RecordAgentClaimInput,
    session?: Session,
  ): Promise<AgentClaimRecord> {
    const db = this.getDb(session);
    await this.ensureAgentClaimControlRun(db);
    const startedAt = asDate(input.startedAt);
    const expiresAt = input.expiresAt
      ? asDate(input.expiresAt)
      : new Date(startedAt.getTime() + 2 * 60 * 60 * 1000);
    const heartbeatAt = input.heartbeatAt
      ? asDate(input.heartbeatAt)
      : startedAt;
    const releasedAt = input.releasedAt ? asDate(input.releasedAt) : undefined;
    const claimId = input.claimId ?? randomUUID();
    const status = input.status ?? (releasedAt ? "released" : "active");
    const event = await this.appendSemanticEvent(
      {
        runId: AGENT_CLAIM_CONTROL_RUN_ID,
        eventId: claimId,
        eventType: AGENT_CLAIM_STATE_EVENT_TYPE,
        summaryText: input.note ?? `${input.scope} ${status}`,
        payloadJson: pruneUndefined({
          claimId,
          scope: input.scope,
          owner: input.owner,
          ownerSession: input.ownerSession,
          mode: input.mode,
          note: input.note,
          repoRoot: input.repoRoot,
          workspaceRoot: input.workspaceRoot,
          startedAt: startedAt.toISOString(),
          expiresAt: expiresAt.toISOString(),
          heartbeatAt: heartbeatAt.toISOString(),
          releasedAt: releasedAt?.toISOString(),
          status,
        }) as JsonValue,
        occurredAt: heartbeatAt,
      },
      session,
    );
    const claim = this.agentClaimFromEvent(event);
    if (!claim) {
      throw new Error(`agent claim ${event.event_id} failed to parse`);
    }
    return claim;
  }

  async renewAgentClaim(
    input: RenewAgentClaimInput,
    session?: Session,
  ): Promise<AgentClaimRecord> {
    const db = this.getDb(session);
    await this.ensureAgentClaimControlRun(db);
    const existing = await this.getLatestAgentClaimByID(db, input.claimId);
    if (!existing) {
      throw new Error(`agent claim ${input.claimId} not found`);
    }
    if (existing.status === "released") {
      throw new Error(`agent claim ${input.claimId} has already been released`);
    }
    const heartbeatAt = input.heartbeatAt
      ? asDate(input.heartbeatAt)
      : new Date();
    const expiresAt = input.expiresAt
      ? asDate(input.expiresAt)
      : existing.expiresAt;
    return this.recordAgentClaim(
      {
        claimId: existing.claimId,
        scope: existing.scope,
        owner: existing.owner,
        ownerSession: existing.ownerSession,
        mode: existing.mode,
        note: existing.note,
        repoRoot: existing.repoRoot,
        workspaceRoot: existing.workspaceRoot,
        startedAt: existing.startedAt,
        expiresAt,
        heartbeatAt,
        status: "active",
      },
      session,
    );
  }

  async releaseAgentClaim(
    input: ReleaseAgentClaimInput,
    session?: Session,
  ): Promise<AgentClaimRecord> {
    const db = this.getDb(session);
    await this.ensureAgentClaimControlRun(db);
    const existing = await this.getLatestAgentClaimByID(db, input.claimId);
    if (!existing) {
      throw new Error(`agent claim ${input.claimId} not found`);
    }
    const releasedAt = input.releasedAt ? asDate(input.releasedAt) : new Date();
    return this.recordAgentClaim(
      {
        claimId: existing.claimId,
        scope: existing.scope,
        owner: existing.owner,
        ownerSession: existing.ownerSession,
        mode: existing.mode,
        note: existing.note,
        repoRoot: existing.repoRoot,
        workspaceRoot: existing.workspaceRoot,
        startedAt: existing.startedAt,
        expiresAt: existing.expiresAt,
        heartbeatAt: releasedAt,
        releasedAt,
        status: "released",
      },
      session,
    );
  }

  async listAgentClaims(
    input: ListAgentClaimsInput = {},
    session?: Session,
  ): Promise<AgentClaimRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: AGENT_CLAIM_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 8),
    );

    const latestClaims = new Map<string, AgentClaimRecord>();
    for (const row of rows) {
      if (row.event_type !== AGENT_CLAIM_STATE_EVENT_TYPE) continue;
      const claim = this.agentClaimFromEvent(row);
      if (!claim || latestClaims.has(claim.claimId)) continue;
      latestClaims.set(claim.claimId, claim);
    }

    const now = new Date();
    return [...latestClaims.values()]
      .map((claim) => {
        if (claim.status === "active" && isExpired(claim.expiresAt, now)) {
          return { ...claim, status: "expired" as const };
        }
        return claim;
      })
      .filter((claim) => {
        if (input.scopePrefix && !claim.scope.startsWith(input.scopePrefix))
          return false;
        if (input.ownerSession && claim.ownerSession !== input.ownerSession)
          return false;
        if (!input.includeReleased && claim.status === "released") return false;
        if (!input.includeExpired && claim.status === "expired") return false;
        return true;
      })
      .sort(
        (left, right) =>
          right.heartbeatAt.getTime() - left.heartbeatAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));
  }

  async recordJob(
    input: RecordJobInput,
    session?: Session,
  ): Promise<JobRecord> {
    const db = this.getDb(session);
    await this.ensureJobControlRun(db);
    const createdAt = asDate(input.createdAt);
    if (!input.jobId && input.kind && input.dedupeKey) {
      const existing = await this.findLiveJobByDedupeKey(
        db,
        input.kind,
        input.dedupeKey,
      );
      if (existing) {
        return existing;
      }
    }

    const jobId = input.jobId ?? randomUUID();
    const existing = await this.getLatestJobByID(db, jobId);
    if (existing) {
      return existing;
    }

    const event = await this.appendSemanticEvent(
      {
        runId: JOB_CONTROL_RUN_ID,
        eventId: jobId,
        eventType: JOB_STATE_EVENT_TYPE,
        summaryText: input.note ?? input.kind,
        payloadJson: pruneUndefined({
          jobId,
          kind: input.kind,
          status: "queued",
          createdAt: createdAt.toISOString(),
          updatedAt: createdAt.toISOString(),
          attempt: 0,
          payloadJson: input.payloadJson,
          resultJson: input.resultJson,
          repoRoot: input.repoRoot,
          workspaceRoot: input.workspaceRoot,
          sourceChatKind: input.sourceChatKind,
          dedupeKey: input.dedupeKey,
          targetSession: input.targetSession,
          targetTurnWatermark: input.targetTurnWatermark,
          sourceSession: input.sourceSession,
          sourceWatermark: input.sourceWatermark,
          note: input.note,
        }) as JsonValue,
        occurredAt: createdAt,
      },
      session,
    );
    await this.appendJobEvent(
      {
        jobId,
        eventType: "created",
        status: "queued",
        attempt: 0,
        note: input.note,
        payloadJson: input.payloadJson,
        resultJson: input.resultJson,
        occurredAt: createdAt,
      },
      session,
    );
    const job = this.jobFromEvent(event);
    if (!job) {
      throw new Error(`job ${event.event_id} failed to parse`);
    }
    return job;
  }

  async claimJob(input: ClaimJobInput, session?: Session): Promise<JobRecord> {
    const db = this.getDb(session);
    await this.ensureJobControlRun(db);
    const existing = await this.getLatestJobByID(db, input.jobId);
    if (!existing) {
      throw new Error(`job ${input.jobId} not found`);
    }
    if (TERMINAL_JOB_STATUSES.has(existing.status)) {
      throw new Error(`job ${input.jobId} is already ${existing.status}`);
    }

    const claimedAt = asDate(input.claimedAt);
    const leaseExpiresAt = input.leaseExpiresAt
      ? asDate(input.leaseExpiresAt)
      : new Date(claimedAt.getTime() + 15 * 60 * 1000);
    const leaseExpired = isExpired(existing.leaseExpiresAt, claimedAt);
    const renewal =
      existing.claimedBy === input.claimedBy &&
      (existing.status === "claimed" || existing.status === "running");
    if (
      !renewal &&
      !leaseExpired &&
      existing.claimedBy &&
      existing.claimedBy !== input.claimedBy &&
      (existing.status === "claimed" || existing.status === "running")
    ) {
      throw new Error(
        `job ${input.jobId} is already claimed by ${existing.claimedBy}`,
      );
    }

    const status =
      renewal && existing.status === "running" ? "running" : "claimed";
    const attempt =
      input.attempt ?? (renewal ? existing.attempt : existing.attempt + 1);
    const event = await this.appendSemanticEvent(
      {
        runId: JOB_CONTROL_RUN_ID,
        eventId: existing.jobId,
        eventType: JOB_STATE_EVENT_TYPE,
        summaryText: input.note ?? existing.note ?? existing.kind,
        payloadJson: pruneUndefined({
          jobId: existing.jobId,
          kind: existing.kind,
          status,
          createdAt: existing.createdAt.toISOString(),
          updatedAt: claimedAt.toISOString(),
          claimedBy: input.claimedBy,
          leaseExpiresAt: leaseExpiresAt.toISOString(),
          attempt,
          payloadJson: existing.payloadJson,
          resultJson: existing.resultJson,
          repoRoot: existing.repoRoot,
          workspaceRoot: existing.workspaceRoot,
          sourceChatKind: existing.sourceChatKind,
          dedupeKey: existing.dedupeKey,
          targetSession: existing.targetSession,
          targetTurnWatermark: existing.targetTurnWatermark,
          sourceSession: existing.sourceSession,
          sourceWatermark: existing.sourceWatermark,
          note: input.note ?? existing.note,
        }) as JsonValue,
        occurredAt: claimedAt,
      },
      session,
    );
    await this.appendJobEvent(
      {
        jobId: existing.jobId,
        eventType: renewal ? "renewed" : "claimed",
        status,
        claimedBy: input.claimedBy,
        leaseExpiresAt,
        attempt,
        note: input.note ?? existing.note,
        occurredAt: claimedAt,
      },
      session,
    );
    const job = this.jobFromEvent(event);
    if (!job) {
      throw new Error(`job ${event.event_id} failed to parse`);
    }
    return job;
  }

  async updateJob(
    input: UpdateJobInput,
    session?: Session,
  ): Promise<JobRecord> {
    const db = this.getDb(session);
    await this.ensureJobControlRun(db);
    const existing = await this.getLatestJobByID(db, input.jobId);
    if (!existing) {
      throw new Error(`job ${input.jobId} not found`);
    }

    const updatedAt = asDate(input.updatedAt);
    const terminal = TERMINAL_JOB_STATUSES.has(input.status);
    const claimedBy =
      terminal || input.status === "queued"
        ? undefined
        : (input.claimedBy ?? existing.claimedBy);
    const leaseExpiresAt =
      terminal || input.status === "queued"
        ? undefined
        : input.leaseExpiresAt
          ? asDate(input.leaseExpiresAt)
          : existing.leaseExpiresAt;
    const attempt = input.attempt ?? existing.attempt;
    const resultJson = input.resultJson ?? existing.resultJson;
    const note = input.note ?? existing.note;

    const event = await this.appendSemanticEvent(
      {
        runId: JOB_CONTROL_RUN_ID,
        eventId: existing.jobId,
        eventType: JOB_STATE_EVENT_TYPE,
        summaryText: note ?? existing.kind,
        payloadJson: pruneUndefined({
          jobId: existing.jobId,
          kind: existing.kind,
          status: input.status,
          createdAt: existing.createdAt.toISOString(),
          updatedAt: updatedAt.toISOString(),
          claimedBy,
          leaseExpiresAt: leaseExpiresAt?.toISOString(),
          attempt,
          payloadJson: existing.payloadJson,
          resultJson,
          repoRoot: existing.repoRoot,
          workspaceRoot: existing.workspaceRoot,
          sourceChatKind: existing.sourceChatKind,
          dedupeKey: existing.dedupeKey,
          targetSession: existing.targetSession,
          targetTurnWatermark: existing.targetTurnWatermark,
          sourceSession: existing.sourceSession,
          sourceWatermark: existing.sourceWatermark,
          note,
        }) as JsonValue,
        occurredAt: updatedAt,
      },
      session,
    );
    await this.appendJobEvent(
      {
        jobId: existing.jobId,
        eventType: jobEventTypeForStatus(input.status),
        status: input.status,
        claimedBy,
        leaseExpiresAt,
        attempt,
        note,
        resultJson,
        occurredAt: updatedAt,
      },
      session,
    );
    const job = this.jobFromEvent(event);
    if (!job) {
      throw new Error(`job ${event.event_id} failed to parse`);
    }
    return job;
  }

  async cancelJob(
    input: CancelJobInput,
    session?: Session,
  ): Promise<JobRecord> {
    return this.updateJob(
      {
        jobId: input.jobId,
        status: input.status ?? "cancelled",
        note: input.reason,
        updatedAt: input.cancelledAt,
      },
      session,
    );
  }

  async getJob(jobId: string, session?: Session): Promise<JobRecord | null> {
    return this.getLatestJobByID(this.getDb(session), jobId);
  }

  async listJobs(
    input: ListJobsInput = {},
    session?: Session,
  ): Promise<JobRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: JOB_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 8),
    );

    const latestJobs = new Map<string, JobRecord>();
    for (const row of rows) {
      if (row.event_type !== JOB_STATE_EVENT_TYPE) continue;
      const job = this.jobFromEvent(row);
      if (!job || latestJobs.has(job.jobId)) continue;
      latestJobs.set(job.jobId, this.normalizeJobLease(job, new Date()));
    }

    return [...latestJobs.values()]
      .filter((job) => {
        if (input.kind && job.kind !== input.kind) return false;
        if (input.status && job.status !== input.status) return false;
        if (input.claimedBy && job.claimedBy !== input.claimedBy) return false;
        if (input.repoRoot && job.repoRoot !== input.repoRoot) return false;
        if (input.targetSession && job.targetSession !== input.targetSession)
          return false;
        if (!input.includeFinished && TERMINAL_JOB_STATUSES.has(job.status))
          return false;
        return true;
      })
      .sort(
        (left, right) => right.updatedAt.getTime() - left.updatedAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));
  }

  async listJobEvents(
    jobId: string,
    limit?: number,
    session?: Session,
  ): Promise<JobEventRecord[]> {
    const db = this.getDb(session);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: JOB_CONTROL_RUN_ID, event_type: JOB_EVENT_EVENT_TYPE })
        .orderBy("occurred_at", "desc")
        .limit(Math.max(clampLimit(limit), 50) * 4),
    );

    return rows
      .map((row) => this.jobEventFromEvent(row))
      .filter((event): event is JobEventRecord => Boolean(event))
      .filter((event) => event.jobId === jobId)
      .sort(
        (left, right) => right.occurredAt.getTime() - left.occurredAt.getTime(),
      )
      .slice(0, clampLimit(limit));
  }

  async recordContextDigest(
    input: RecordContextDigestInput,
    session?: Session,
  ): Promise<ContextDigestRecord> {
    const db = this.getDb(session);
    await this.ensureContextDigestControlRun(db);
    const digestId = input.digestId ?? randomUUID();
    const generatedAt = asDate(input.generatedAt);
    const expiresAt = input.expiresAt ? asDate(input.expiresAt) : undefined;
    const event = await this.appendSemanticEvent(
      {
        runId: CONTEXT_DIGEST_CONTROL_RUN_ID,
        eventId: digestId,
        eventType: CONTEXT_DIGEST_EVENT_TYPE,
        summaryText: input.reason ?? input.kind,
        payloadJson: pruneUndefined({
          digestId,
          targetProvider: input.targetProvider,
          targetSession: input.targetSession,
          targetTurnOrdinal: input.targetTurnOrdinal,
          targetConversation: input.targetConversation,
          targetConversationHash: input.targetConversationHash,
          sourceSession: input.sourceSession,
          sourceWatermarkKind: input.sourceWatermarkKind,
          sourceWatermarkValue: input.sourceWatermarkValue,
          sourceConversationHash: input.sourceConversationHash,
          kind: input.kind,
          digestText: input.digestText,
          modelUsed: input.modelUsed,
          score: input.score,
          confidence: input.confidence,
          reason: input.reason,
          generatedAt: generatedAt.toISOString(),
          expiresAt: expiresAt?.toISOString(),
          status: input.status ?? "ready",
        }) as JsonValue,
        occurredAt: generatedAt,
      },
      session,
    );
    const digest = this.contextDigestFromEvent(event);
    if (!digest) {
      throw new Error(`context digest ${event.event_id} failed to parse`);
    }
    return digest;
  }

  async listContextDigests(
    input: ListContextDigestsInput = {},
    session?: Session,
  ): Promise<ContextDigestRecord[]> {
    const db = this.getDb(session);
    const limit = Math.max(clampLimit(input.limit), 50);
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: CONTEXT_DIGEST_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(limit * 8),
    );

    const latestDigests = new Map<string, ContextDigestRecord>();
    for (const row of rows) {
      if (row.event_type !== CONTEXT_DIGEST_EVENT_TYPE) continue;
      const digest = this.contextDigestFromEvent(row);
      if (!digest || latestDigests.has(digest.digestId)) continue;
      latestDigests.set(digest.digestId, digest);
    }

    const now = new Date();
    return [...latestDigests.values()]
      .map((digest) => {
        if (digest.status === "ready" && isExpired(digest.expiresAt, now)) {
          return { ...digest, status: "expired" as const };
        }
        return digest;
      })
      .filter((digest) => {
        if (input.targetSession && digest.targetSession !== input.targetSession)
          return false;
        if (
          input.targetConversation &&
          digest.targetConversation !== input.targetConversation
        )
          return false;
        if (
          input.targetConversationHash &&
          digest.targetConversationHash !== input.targetConversationHash
        ) {
          return false;
        }
        if (
          input.targetTurnOrdinal !== undefined &&
          digest.targetTurnOrdinal !== input.targetTurnOrdinal
        ) {
          return false;
        }
        if (input.sourceSession && digest.sourceSession !== input.sourceSession)
          return false;
        if (input.kind && digest.kind !== input.kind) return false;
        if (!input.includeExpired && digest.status === "expired") return false;
        return true;
      })
      .sort(
        (left, right) =>
          right.generatedAt.getTime() - left.generatedAt.getTime(),
      )
      .slice(0, clampLimit(input.limit));
  }

  async listRecentRuns(limit?: number, session?: Session): Promise<AgentRun[]> {
    return this.getDb(session).all(
      app.agent_runs.orderBy("started_at", "desc").limit(clampLimit(limit)),
    );
  }

  async listActiveRuns(limit?: number, session?: Session): Promise<AgentRun[]> {
    const recent = await this.listRecentRuns(
      Math.max(clampLimit(limit), 50),
      session,
    );
    return recent
      .filter((run) => !TERMINAL_RUN_STATUSES.has(run.status))
      .slice(0, clampLimit(limit));
  }

  async getRunSummary(
    runId: string,
    session?: Session,
  ): Promise<AgentRunSummary | null> {
    const db = this.getDb(session);
    const run = await this.getRunByExternalId(db, runId);
    if (!run) return null;

    const [
      items,
      semanticEvents,
      wireEvents,
      artifacts,
      workspaceSnapshots,
      memoryLinks,
      sourceFiles,
    ] = await Promise.all([
      db.all(app.run_items.where({ run_id: runId }).orderBy("sequence", "asc")),
      db.all(
        app.semantic_events
          .where({ run_id: runId })
          .orderBy("occurred_at", "asc"),
      ),
      db.all(
        app.wire_events.where({ run_id: runId }).orderBy("occurred_at", "asc"),
      ),
      db.all(
        app.artifacts.where({ run_id: runId }).orderBy("created_at", "asc"),
      ),
      db.all(
        app.workspace_snapshots
          .where({ run_id: runId })
          .orderBy("captured_at", "desc"),
      ),
      db.all(
        app.memory_links.where({ run_id: runId }).orderBy("created_at", "asc"),
      ),
      db.all(
        app.source_files.where({ run_id: runId }).orderBy("created_at", "desc"),
      ),
    ]);

    const latestAgentState = await db.one(
      app.agent_state_snapshots
        .where({ agent_id: run.agent_id })
        .orderBy("captured_at", "desc")
        .limit(1),
    );

    return {
      run,
      items,
      semanticEvents,
      wireEvents,
      artifacts,
      workspaceSnapshots,
      memoryLinks,
      sourceFiles,
      latestAgentState,
    };
  }

  private getDb(session?: Session): Db {
    const db = session
      ? this.context.forSession(session, app)
      : this.useBackendSync
        ? this.context.asBackend(app)
        : this.context.db(app);
    const compatible = db as Db & {
      insertDurable?: (
        table: unknown,
        values: unknown,
        options?: { readonly tier?: DurabilityTier },
      ) => Promise<unknown>;
      updateDurable?: (
        tableOrId: unknown,
        idOrValues: unknown,
        updatesOrOptions?: unknown,
        options?: { readonly tier?: DurabilityTier },
      ) => Promise<unknown>;
    };
    compatible.insertDurable ??= async (table, values, options) =>
      await db
        .insert(table as never, values as never)
        .wait({ tier: options?.tier ?? this.writeTier });
    compatible.updateDurable ??= async (
      tableOrId,
      idOrValues,
      updatesOrOptions,
      options,
    ) => {
      if (typeof tableOrId === "string") {
        await (db as any)
          .update(tableOrId as never, idOrValues as never)
          .wait({ tier: options?.tier ?? this.writeTier });
        return;
      }
      await db
        .update(
          tableOrId as never,
          idOrValues as string,
          updatesOrOptions as never,
        )
        .wait({
          tier:
            (options ?? (updatesOrOptions as { readonly tier?: DurabilityTier }))
              ?.tier ?? this.writeTier,
        });
    };
    return db;
  }

  private async getAgentByExternalId(
    db: Db,
    agentId: string,
  ): Promise<Agent | null> {
    return db.one(app.agents.where({ agent_id: agentId }));
  }

  private async getRunByExternalId(
    db: Db,
    runId: string,
  ): Promise<AgentRun | null> {
    return db.one(app.agent_runs.where({ run_id: runId }));
  }

  private async getItemByExternalId(
    db: Db,
    runId: string,
    itemId: string,
  ): Promise<RunItem | null> {
    return db.one(app.run_items.where({ run_id: runId, item_id: itemId }));
  }

  private async getTaskByExternalId(
    db: Db,
    taskId: string,
  ): Promise<TaskRecord | null> {
    return db.one(app.task_records.where({ task_id: taskId }));
  }

  private async getDaemonLogSourceByExternalId(
    db: Db,
    sourceId: string,
  ): Promise<DaemonLogSource | null> {
    return db.one(app.daemon_log_sources.where({ source_id: sourceId }));
  }

  private async getDaemonLogChunkByExternalId(
    db: Db,
    chunkId: string,
  ): Promise<DaemonLogChunk | null> {
    return db.one(app.daemon_log_chunks.where({ chunk_id: chunkId }));
  }

  private async getDesignerObjectRefByExternalId(
    db: Db,
    objectRefId: string,
  ): Promise<DesignerObjectRef | null> {
    return db.one(
      app.designer_object_refs.where({ object_ref_id: objectRefId }),
    );
  }

  private async requireDesignerObjectRefByExternalId(
    db: Db,
    objectRefId: string,
  ): Promise<DesignerObjectRef> {
    const objectRef = await this.getDesignerObjectRefByExternalId(
      db,
      objectRefId,
    );
    if (!objectRef) {
      throw new Error(`designer object ref ${objectRefId} not found`);
    }
    return objectRef;
  }

  private async getDesignerCodexConversationByExternalId(
    db: Db,
    conversationId: string,
  ): Promise<DesignerCodexConversation | null> {
    return db.one(
      app.designer_codex_conversations.where({
        conversation_id: conversationId,
      }),
    );
  }

  private async requireDesignerCodexConversationByExternalId(
    db: Db,
    conversationId: string,
  ): Promise<DesignerCodexConversation> {
    const conversation = await this.getDesignerCodexConversationByExternalId(
      db,
      conversationId,
    );
    if (!conversation) {
      throw new Error(`designer codex conversation ${conversationId} not found`);
    }
    return conversation;
  }

  private async getDesignerCodexTurnByExternalId(
    db: Db,
    turnId: string,
  ): Promise<DesignerCodexTurn | null> {
    return db.one(app.designer_codex_turns.where({ turn_id: turnId }));
  }

  private async requireDesignerCodexTurnByExternalId(
    db: Db,
    turnId: string,
  ): Promise<DesignerCodexTurn> {
    const turn = await this.getDesignerCodexTurnByExternalId(db, turnId);
    if (!turn) {
      throw new Error(`designer codex turn ${turnId} not found`);
    }
    return turn;
  }

  private async resolveDesignerCodexTurn(
    db: Db,
    input: {
      sourceConversationId?: string;
      sourceTurnId?: string;
      sourceTurnOrdinal?: number;
    },
  ): Promise<DesignerCodexTurn | null> {
    if (input.sourceTurnId) {
      return this.requireDesignerCodexTurnByExternalId(db, input.sourceTurnId);
    }
    if (
      input.sourceConversationId &&
      input.sourceTurnOrdinal !== undefined
    ) {
      return db.one(
        app.designer_codex_turns.where({
          conversation_id: input.sourceConversationId,
          sequence: input.sourceTurnOrdinal,
        }),
      );
    }
    return null;
  }

  private async getDesignerAgentByExternalId(
    db: Db,
    agentId: string,
  ): Promise<DesignerAgent | null> {
    return db.one(app.designer_agents.where({ agent_id: agentId }));
  }

  private async requireDesignerAgentByExternalId(
    db: Db,
    agentId: string,
  ): Promise<DesignerAgent> {
    const agent = await this.getDesignerAgentByExternalId(db, agentId);
    if (!agent) {
      throw new Error(`designer agent ${agentId} not found`);
    }
    return agent;
  }

  private async getDesignerAgentToolByExternalId(
    db: Db,
    toolId: string,
  ): Promise<DesignerAgentTool | null> {
    return db.one(app.designer_agent_tools.where({ tool_id: toolId }));
  }

  private async requireDesignerAgentToolByExternalId(
    db: Db,
    toolId: string,
  ): Promise<DesignerAgentTool> {
    const tool = await this.getDesignerAgentToolByExternalId(db, toolId);
    if (!tool) {
      throw new Error(`designer agent tool ${toolId} not found`);
    }
    return tool;
  }

  private async getDesignerAgentContextByExternalId(
    db: Db,
    contextId: string,
  ): Promise<DesignerAgentContext | null> {
    return db.one(
      app.designer_agent_contexts.where({ context_id: contextId }),
    );
  }

  private async requireDesignerAgentContextByExternalId(
    db: Db,
    contextId: string,
  ): Promise<DesignerAgentContext> {
    const context = await this.getDesignerAgentContextByExternalId(
      db,
      contextId,
    );
    if (!context) {
      throw new Error(`designer agent context ${contextId} not found`);
    }
    return context;
  }

  private async getDesignerLiveCommitByExternalId(
    db: Db,
    commitId: string,
  ): Promise<DesignerLiveCommit | null> {
    return db.one(app.designer_live_commits.where({ commit_id: commitId }));
  }

  private async requireDesignerLiveCommitByExternalId(
    db: Db,
    commitId: string,
  ): Promise<DesignerLiveCommit> {
    const commit = await this.getDesignerLiveCommitByExternalId(db, commitId);
    if (!commit) {
      throw new Error(`designer live commit ${commitId} not found`);
    }
    return commit;
  }

  private async getDesignerCadWorkspaceByExternalId(
    db: Db,
    workspaceId: string,
  ): Promise<DesignerCadWorkspace | null> {
    return db.one(
      app.designer_cad_workspaces.where({ workspace_id: workspaceId }),
    );
  }

  private async getDesignerCadDocumentByExternalId(
    db: Db,
    documentId: string,
  ): Promise<DesignerCadDocument | null> {
    return db.one(
      app.designer_cad_documents.where({ document_id: documentId }),
    );
  }

  private async getDesignerCadSessionByExternalId(
    db: Db,
    cadSessionId: string,
  ): Promise<DesignerCadSession | null> {
    return db.one(
      app.designer_cad_sessions.where({ cad_session_id: cadSessionId }),
    );
  }

  private async getDesignerCadToolSessionByExternalId(
    db: Db,
    toolSessionId: string,
  ): Promise<DesignerCadToolSession | null> {
    return db.one(
      app.designer_cad_tool_sessions.where({
        tool_session_id: toolSessionId,
      }),
    );
  }

  private async getDesignerCadOperationByExternalId(
    db: Db,
    operationId: string,
  ): Promise<DesignerCadOperation | null> {
    return db.one(
      app.designer_cad_operations.where({ operation_id: operationId }),
    );
  }

  private async getDesignerCadPreviewHandleByExternalId(
    db: Db,
    previewId: string,
  ): Promise<DesignerCadPreviewHandle | null> {
    return db.one(
      app.designer_cad_preview_handles.where({ preview_id: previewId }),
    );
  }

  private async requireDaemonLogSourceByExternalId(
    db: Db,
    sourceId: string,
  ): Promise<DaemonLogSource> {
    const source = await this.getDaemonLogSourceByExternalId(db, sourceId);
    if (!source) {
      throw new Error(`daemon log source ${sourceId} not found`);
    }
    return source;
  }

  private async requireDaemonLogChunkByExternalId(
    db: Db,
    chunkId: string,
  ): Promise<DaemonLogChunk> {
    const chunk = await this.getDaemonLogChunkByExternalId(db, chunkId);
    if (!chunk) {
      throw new Error(`daemon log chunk ${chunkId} not found`);
    }
    return chunk;
  }

  private async requireDesignerCadWorkspaceByExternalId(
    db: Db,
    workspaceId: string,
  ): Promise<DesignerCadWorkspace> {
    const workspace = await this.getDesignerCadWorkspaceByExternalId(
      db,
      workspaceId,
    );
    if (!workspace) {
      throw new Error(`designer cad workspace ${workspaceId} not found`);
    }
    return workspace;
  }

  private async requireDesignerCadDocumentByExternalId(
    db: Db,
    documentId: string,
  ): Promise<DesignerCadDocument> {
    const document = await this.getDesignerCadDocumentByExternalId(
      db,
      documentId,
    );
    if (!document) {
      throw new Error(`designer cad document ${documentId} not found`);
    }
    return document;
  }

  private async requireDesignerCadSessionByExternalId(
    db: Db,
    cadSessionId: string,
  ): Promise<DesignerCadSession> {
    const cadSession = await this.getDesignerCadSessionByExternalId(
      db,
      cadSessionId,
    );
    if (!cadSession) {
      throw new Error(`designer cad session ${cadSessionId} not found`);
    }
    return cadSession;
  }

  private async requireDesignerCadSceneNodeByExternalId(
    db: Db,
    cadSessionId: string,
    nodeId: string,
  ): Promise<DesignerCadSceneNode> {
    const node = await db.one(
      app.designer_cad_scene_nodes.where({
        cad_session_id: cadSessionId,
        node_id: nodeId,
      }),
    );
    if (!node) {
      throw new Error(
        `designer cad scene node ${cadSessionId}:${nodeId} not found`,
      );
    }
    return node;
  }

  private async requireDesignerCadToolSessionByExternalId(
    db: Db,
    toolSessionId: string,
  ): Promise<DesignerCadToolSession> {
    const toolSession = await this.getDesignerCadToolSessionByExternalId(
      db,
      toolSessionId,
    );
    if (!toolSession) {
      throw new Error(`designer cad tool session ${toolSessionId} not found`);
    }
    return toolSession;
  }

  private async requireDesignerCadOperationByExternalId(
    db: Db,
    operationId: string,
  ): Promise<DesignerCadOperation> {
    const operation = await this.getDesignerCadOperationByExternalId(
      db,
      operationId,
    );
    if (!operation) {
      throw new Error(`designer cad operation ${operationId} not found`);
    }
    return operation;
  }

  private async requireDesignerCadPreviewHandleByExternalId(
    db: Db,
    previewId: string,
  ): Promise<DesignerCadPreviewHandle> {
    const preview = await this.getDesignerCadPreviewHandleByExternalId(
      db,
      previewId,
    );
    if (!preview) {
      throw new Error(`designer cad preview ${previewId} not found`);
    }
    return preview;
  }

  private async ensureCursorReviewControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(db, CURSOR_REVIEW_CONTROL_RUN_ID);
    if (run) return run;
    await this.recordRunStarted({
      runId: CURSOR_REVIEW_CONTROL_RUN_ID,
      agentId: CURSOR_REVIEW_AGENT_ID,
      requestSummary: "Cursor review UI control plane",
      status: "running",
      agent: {
        lane: "cursor-review",
        promptSurface: "cursor-review",
      },
    });
    run = await this.getRunByExternalId(db, CURSOR_REVIEW_CONTROL_RUN_ID);
    if (!run) {
      throw new Error("cursor review control run not found after creation");
    }
    return run;
  }

  private async ensureBranchFileReviewControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(
      db,
      BRANCH_FILE_REVIEW_CONTROL_RUN_ID,
    );
    if (run) return run;
    await this.recordRunStarted({
      runId: BRANCH_FILE_REVIEW_CONTROL_RUN_ID,
      agentId: BRANCH_FILE_REVIEW_AGENT_ID,
      requestSummary: "Branch file review state control plane",
      status: "running",
      agent: {
        lane: "branch-file-review",
        promptSurface: "branch-file-review",
      },
    });
    run = await this.getRunByExternalId(db, BRANCH_FILE_REVIEW_CONTROL_RUN_ID);
    if (!run) {
      throw new Error(
        "branch file review control run not found after creation",
      );
    }
    return run;
  }

  private async ensureCommitTurnControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(db, COMMIT_TURN_CONTROL_RUN_ID);
    if (run) return run;
    await this.recordRunStarted({
      runId: COMMIT_TURN_CONTROL_RUN_ID,
      agentId: COMMIT_TURN_AGENT_ID,
      requestSummary: "Commit turn event control plane",
      status: "running",
      agent: {
        lane: "commit-turn",
        promptSurface: "commit-turn",
      },
    });
    run = await this.getRunByExternalId(db, COMMIT_TURN_CONTROL_RUN_ID);
    if (!run) {
      throw new Error("commit turn control run not found after creation");
    }
    return run;
  }

  private async ensureAgentClaimControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(db, AGENT_CLAIM_CONTROL_RUN_ID);
    if (run) return run;
    await this.recordRunStarted({
      runId: AGENT_CLAIM_CONTROL_RUN_ID,
      agentId: AGENT_CLAIM_AGENT_ID,
      requestSummary: "Workflow advisory claim control plane",
      status: "running",
      agent: {
        lane: "ops-claim",
        promptSurface: "ops-claim",
      },
    });
    run = await this.getRunByExternalId(db, AGENT_CLAIM_CONTROL_RUN_ID);
    if (!run) {
      throw new Error("agent claim control run not found after creation");
    }
    return run;
  }

  private async ensureJobControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(db, JOB_CONTROL_RUN_ID);
    if (run) return run;
    await this.recordRunStarted({
      runId: JOB_CONTROL_RUN_ID,
      agentId: JOB_AGENT_ID,
      requestSummary: "Durable workflow job control plane",
      status: "running",
      agent: {
        lane: "job",
        promptSurface: "job",
      },
    });
    run = await this.getRunByExternalId(db, JOB_CONTROL_RUN_ID);
    if (!run) {
      throw new Error("job control run not found after creation");
    }
    return run;
  }

  private async ensureContextDigestControlRun(db: Db): Promise<AgentRun> {
    let run = await this.getRunByExternalId(db, CONTEXT_DIGEST_CONTROL_RUN_ID);
    if (run) return run;
    await this.recordRunStarted({
      runId: CONTEXT_DIGEST_CONTROL_RUN_ID,
      agentId: CONTEXT_DIGEST_AGENT_ID,
      requestSummary: "Context digest control plane",
      status: "running",
      agent: {
        lane: "context-digest",
        promptSurface: "context-digest",
      },
    });
    run = await this.getRunByExternalId(db, CONTEXT_DIGEST_CONTROL_RUN_ID);
    if (!run) {
      throw new Error("context digest control run not found after creation");
    }
    return run;
  }

  private cursorReviewOperationFromEvent(
    event: SemanticEvent,
  ): CursorReviewOperationRecord | null {
    if (event.event_type !== CURSOR_REVIEW_OPERATION_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const operationId =
      readObjectString(payload, "operationId") ?? event.event_id;
    const operationType = readObjectString(payload, "operationType");
    if (!isCursorReviewOperationType(operationType)) {
      return null;
    }
    return {
      eventId: event.event_id,
      operationId,
      operationType,
      repoRoot: readObjectString(payload, "repoRoot"),
      workspaceRoot: readObjectString(payload, "workspaceRoot"),
      bookmark: readObjectString(payload, "bookmark"),
      relPath: readObjectString(payload, "relPath"),
      note:
        readObjectString(payload, "note") ?? event.summary_text ?? undefined,
      sourceSessionId: readObjectString(payload, "sourceSessionId"),
      sourceChatKind: readObjectString(payload, "sourceChatKind"),
      createdAt: event.occurred_at,
    };
  }

  private cursorReviewResultFromEvent(
    event: SemanticEvent,
  ): CursorReviewOperationResultRecord | null {
    if (event.event_type !== CURSOR_REVIEW_RESULT_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const operationId = readObjectString(payload, "operationId");
    const status = readObjectString(payload, "status");
    if (!operationId) return null;
    if (status !== "completed" && status !== "failed" && status !== "ignored")
      return null;
    return {
      eventId: event.event_id,
      operationId,
      status,
      clientId: readObjectString(payload, "clientId"),
      repoRoot: readObjectString(payload, "repoRoot"),
      message:
        readObjectString(payload, "message") ?? event.summary_text ?? undefined,
      processedAt: event.occurred_at,
    };
  }

  private branchFileReviewStateFromEvent(
    event: SemanticEvent,
  ): BranchFileReviewStateRecord | null {
    if (event.event_type !== BRANCH_FILE_REVIEW_STATE_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const repoRoot = readObjectString(payload, "repoRoot");
    const bookmark = readObjectString(payload, "bookmark");
    const relPath = readObjectString(payload, "relPath");
    const status = readObjectString(payload, "status");
    if (!repoRoot || !bookmark || !relPath) return null;
    if (status !== "happy" && status !== "needs-work" && status !== "cleared")
      return null;
    return {
      eventId: event.event_id,
      repoRoot,
      workspaceRoot: readObjectString(payload, "workspaceRoot"),
      bookmark,
      relPath,
      status,
      note:
        readObjectString(payload, "note") ?? event.summary_text ?? undefined,
      sourceSessionId: readObjectString(payload, "sourceSessionId"),
      sourceChatKind: readObjectString(payload, "sourceChatKind"),
      createdAt: event.occurred_at,
    };
  }

  private commitTurnOperationFromEvent(
    event: SemanticEvent,
  ): CommitTurnOperationRecord | null {
    if (event.event_type !== COMMIT_TURN_OPERATION_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const operationId =
      readObjectString(payload, "operationId") ?? event.event_id;
    const provider = readObjectString(payload, "provider");
    const sessionId = readObjectString(payload, "sessionId");
    const conversation = readObjectString(payload, "conversation");
    const conversationHash = readObjectString(payload, "conversationHash");
    const trigger = readObjectString(payload, "trigger");
    const sessionEventId = readObjectString(payload, "sessionEventId");
    const turnOrdinalRaw = payload?.turnOrdinal;
    const turnOrdinal =
      typeof turnOrdinalRaw === "number" ? turnOrdinalRaw : null;
    if (
      !provider ||
      !sessionId ||
      !conversation ||
      !conversationHash ||
      !trigger ||
      !sessionEventId ||
      !turnOrdinal
    ) {
      return null;
    }
    return {
      eventId: event.event_id,
      operationId,
      provider,
      sessionId,
      conversation,
      conversationHash,
      trigger,
      turnOrdinal,
      sessionEventId,
      repoRoot: readObjectString(payload, "repoRoot"),
      repoRoots: readObjectStringArray(payload, "repoRoots"),
      cwd: readObjectString(payload, "cwd"),
      artifactPath: readObjectString(payload, "artifactPath"),
      promptPreview:
        readObjectString(payload, "promptPreview") ??
        event.summary_text ??
        undefined,
      sourceChatKind: readObjectString(payload, "sourceChatKind"),
      createdAt: event.occurred_at,
    };
  }

  private commitTurnResultFromEvent(
    event: SemanticEvent,
  ): CommitTurnResultRecord | null {
    if (event.event_type !== COMMIT_TURN_RESULT_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const operationId = readObjectString(payload, "operationId");
    const status = readObjectString(payload, "status");
    if (!operationId) return null;
    if (status !== "completed" && status !== "failed" && status !== "ignored")
      return null;
    return {
      eventId: event.event_id,
      operationId,
      status,
      agentId: readObjectString(payload, "agentId"),
      runId: readObjectString(payload, "runId"),
      threadId: readObjectString(payload, "threadId"),
      repoRoot: readObjectString(payload, "repoRoot"),
      model: readObjectString(payload, "model"),
      effort: readObjectString(payload, "effort"),
      traceRef: readObjectString(payload, "traceRef"),
      message:
        readObjectString(payload, "message") ?? event.summary_text ?? undefined,
      classification: readObjectString(payload, "classification"),
      title: readObjectString(payload, "title"),
      description: readObjectString(payload, "description"),
      commitMessage: readObjectString(payload, "commitMessage"),
      todoItems: readObjectStringArray(payload, "todoItems"),
      notes: readObjectString(payload, "notes"),
      group: readObjectString(payload, "group"),
      groupReason: readObjectString(payload, "groupReason"),
      groupIsNew: readObjectBoolean(payload, "groupIsNew"),
      snapshotCommitId: readObjectString(payload, "snapshotCommitId"),
      reviewJobId: readObjectString(payload, "reviewJobId"),
      conversationHash: readObjectString(payload, "conversationHash"),
      processedAt: event.occurred_at,
    };
  }

  private agentClaimFromEvent(event: SemanticEvent): AgentClaimRecord | null {
    if (event.event_type !== AGENT_CLAIM_STATE_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const claimId = readObjectString(payload, "claimId") ?? event.event_id;
    const scope = readObjectString(payload, "scope");
    const owner = readObjectString(payload, "owner");
    const startedAt = readObjectDate(payload, "startedAt");
    const expiresAt = readObjectDate(payload, "expiresAt");
    const heartbeatAt =
      readObjectDate(payload, "heartbeatAt") ?? event.occurred_at;
    const releasedAt = readObjectDate(payload, "releasedAt");
    const status = readObjectString(payload, "status");
    if (!scope || !owner || !startedAt || !expiresAt || !heartbeatAt)
      return null;
    if (status !== "active" && status !== "released" && status !== "expired")
      return null;
    return {
      eventId: event.event_id,
      claimId,
      scope,
      owner,
      ownerSession: readObjectString(payload, "ownerSession"),
      mode: readObjectString(payload, "mode"),
      note:
        readObjectString(payload, "note") ?? event.summary_text ?? undefined,
      repoRoot: readObjectString(payload, "repoRoot"),
      workspaceRoot: readObjectString(payload, "workspaceRoot"),
      startedAt,
      expiresAt,
      heartbeatAt,
      releasedAt,
      status,
    };
  }

  private jobFromEvent(event: SemanticEvent): JobRecord | null {
    if (event.event_type !== JOB_STATE_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const jobId = readObjectString(payload, "jobId") ?? event.event_id;
    const kind = readObjectString(payload, "kind");
    const status = readObjectString(payload, "status");
    const createdAt = readObjectDate(payload, "createdAt");
    const updatedAt = readObjectDate(payload, "updatedAt") ?? event.occurred_at;
    const attempt = readObjectNumber(payload, "attempt") ?? 0;
    if (!kind || !createdAt || !updatedAt) return null;
    if (
      status !== "queued" &&
      status !== "claimed" &&
      status !== "running" &&
      status !== "completed" &&
      status !== "failed" &&
      status !== "cancelled" &&
      status !== "cancelled-superseded"
    ) {
      return null;
    }
    return {
      eventId: event.event_id,
      jobId,
      kind,
      status,
      createdAt,
      updatedAt,
      claimedBy: readObjectString(payload, "claimedBy"),
      leaseExpiresAt: readObjectDate(payload, "leaseExpiresAt"),
      attempt,
      payloadJson: payload?.payloadJson as JsonValue | undefined,
      resultJson: payload?.resultJson as JsonValue | undefined,
      repoRoot: readObjectString(payload, "repoRoot"),
      workspaceRoot: readObjectString(payload, "workspaceRoot"),
      sourceChatKind: readObjectString(payload, "sourceChatKind"),
      dedupeKey: readObjectString(payload, "dedupeKey"),
      targetSession: readObjectString(payload, "targetSession"),
      targetTurnWatermark: readObjectString(payload, "targetTurnWatermark"),
      sourceSession: readObjectString(payload, "sourceSession"),
      sourceWatermark: readObjectString(payload, "sourceWatermark"),
      note:
        readObjectString(payload, "note") ?? event.summary_text ?? undefined,
    };
  }

  private jobEventFromEvent(event: SemanticEvent): JobEventRecord | null {
    if (event.event_type !== JOB_EVENT_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const jobId = readObjectString(payload, "jobId");
    const eventType = readObjectString(payload, "eventType");
    if (!jobId) return null;
    if (
      eventType !== "created" &&
      eventType !== "claimed" &&
      eventType !== "renewed" &&
      eventType !== "running" &&
      eventType !== "completed" &&
      eventType !== "failed" &&
      eventType !== "cancelled" &&
      eventType !== "cancelled-superseded"
    ) {
      return null;
    }

    const status = readObjectString(payload, "status");
    if (
      status !== undefined &&
      status !== "queued" &&
      status !== "claimed" &&
      status !== "running" &&
      status !== "completed" &&
      status !== "failed" &&
      status !== "cancelled" &&
      status !== "cancelled-superseded"
    ) {
      return null;
    }

    return {
      eventId: event.event_id,
      jobId,
      eventType,
      status,
      claimedBy: readObjectString(payload, "claimedBy"),
      leaseExpiresAt: readObjectDate(payload, "leaseExpiresAt"),
      attempt: readObjectNumber(payload, "attempt"),
      note:
        readObjectString(payload, "note") ?? event.summary_text ?? undefined,
      payloadJson: payload?.payloadJson as JsonValue | undefined,
      resultJson: payload?.resultJson as JsonValue | undefined,
      occurredAt: event.occurred_at,
    };
  }

  private contextDigestFromEvent(
    event: SemanticEvent,
  ): ContextDigestRecord | null {
    if (event.event_type !== CONTEXT_DIGEST_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const digestId = readObjectString(payload, "digestId") ?? event.event_id;
    const targetProvider = readObjectString(payload, "targetProvider");
    const targetSession = readObjectString(payload, "targetSession");
    const targetTurnOrdinal = readObjectNumber(payload, "targetTurnOrdinal");
    const targetConversation = readObjectString(payload, "targetConversation");
    const targetConversationHash = readObjectString(
      payload,
      "targetConversationHash",
    );
    const sourceSession = readObjectString(payload, "sourceSession");
    const sourceWatermarkKind = readObjectString(
      payload,
      "sourceWatermarkKind",
    );
    const sourceWatermarkValue = readObjectString(
      payload,
      "sourceWatermarkValue",
    );
    const kind = readObjectString(payload, "kind");
    const digestText = readObjectString(payload, "digestText");
    const generatedAt =
      readObjectDate(payload, "generatedAt") ?? event.occurred_at;
    const status = readObjectString(payload, "status");
    if (
      !targetProvider ||
      !targetSession ||
      targetTurnOrdinal === undefined ||
      !targetConversation ||
      !targetConversationHash ||
      !sourceSession ||
      !sourceWatermarkKind ||
      !sourceWatermarkValue ||
      !kind ||
      !digestText ||
      !generatedAt
    ) {
      return null;
    }
    if (
      status !== "ready" &&
      status !== "superseded" &&
      status !== "expired" &&
      status !== "error"
    ) {
      return null;
    }
    return {
      eventId: event.event_id,
      digestId,
      targetProvider,
      targetSession,
      targetTurnOrdinal,
      targetConversation,
      targetConversationHash,
      sourceSession,
      sourceWatermarkKind,
      sourceWatermarkValue,
      sourceConversationHash: readObjectString(
        payload,
        "sourceConversationHash",
      ),
      kind,
      digestText,
      modelUsed: readObjectString(payload, "modelUsed"),
      score: readObjectNumber(payload, "score"),
      confidence: readObjectString(payload, "confidence"),
      reason:
        readObjectString(payload, "reason") ?? event.summary_text ?? undefined,
      generatedAt,
      expiresAt: readObjectDate(payload, "expiresAt"),
      status,
    };
  }

  private async getLatestAgentClaimByID(
    db: Db,
    claimId: string,
  ): Promise<AgentClaimRecord | null> {
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: AGENT_CLAIM_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(200),
    );
    for (const row of rows) {
      if (row.event_type !== AGENT_CLAIM_STATE_EVENT_TYPE) continue;
      const claim = this.agentClaimFromEvent(row);
      if (claim?.claimId === claimId) {
        return claim;
      }
    }
    return null;
  }

  private async getLatestJobByID(
    db: Db,
    jobId: string,
  ): Promise<JobRecord | null> {
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: JOB_CONTROL_RUN_ID })
        .orderBy("occurred_at", "desc")
        .limit(400),
    );
    for (const row of rows) {
      if (row.event_type !== JOB_STATE_EVENT_TYPE) continue;
      const job = this.jobFromEvent(row);
      if (job?.jobId === jobId) {
        return this.normalizeJobLease(job, new Date());
      }
    }
    return null;
  }

  private async findLiveJobByDedupeKey(
    db: Db,
    kind: string,
    dedupeKey: string,
  ): Promise<JobRecord | null> {
    const rows = await db.all(
      app.semantic_events
        .where({ run_id: JOB_CONTROL_RUN_ID, event_type: JOB_STATE_EVENT_TYPE })
        .orderBy("occurred_at", "desc")
        .limit(400),
    );
    const now = new Date();
    for (const row of rows) {
      const job = this.jobFromEvent(row);
      if (!job) continue;
      const normalized = this.normalizeJobLease(job, now);
      if (
        normalized.kind === kind &&
        normalized.dedupeKey === dedupeKey &&
        !TERMINAL_JOB_STATUSES.has(normalized.status)
      ) {
        return normalized;
      }
    }
    return null;
  }

  private normalizeJobLease(job: JobRecord, now: Date): JobRecord {
    if (
      (job.status === "claimed" || job.status === "running") &&
      isExpired(job.leaseExpiresAt, now)
    ) {
      return {
        ...job,
        status: "queued",
        claimedBy: undefined,
        leaseExpiresAt: undefined,
      };
    }
    return job;
  }

  private async appendJobEvent(
    input: {
      jobId: string;
      eventType: JobEventType;
      status?: JobStatus;
      claimedBy?: string;
      leaseExpiresAt?: Date;
      attempt?: number;
      note?: string;
      payloadJson?: JsonValue;
      resultJson?: JsonValue;
      occurredAt?: Date;
    },
    session?: Session,
  ): Promise<JobEventRecord> {
    const event = await this.appendSemanticEvent(
      {
        runId: JOB_CONTROL_RUN_ID,
        eventType: JOB_EVENT_EVENT_TYPE,
        summaryText: input.note ?? `${input.jobId} ${input.eventType}`,
        payloadJson: pruneUndefined({
          jobId: input.jobId,
          eventType: input.eventType,
          status: input.status,
          claimedBy: input.claimedBy,
          leaseExpiresAt: input.leaseExpiresAt?.toISOString(),
          attempt: input.attempt,
          note: input.note,
          payloadJson: input.payloadJson,
          resultJson: input.resultJson,
        }) as JsonValue,
        occurredAt: input.occurredAt,
      },
      session,
    );
    const jobEvent = this.jobEventFromEvent(event);
    if (!jobEvent) {
      throw new Error(`job event ${event.event_id} failed to parse`);
    }
    return jobEvent;
  }

  private async requireItemByExternalId(
    db: Db,
    runId: string,
    itemId: string,
  ): Promise<RunItem> {
    const item = await this.getItemByExternalId(db, runId, itemId);
    if (!item) {
      throw new Error(
        `Run item not found for run_id=${runId} item_id=${itemId}`,
      );
    }
    return item;
  }

  private async requireByQuery<T>(
    db: Db,
    query: QueryBuilder<T>,
    description: string,
  ): Promise<T> {
    const row = await db.one(query);
    if (!row) {
      throw new Error(`${description} not found`);
    }
    return row;
  }

  private async updateRow<T, Init>(
    db: Db,
    table: TableProxy<T, Init>,
    id: string,
    updates: Partial<Init>,
  ): Promise<void> {
    const payload = pruneUndefined(updates as Record<string, unknown>);
    if (Object.keys(payload).length === 0) return;
    await db.update(table as never, id, payload as never).wait({
      tier: this.writeTier,
    });
  }
}

export function createAgentDataStore(
  config: AgentDataStoreConfig,
): AgentDataStore {
  const tier = config.tier ?? "edge";
  const context = createJazzContext({
    appId: config.appId ?? DEFAULT_APP_ID,
    app,
    permissions: {},
    driver: { type: "persistent", dataPath: config.dataPath },
    env: config.env ?? "dev",
    userBranch: config.userBranch ?? "main",
    serverUrl: config.serverUrl,
    backendSecret: config.backendSecret,
    adminSecret: config.adminSecret,
    tier,
  });
  return new AgentDataStore(context, tier, Boolean(config.serverUrl));
}
