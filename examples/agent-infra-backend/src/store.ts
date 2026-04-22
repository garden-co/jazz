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
const COMMIT_TURN_AGENT_ID = "commit";
const COMMIT_TURN_CONTROL_RUN_ID = "commit-turn-control";
const COMMIT_TURN_OPERATION_EVENT_TYPE = "commit_turn_operation";
const COMMIT_TURN_RESULT_EVENT_TYPE = "commit_turn_result";
const AGENT_CLAIM_AGENT_ID = "ops-claim";
const AGENT_CLAIM_CONTROL_RUN_ID = "ops-claim-control";
const AGENT_CLAIM_STATE_EVENT_TYPE = "agent_claim_state";
const CONTEXT_DIGEST_AGENT_ID = "context-distill";
const CONTEXT_DIGEST_CONTROL_RUN_ID = "context-digest-control";
const CONTEXT_DIGEST_EVENT_TYPE = "context_digest";
const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "cancelled", "error"]);
const TERMINAL_CURSOR_REVIEW_RESULT_STATUSES = new Set(["completed", "failed", "ignored"]);
const TERMINAL_COMMIT_TURN_RESULT_STATUSES = new Set(["completed", "failed", "ignored"]);
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

export type CursorReviewOperationType =
  | "focus-branch-review"
  | "refresh-branch-review"
  | "copy-branch-review-prompt"
  | "open-branch-review-chat"
  | "show-branch-diff"
  | "open-branch-file-diff";

export type CursorReviewOperationResultStatus = "completed" | "failed" | "ignored";

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
  message?: string;
  classification?: string;
  title?: string;
  description?: string;
  commitMessage?: string;
  todoItems?: string[];
  notes?: string;
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
  message?: string;
  classification?: string;
  title?: string;
  description?: string;
  commitMessage?: string;
  todoItems?: string[];
  notes?: string;
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
  const placementCompare = taskPlacementRank(left.placement) - taskPlacementRank(right.placement);
  if (placementCompare !== 0) return placementCompare;

  const leftFocusRank = left.focus_rank ?? Number.MAX_SAFE_INTEGER;
  const rightFocusRank = right.focus_rank ?? Number.MAX_SAFE_INTEGER;
  if (leftFocusRank !== rightFocusRank) {
    return leftFocusRank - rightFocusRank;
  }

  const priorityCompare = taskPriorityRank(left.priority) - taskPriorityRank(right.priority);
  if (priorityCompare !== 0) return priorityCompare;

  const updatedCompare = right.updated_at.getTime() - left.updated_at.getTime();
  if (updatedCompare !== 0) return updatedCompare;

  return left.task_id.localeCompare(right.task_id);
}

function asObjectRecord(value: JsonValue | undefined): Record<string, JsonValue> | null {
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
  const items = raw.filter((entry): entry is string => typeof entry === "string");
  return items.length > 0 ? items : undefined;
}

function readObjectNumber(
  value: Record<string, JsonValue> | null,
  key: string,
): number | undefined {
  const raw = value?.[key];
  return typeof raw === "number" ? raw : undefined;
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

function pruneUndefined<T extends Record<string, unknown>>(input: T): Partial<T> {
  const entries = Object.entries(input).filter(([, value]) => value !== undefined);
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
  ) {}

  flush(): void {
    this.context.flush();
  }

  async shutdown(): Promise<void> {
    await this.context.shutdown();
  }

  async upsertAgent(input: UpsertAgentInput, session?: Session): Promise<Agent> {
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
      return this.requireByQuery(db, app.agents.where({ agent_id: input.agentId }), "agent");
    }

    return db.insertDurable(
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
      { tier: this.writeTier },
    );
  }

  async recordRunStarted(input: RecordRunStartedInput, session?: Session): Promise<AgentRun> {
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
      return this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
    }

    return db.insertDurable(
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
      { tier: this.writeTier },
    );
  }

  async recordRunCompleted(input: RecordRunCompletedInput, session?: Session): Promise<AgentRun> {
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
    return this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
  }

  async recordItemStarted(input: RecordItemStartedInput, session?: Session): Promise<RunItem> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
    const existing = await this.getItemByExternalId(db, input.runId, input.itemId);

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

    return db.insertDurable(
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

  async recordItemCompleted(input: RecordItemCompletedInput, session?: Session): Promise<RunItem> {
    const db = this.getDb(session);
    const existing = await this.requireItemByExternalId(db, input.runId, input.itemId);
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
    const run = await this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
    const item = input.itemId ? await this.getItemByExternalId(db, input.runId, input.itemId) : null;
    const eventId = input.eventId ?? randomUUID();
    const existing = await db.one(app.semantic_events.where({ event_id: eventId }));

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

    return db.insertDurable(
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
      { tier: this.writeTier },
    );
  }

  async appendWireEvent(input: AppendWireEventInput, session?: Session): Promise<WireEvent> {
    const db = this.getDb(session);
    const run = input.runId ? await this.getRunByExternalId(db, input.runId) : null;
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
      return this.requireByQuery(db, app.wire_events.where({ event_id: eventId }), "wire event");
    }

    return db.insertDurable(
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

  async recordArtifact(input: RecordArtifactInput, session?: Session): Promise<Artifact> {
    const db = this.getDb(session);
    const run = await this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
    const artifactId = input.artifactId ?? randomUUID();
    const existing = await db.one(app.artifacts.where({ artifact_id: artifactId }));

    if (existing) {
      await this.updateRow(db, app.artifacts, existing.id, {
        artifact_kind: input.artifactKind,
        title: input.title,
        absolute_path: input.absolutePath,
        checksum: input.checksum,
        created_at: input.createdAt ? asDate(input.createdAt) : undefined,
      });
      return this.requireByQuery(db, app.artifacts.where({ artifact_id: artifactId }), "artifact");
    }

    return db.insertDurable(
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
    const run = await this.requireByQuery(db, app.agent_runs.where({ run_id: input.runId }), "agent run");
    const snapshotId = input.snapshotId ?? randomUUID();
    const existing = await db.one(app.workspace_snapshots.where({ snapshot_id: snapshotId }));

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

    return db.insertDurable(
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
    const existing = await db.one(app.agent_state_snapshots.where({ snapshot_id: snapshotId }));

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

    return db.insertDurable(
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
      { tier: this.writeTier },
    );
  }

  async recordMemoryLink(input: RecordMemoryLinkInput, session?: Session): Promise<MemoryLink> {
    const db = this.getDb(session);
    const run = input.runId ? await this.getRunByExternalId(db, input.runId) : null;
    const item =
      input.runId && input.itemId ? await this.getItemByExternalId(db, input.runId, input.itemId) : null;
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
      return this.requireByQuery(db, app.memory_links.where({ link_id: linkId }), "memory link");
    }

    return db.insertDurable(
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

  async recordSourceFile(input: RecordSourceFileInput, session?: Session): Promise<SourceFile> {
    const db = this.getDb(session);
    const run = input.runId ? await this.getRunByExternalId(db, input.runId) : null;
    const sourceFileId = input.sourceFileId ?? randomUUID();
    const existing = await db.one(app.source_files.where({ source_file_id: sourceFileId }));

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

    return db.insertDurable(
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

    return db.insertDurable(
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

  async getTaskRecord(taskId: string, session?: Session): Promise<TaskRecord | null> {
    return this.getTaskByExternalId(this.getDb(session), taskId);
  }

  async listTaskRecords(
    input: ListTaskRecordsInput = {},
    session?: Session,
  ): Promise<TaskRecord[]> {
    const db = this.getDb(session);
    const rawRecords = input.context
      ? await db.all(app.task_records.where({ context: input.context }).orderBy("updated_at", "desc"))
      : await db.all(app.task_records.orderBy("updated_at", "desc"));

    const statuses = input.statuses?.map((value) => value.toLowerCase());
    const priorities = input.priorities?.map((value) => value.toUpperCase());
    const placements = input.placements?.map((value) => value.toLowerCase());

    return rawRecords
      .filter((record) => {
        if (statuses && statuses.length > 0 && !statuses.includes(record.status.toLowerCase())) {
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
      if (!existing || existing.processedAt.getTime() < result.processedAt.getTime()) {
        latestResults.set(result.operationId, result);
      }
    }

    const operations = rows
      .filter((row) => row.event_type === CURSOR_REVIEW_OPERATION_EVENT_TYPE)
      .map((row) => this.cursorReviewOperationFromEvent(row))
      .filter((row): row is CursorReviewOperationRecord => Boolean(row))
      .filter((row) => {
        if (input.repoRoot && row.repoRoot && row.repoRoot !== input.repoRoot) return false;
        if (input.workspaceRoot && row.workspaceRoot && row.workspaceRoot !== input.workspaceRoot) return false;
        const latestResult = latestResults.get(row.operationId);
        if (!input.includeProcessed && latestResult && TERMINAL_CURSOR_REVIEW_RESULT_STATUSES.has(latestResult.status)) {
          return false;
        }
        row.latestResult = latestResult;
        return true;
      })
      .sort((left, right) => left.createdAt.getTime() - right.createdAt.getTime())
      .slice(0, clampLimit(input.limit));

    return operations;
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
          message: input.message,
          classification: input.classification,
          title: input.title,
          description: input.description,
          commitMessage: input.commitMessage,
          todoItems: input.todoItems,
          notes: input.notes,
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
      if (!existing || existing.processedAt.getTime() < result.processedAt.getTime()) {
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
        if (input.conversationHash && row.conversationHash !== input.conversationHash) {
          return false;
        }
        const latestResult = latestResults.get(row.operationId);
        if (!input.includeProcessed && latestResult && TERMINAL_COMMIT_TURN_RESULT_STATUSES.has(latestResult.status)) {
          return false;
        }
        row.latestResult = latestResult;
        return true;
      })
      .sort((left, right) => left.createdAt.getTime() - right.createdAt.getTime())
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
    const heartbeatAt = input.heartbeatAt ? asDate(input.heartbeatAt) : startedAt;
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
    const heartbeatAt = input.heartbeatAt ? asDate(input.heartbeatAt) : new Date();
    const expiresAt = input.expiresAt ? asDate(input.expiresAt) : existing.expiresAt;
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
        if (input.scopePrefix && !claim.scope.startsWith(input.scopePrefix)) return false;
        if (input.ownerSession && claim.ownerSession !== input.ownerSession) return false;
        if (!input.includeReleased && claim.status === "released") return false;
        if (!input.includeExpired && claim.status === "expired") return false;
        return true;
      })
      .sort((left, right) => right.heartbeatAt.getTime() - left.heartbeatAt.getTime())
      .slice(0, clampLimit(input.limit));
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
        if (input.targetSession && digest.targetSession !== input.targetSession) return false;
        if (input.targetConversation && digest.targetConversation !== input.targetConversation) return false;
        if (input.targetConversationHash && digest.targetConversationHash !== input.targetConversationHash) {
          return false;
        }
        if (
          input.targetTurnOrdinal !== undefined
          && digest.targetTurnOrdinal !== input.targetTurnOrdinal
        ) {
          return false;
        }
        if (input.sourceSession && digest.sourceSession !== input.sourceSession) return false;
        if (input.kind && digest.kind !== input.kind) return false;
        if (!input.includeExpired && digest.status === "expired") return false;
        return true;
      })
      .sort((left, right) => right.generatedAt.getTime() - left.generatedAt.getTime())
      .slice(0, clampLimit(input.limit));
  }

  async listRecentRuns(limit?: number, session?: Session): Promise<AgentRun[]> {
    return this.getDb(session).all(
      app.agent_runs.orderBy("started_at", "desc").limit(clampLimit(limit)),
    );
  }

  async listActiveRuns(limit?: number, session?: Session): Promise<AgentRun[]> {
    const recent = await this.listRecentRuns(Math.max(clampLimit(limit), 50), session);
    return recent
      .filter((run) => !TERMINAL_RUN_STATUSES.has(run.status))
      .slice(0, clampLimit(limit));
  }

  async getRunSummary(runId: string, session?: Session): Promise<AgentRunSummary | null> {
    const db = this.getDb(session);
    const run = await this.getRunByExternalId(db, runId);
    if (!run) return null;

    const [items, semanticEvents, wireEvents, artifacts, workspaceSnapshots, memoryLinks, sourceFiles] =
      await Promise.all([
        db.all(app.run_items.where({ run_id: runId }).orderBy("sequence", "asc")),
        db.all(app.semantic_events.where({ run_id: runId }).orderBy("occurred_at", "asc")),
        db.all(app.wire_events.where({ run_id: runId }).orderBy("occurred_at", "asc")),
        db.all(app.artifacts.where({ run_id: runId }).orderBy("created_at", "asc")),
        db.all(app.workspace_snapshots.where({ run_id: runId }).orderBy("captured_at", "desc")),
        db.all(app.memory_links.where({ run_id: runId }).orderBy("created_at", "asc")),
        db.all(app.source_files.where({ run_id: runId }).orderBy("created_at", "desc")),
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
    return session ? this.context.forSession(session, app) : this.context.db(app);
  }

  private async getAgentByExternalId(db: Db, agentId: string): Promise<Agent | null> {
    return db.one(app.agents.where({ agent_id: agentId }));
  }

  private async getRunByExternalId(db: Db, runId: string): Promise<AgentRun | null> {
    return db.one(app.agent_runs.where({ run_id: runId }));
  }

  private async getItemByExternalId(db: Db, runId: string, itemId: string): Promise<RunItem | null> {
    return db.one(app.run_items.where({ run_id: runId, item_id: itemId }));
  }

  private async getTaskByExternalId(db: Db, taskId: string): Promise<TaskRecord | null> {
    return db.one(app.task_records.where({ task_id: taskId }));
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
    const operationId = readObjectString(payload, "operationId") ?? event.event_id;
    const operationType = readObjectString(payload, "operationType");
    if (
      operationType !== "focus-branch-review"
      && operationType !== "refresh-branch-review"
      && operationType !== "copy-branch-review-prompt"
      && operationType !== "open-branch-review-chat"
      && operationType !== "show-branch-diff"
      && operationType !== "open-branch-file-diff"
    ) {
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
      note: readObjectString(payload, "note") ?? event.summary_text ?? undefined,
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
    if (status !== "completed" && status !== "failed" && status !== "ignored") return null;
    return {
      eventId: event.event_id,
      operationId,
      status,
      clientId: readObjectString(payload, "clientId"),
      repoRoot: readObjectString(payload, "repoRoot"),
      message: readObjectString(payload, "message") ?? event.summary_text ?? undefined,
      processedAt: event.occurred_at,
    };
  }

  private commitTurnOperationFromEvent(
    event: SemanticEvent,
  ): CommitTurnOperationRecord | null {
    if (event.event_type !== COMMIT_TURN_OPERATION_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const operationId = readObjectString(payload, "operationId") ?? event.event_id;
    const provider = readObjectString(payload, "provider");
    const sessionId = readObjectString(payload, "sessionId");
    const conversation = readObjectString(payload, "conversation");
    const conversationHash = readObjectString(payload, "conversationHash");
    const trigger = readObjectString(payload, "trigger");
    const sessionEventId = readObjectString(payload, "sessionEventId");
    const turnOrdinalRaw = payload?.turnOrdinal;
    const turnOrdinal = typeof turnOrdinalRaw === "number" ? turnOrdinalRaw : null;
    if (!provider || !sessionId || !conversation || !conversationHash || !trigger || !sessionEventId || !turnOrdinal) {
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
      promptPreview: readObjectString(payload, "promptPreview") ?? event.summary_text ?? undefined,
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
    if (status !== "completed" && status !== "failed" && status !== "ignored") return null;
    return {
      eventId: event.event_id,
      operationId,
      status,
      agentId: readObjectString(payload, "agentId"),
      runId: readObjectString(payload, "runId"),
      threadId: readObjectString(payload, "threadId"),
      repoRoot: readObjectString(payload, "repoRoot"),
      message: readObjectString(payload, "message") ?? event.summary_text ?? undefined,
      classification: readObjectString(payload, "classification"),
      title: readObjectString(payload, "title"),
      description: readObjectString(payload, "description"),
      commitMessage: readObjectString(payload, "commitMessage"),
      todoItems: readObjectStringArray(payload, "todoItems"),
      notes: readObjectString(payload, "notes"),
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
    const heartbeatAt = readObjectDate(payload, "heartbeatAt") ?? event.occurred_at;
    const releasedAt = readObjectDate(payload, "releasedAt");
    const status = readObjectString(payload, "status");
    if (!scope || !owner || !startedAt || !expiresAt || !heartbeatAt) return null;
    if (status !== "active" && status !== "released" && status !== "expired") return null;
    return {
      eventId: event.event_id,
      claimId,
      scope,
      owner,
      ownerSession: readObjectString(payload, "ownerSession"),
      mode: readObjectString(payload, "mode"),
      note: readObjectString(payload, "note") ?? event.summary_text ?? undefined,
      repoRoot: readObjectString(payload, "repoRoot"),
      workspaceRoot: readObjectString(payload, "workspaceRoot"),
      startedAt,
      expiresAt,
      heartbeatAt,
      releasedAt,
      status,
    };
  }

  private contextDigestFromEvent(event: SemanticEvent): ContextDigestRecord | null {
    if (event.event_type !== CONTEXT_DIGEST_EVENT_TYPE) return null;
    const payload = asObjectRecord(event.payload_json);
    const digestId = readObjectString(payload, "digestId") ?? event.event_id;
    const targetProvider = readObjectString(payload, "targetProvider");
    const targetSession = readObjectString(payload, "targetSession");
    const targetTurnOrdinal = readObjectNumber(payload, "targetTurnOrdinal");
    const targetConversation = readObjectString(payload, "targetConversation");
    const targetConversationHash = readObjectString(payload, "targetConversationHash");
    const sourceSession = readObjectString(payload, "sourceSession");
    const sourceWatermarkKind = readObjectString(payload, "sourceWatermarkKind");
    const sourceWatermarkValue = readObjectString(payload, "sourceWatermarkValue");
    const kind = readObjectString(payload, "kind");
    const digestText = readObjectString(payload, "digestText");
    const generatedAt = readObjectDate(payload, "generatedAt") ?? event.occurred_at;
    const status = readObjectString(payload, "status");
    if (
      !targetProvider
      || !targetSession
      || targetTurnOrdinal === undefined
      || !targetConversation
      || !targetConversationHash
      || !sourceSession
      || !sourceWatermarkKind
      || !sourceWatermarkValue
      || !kind
      || !digestText
      || !generatedAt
    ) {
      return null;
    }
    if (status !== "ready" && status !== "superseded" && status !== "expired" && status !== "error") {
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
      sourceConversationHash: readObjectString(payload, "sourceConversationHash"),
      kind,
      digestText,
      modelUsed: readObjectString(payload, "modelUsed"),
      score: readObjectNumber(payload, "score"),
      confidence: readObjectString(payload, "confidence"),
      reason: readObjectString(payload, "reason") ?? event.summary_text ?? undefined,
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

  private async requireItemByExternalId(db: Db, runId: string, itemId: string): Promise<RunItem> {
    const item = await this.getItemByExternalId(db, runId, itemId);
    if (!item) {
      throw new Error(`Run item not found for run_id=${runId} item_id=${itemId}`);
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
    await db.updateDurable(table as never, id, payload as never, { tier: this.writeTier });
  }
}

export function createAgentDataStore(config: AgentDataStoreConfig): AgentDataStore {
  const tier = config.tier ?? "edge";
  const context = createJazzContext({
    appId: config.appId ?? DEFAULT_APP_ID,
    app,
    permissions: {},
    driver: { type: "persistent", dataPath: config.dataPath },
    env: config.env ?? "dev",
    userBranch: config.userBranch ?? "main",
    serverUrl: config.serverUrl,
    serverPathPrefix: config.serverPathPrefix,
    backendSecret: config.backendSecret,
    adminSecret: config.adminSecret,
    tier,
  });
  return new AgentDataStore(context, tier);
}
