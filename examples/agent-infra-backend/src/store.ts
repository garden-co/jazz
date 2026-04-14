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
const TERMINAL_RUN_STATUSES = new Set(["completed", "failed", "cancelled", "error"]);
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

type DurabilityTier = "worker" | "edge" | "global";
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
