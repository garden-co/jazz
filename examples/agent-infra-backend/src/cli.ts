import { readFileSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import {
  type AgentRun,
  type AgentRunSummary,
  type AgentStateSnapshot,
  type Artifact,
  createAgentDataStore,
  type ListTaskRecordsInput,
  type MemoryLink,
  type RecordArtifactInput,
  type RecordItemCompletedInput,
  type RecordItemStartedInput,
  type RecordRunCompletedInput,
  type RecordRunStartedInput,
  type RecordWorkspaceSnapshotInput,
  type RunItem,
  type SemanticEvent,
  type SourceFile,
  type TaskRecord,
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

interface SerializedAgentStateSnapshot {
  snapshotId: string;
  agentId: string;
  stateVersion: number | null;
  status: string | null;
  stateJson: unknown | null;
  capturedAt: string;
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
    throw new Error(`${command} accepts only one of --input-json or --input-file`);
  }

  const text = inputFile
    ? readFileSync(expandHomePath(inputFile), "utf8")
    : inlineJson
      ? inlineJson
      : !process.stdin.isTTY
        ? readFileSync(0, "utf8")
        : null;

  if (!text) {
    throw new Error(`${command} requires --input-json, --input-file, or stdin JSON`);
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

function serializeWorkspaceSnapshot(snapshot: WorkspaceSnapshot): SerializedWorkspaceSnapshot {
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

function serializeRunSummary(summary: AgentRunSummary): SerializedAgentRunSummary {
  return {
    run: serializeAgentRun(summary.run),
    items: summary.items.map(serializeRunItem),
    semanticEvents: summary.semanticEvents.map(serializeSemanticEvent),
    wireEvents: summary.wireEvents.map(serializeWireEvent),
    artifacts: summary.artifacts.map(serializeArtifact),
    workspaceSnapshots: summary.workspaceSnapshots.map(serializeWorkspaceSnapshot),
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

async function main(): Promise<void> {
  const command = requireCommand();
  const dataPath = expandHomePath(readFlag("--data-path") ?? "~/.jazz2/agent-infra.db");
  await mkdir(path.dirname(dataPath), { recursive: true });

  const store = createAgentDataStore({
    appId: "run-agent-infra",
    dataPath,
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
        const input = readJsonInput<RecordRunStartedInput>("record-run-started");
        const run = await store.recordRunStarted(input);
        renderJson(serializeAgentRun(run));
        break;
      }
      case "record-run-completed": {
        const input = readJsonInput<RecordRunCompletedInput>("record-run-completed");
        const run = await store.recordRunCompleted(input);
        renderJson(serializeAgentRun(run));
        break;
      }
      case "record-item-started": {
        const input = readJsonInput<RecordItemStartedInput>("record-item-started");
        const item = await store.recordItemStarted(input);
        renderJson(serializeRunItem(item));
        break;
      }
      case "record-item-completed": {
        const input = readJsonInput<RecordItemCompletedInput>("record-item-completed");
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
