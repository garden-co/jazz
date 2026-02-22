import { describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { deriveLocalPrincipalId } from "../../src/runtime/client-session.js";
import { translateQuery } from "../../src/runtime/query-adapter.js";
import { ADMIN_SECRET, APP_ID, JWT_SECRET, TEST_PORT } from "./test-constants.js";

import schemaJson from "../../../../benchmarks/realistic/schema/project_board.schema.json";
import profileJson from "../../../../benchmarks/realistic/profiles/s.json";
import w1Json from "../../../../benchmarks/realistic/scenarios/w1_interactive.json";
import w3Json from "../../../../benchmarks/realistic/scenarios/w3_offline_reconnect.json";
import w4Json from "../../../../benchmarks/realistic/scenarios/w4_cold_start.json";
import b1Json from "../../../../benchmarks/realistic/scenarios/b1_server_crud_sustained.json";
import b2Json from "../../../../benchmarks/realistic/scenarios/b2_server_reads_sustained.json";
import b3Json from "../../../../benchmarks/realistic/scenarios/b3_server_cold_load_large.json";
import b4Json from "../../../../benchmarks/realistic/scenarios/b4_server_fanout_updates.json";
import b5Json from "../../../../benchmarks/realistic/scenarios/b5_server_permission_recursive.json";
import b6Json from "../../../../benchmarks/realistic/scenarios/b6_server_hotspot_history.json";

type PersistenceTier = "worker" | "edge" | "core";

interface ProfileConfig {
  id: string;
  seed: number;
  users: number;
  organizations: number;
  projects: number;
  tasks: number;
  comments: number;
  watchers_per_task: number;
  activity_events: number;
  hot_project_fraction: number;
}

interface W1Scenario {
  id: string;
  name: string;
  seed: number;
  operation_count: number;
  mix: Array<{ operation: string; weight: number }>;
}

interface W3Scenario {
  id: string;
  name: string;
  seed: number;
  offline_write_count: number;
  timeout_seconds: number;
}

interface W4Scenario {
  id: string;
  name: string;
  seed: number;
  reopen_cycles: number;
}

interface B1Scenario {
  id: string;
  name: string;
  seed: number;
  insert_count: number;
  update_count: number;
  delete_count: number;
}

interface B2Scenario {
  id: string;
  name: string;
  seed: number;
  request_count: number;
  mix: Array<{ operation: string; weight: number }>;
}

interface B3Scenario {
  id: string;
  name: string;
  seed: number;
  reopen_cycles: number;
  large_multiplier: number;
}

interface B4Scenario {
  id: string;
  name: string;
  seed: number;
  subscriber_counts: number[];
  rounds: number;
  timeout_seconds: number;
}

interface B5Scenario {
  id: string;
  name: string;
  seed: number;
  folders: number;
  documents: number;
  read_request_count: number;
  update_attempt_count: number;
  allow_fraction: number;
  recursive_depth: number;
}

interface B6Scenario {
  id: string;
  name: string;
  seed: number;
  hot_task_count: number;
  update_count: number;
}

interface UserRow {
  id: string;
  display_name: string;
  email: string;
}

interface OrganizationRow {
  id: string;
  name: string;
  created_at: number;
}

interface MembershipRow {
  id: string;
  organization_id: string;
  user_id: string;
  role: string;
}

interface ProjectRow {
  id: string;
  organization_id: string;
  name: string;
  archived: boolean;
  updated_at: number;
}

interface TaskRow {
  id: string;
  project_id: string;
  title: string;
  status: string;
  priority: number;
  assignee_id: string;
  updated_at: number;
  due_at: number | null;
}

interface CommentRow {
  id: string;
  task_id: string;
  author_id: string;
  body: string;
  created_at: number;
}

interface TaskWatcherRow {
  id: string;
  task_id: string;
  user_id: string;
}

interface ActivityRow {
  id: string;
  project_id: string;
  task_id: string | null;
  actor_id: string;
  kind: string;
  created_at: number;
  payload: string;
}

interface PermissionFolderRow {
  id: string;
  parent_id: string | null;
  owner_id: string;
  title: string;
  updated_at: number;
}

interface PermissionDocumentRow {
  id: string;
  folder_id: string;
  body: string;
  revision: number;
  updated_at: number;
}

interface SeedState {
  users: string[];
  projects: string[];
  taskIds: string[];
  taskProjectIdx: number[];
  commentsPerTask: number[];
  hotProjectCount: number;
}

interface OpSummary {
  count: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
}

interface RuntimeRow {
  id: string;
  values: unknown[];
}

interface RuntimeSession {
  user_id: string;
  claims: Record<string, unknown>;
}

interface InternalPolicyClient {
  queryInternal(
    queryJson: string,
    session?: RuntimeSession,
    settledTier?: "worker" | "edge" | "core",
  ): Promise<RuntimeRow[]>;
  getRuntime(): {
    updateWithSession?: (
      objectId: string,
      values: Record<string, unknown>,
      sessionJson?: string,
    ) => void;
  };
}

interface ScenarioResult {
  scenario_id: string;
  scenario_name: string;
  profile_id: string;
  topology: string;
  total_operations: number;
  wall_time_ms: number;
  throughput_ops_per_sec: number;
  operation_summaries: Record<string, OpSummary>;
  extra: Record<string, unknown>;
}

const schema = schemaJson as unknown as WasmSchema;
const profile = profileJson as unknown as ProfileConfig;
const w1 = w1Json as unknown as W1Scenario;
const w3 = w3Json as unknown as W3Scenario;
const w4 = w4Json as unknown as W4Scenario;
const b1 = b1Json as unknown as B1Scenario;
const b2 = b2Json as unknown as B2Scenario;
const b3 = b3Json as unknown as B3Scenario;
const b4 = b4Json as unknown as B4Scenario;
const b5 = b5Json as unknown as B5Scenario;
const b6 = b6Json as unknown as B6Scenario;

const usersTable = tableProxy<UserRow, Omit<UserRow, "id">>("users");
const organizationsTable = tableProxy<OrganizationRow, Omit<OrganizationRow, "id">>(
  "organizations",
);
const membershipsTable = tableProxy<MembershipRow, Omit<MembershipRow, "id">>("memberships");
const projectsTable = tableProxy<ProjectRow, Omit<ProjectRow, "id">>("projects");
const tasksTable = tableProxy<TaskRow, Omit<TaskRow, "id">>("tasks");
const commentsTable = tableProxy<CommentRow, Omit<CommentRow, "id">>("task_comments");
const taskWatchersTable = tableProxy<TaskWatcherRow, Omit<TaskWatcherRow, "id">>("task_watchers");
const activityTable = tableProxy<ActivityRow, Omit<ActivityRow, "id">>("activity_events");

class Lcg {
  private state: bigint;

  constructor(seed: number) {
    this.state = BigInt(seed >>> 0) | 1n;
  }

  nextU64(): bigint {
    this.state = (this.state * 6364136223846793005n + 1442695040888963407n) & ((1n << 64n) - 1n);
    return this.state;
  }

  nextInt(upper: number): number {
    if (upper <= 1) return 0;
    return Number(this.nextU64() % BigInt(upper));
  }

  pickWeightedIndex(weights: number[]): number {
    const total = weights.reduce((sum, w) => sum + w, 0);
    if (total <= 0) return 0;
    let cursor = this.nextInt(total);
    for (let i = 0; i < weights.length; i += 1) {
      if (cursor < weights[i]) return i;
      cursor -= weights[i];
    }
    return Math.max(0, weights.length - 1);
  }
}

function tableProxy<T, Init>(table: string, tableSchema: WasmSchema = schema): TableProxy<T, Init> {
  return {
    _table: table,
    _schema: tableSchema,
    _rowType: {} as T,
    _initType: {} as Init,
  };
}

function query<T>(
  table: string,
  conditions: Array<{ column: string; op: string; value: unknown }> = [],
  orderBy: Array<[string, "asc" | "desc"]> = [],
  limit?: number,
  querySchema: WasmSchema = schema,
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: querySchema,
    _rowType: {} as T,
    _build() {
      return JSON.stringify({
        table,
        conditions,
        includes: {},
        orderBy,
        limit,
        offset: 0,
      });
    },
  };
}

function nowMicros(): number {
  return Date.now() * 1000;
}

function uniqueDbName(label: string): string {
  return `realistic-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function summarizeLatencies(values: number[]): OpSummary {
  if (values.length === 0) {
    return { count: 0, avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const at = (p: number): number => {
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1));
    return sorted[idx];
  };
  const avg = sorted.reduce((sum, x) => sum + x, 0) / sorted.length;
  return {
    count: sorted.length,
    avg_ms: avg,
    p50_ms: at(0.5),
    p95_ms: at(0.95),
    p99_ms: at(0.99),
  };
}

function progressLog(message: string): void {
  // eslint-disable-next-line no-console
  console.log(`[realistic-progress] ${message}`);
}

function reportLoopProgress(label: string, index: number, total: number): void {
  if (total <= 0) return;
  const step = Math.max(1, Math.floor(total / 4));
  if (index > 0 && index % step === 0) {
    progressLog(`${label}: ${index}/${total}`);
  }
}

function policyClient(db: Db, policySchema: WasmSchema): InternalPolicyClient {
  const internal = db as unknown as {
    getClient(schema: WasmSchema): InternalPolicyClient;
  };
  return internal.getClient(policySchema);
}

function scaledProfile(input: ProfileConfig): ProfileConfig {
  // Browser benchmark pass keeps relational shape but scales volume down
  // so W1/W3/W4 complete reliably in CI-like environments.
  const tasks = Math.max(100, Math.floor(input.tasks * 0.03));
  const comments = Math.max(tasks, Math.floor(input.comments * 0.03));
  const activity_events = Math.max(tasks, Math.floor(input.activity_events * 0.03));
  return {
    ...input,
    tasks,
    comments,
    activity_events,
  };
}

function scaledLargeProfile(input: ProfileConfig, multiplier: number): ProfileConfig {
  const base = scaledProfile(input);
  const factor = Math.max(1, Math.floor(multiplier));
  const tasks = Math.min(4000, base.tasks * factor);
  const comments = Math.min(16000, Math.max(tasks, base.comments * factor));
  const activity_events = Math.min(12000, Math.max(tasks, base.activity_events * factor));
  return {
    ...base,
    id: `${base.id}_L`,
    tasks,
    comments,
    activity_events,
  };
}

async function createServerDb(
  dbName: string,
  sub: string,
  claims: Record<string, unknown> = {},
  options: {
    includeAdminSecret?: boolean;
    includeJwt?: boolean;
    localAuthMode?: "anonymous" | "demo";
    localAuthToken?: string;
  } = {},
): Promise<Db> {
  const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
  const config: Parameters<typeof createDb>[0] = {
    appId: APP_ID,
    dbName,
    serverUrl,
    adminSecret: options.includeAdminSecret === false ? undefined : ADMIN_SECRET,
    logLevel: "warn",
  };
  if (options.includeJwt !== false) {
    config.jwtToken = await signJwt(sub, JWT_SECRET, claims);
  }
  if (options.localAuthMode) {
    config.localAuthMode = options.localAuthMode;
  }
  if (options.localAuthToken) {
    config.localAuthToken = options.localAuthToken;
  }
  return createDb(config);
}

async function waitForCondition(
  description: string,
  timeoutMs: number,
  condition: () => boolean,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function storageUsageBytes(): Promise<number | null> {
  if (typeof navigator === "undefined" || !navigator.storage?.estimate) return null;
  const estimate = await navigator.storage.estimate();
  return typeof estimate.usage === "number" ? estimate.usage : null;
}

async function seedDataset(db: Db, config: ProfileConfig): Promise<SeedState> {
  progressLog(
    `seed start profile=${config.id} users=${config.users} orgs=${config.organizations} projects=${config.projects} tasks=${config.tasks} comments=${config.comments}`,
  );
  const users: string[] = [];
  const organizations: string[] = [];
  const projects: string[] = [];
  const taskIds: string[] = [];
  const taskProjectIdx: number[] = [];
  const commentsPerTask = new Array<number>(config.tasks).fill(0);

  const ts = nowMicros();
  for (let i = 0; i < config.users; i += 1) {
    reportLoopProgress("seed users", i, config.users);
    const id = db.insert(usersTable, {
      display_name: `User ${i}`,
      email: `user${i}@bench.test`,
    });
    users.push(id);
  }

  for (let i = 0; i < config.organizations; i += 1) {
    reportLoopProgress("seed organizations", i, config.organizations);
    const id = db.insert(organizationsTable, {
      name: `Org ${i}`,
      created_at: ts + i,
    });
    organizations.push(id);
  }

  for (let i = 0; i < config.users; i += 1) {
    reportLoopProgress("seed memberships", i, config.users);
    db.insert(membershipsTable, {
      organization_id: organizations[i % organizations.length],
      user_id: users[i],
      role: i % 9 === 0 ? "admin" : "member",
    });
  }

  for (let i = 0; i < config.projects; i += 1) {
    reportLoopProgress("seed projects", i, config.projects);
    const id = db.insert(projectsTable, {
      organization_id: organizations[i % organizations.length],
      name: `Project ${i}`,
      archived: false,
      updated_at: ts + i,
    });
    projects.push(id);
  }

  const statuses = ["todo", "in_progress", "review", "done"] as const;
  for (let i = 0; i < config.tasks; i += 1) {
    reportLoopProgress("seed tasks", i, config.tasks);
    const projectIdx = i % projects.length;
    const assigneeIdx = i % users.length;
    const id = db.insert(tasksTable, {
      project_id: projects[projectIdx],
      title: `Task ${i}`,
      status: statuses[i % statuses.length],
      priority: 1 + (i % 4),
      assignee_id: users[assigneeIdx],
      updated_at: ts + i,
      due_at: ts + i * 11,
    });
    taskIds.push(id);
    taskProjectIdx.push(projectIdx);
  }

  for (let i = 0; i < config.comments; i += 1) {
    reportLoopProgress("seed comments", i, config.comments);
    const taskIdx = i % taskIds.length;
    db.insert(commentsTable, {
      task_id: taskIds[taskIdx],
      author_id: users[(i * 7) % users.length],
      body: `Comment ${i} body`,
      created_at: ts + i,
    });
    commentsPerTask[taskIdx] += 1;
  }

  for (let taskIdx = 0; taskIdx < taskIds.length; taskIdx += 1) {
    reportLoopProgress("seed task_watchers", taskIdx, taskIds.length);
    for (let w = 0; w < config.watchers_per_task; w += 1) {
      db.insert(taskWatchersTable, {
        task_id: taskIds[taskIdx],
        user_id: users[(taskIdx + w) % users.length],
      });
    }
  }

  for (let i = 0; i < config.activity_events; i += 1) {
    reportLoopProgress("seed activity_events", i, config.activity_events);
    const taskIdx = i % taskIds.length;
    db.insert(activityTable, {
      project_id: projects[taskProjectIdx[taskIdx]],
      task_id: taskIds[taskIdx],
      actor_id: users[(i * 11) % users.length],
      kind: i % 3 === 0 ? "task_updated" : "comment_added",
      created_at: ts + i,
      payload: `{"event":${i}}`,
    });
  }

  const hotProjectCount = Math.max(1, Math.round(config.projects * config.hot_project_fraction));
  progressLog("seed complete");
  return {
    users,
    projects,
    taskIds,
    taskProjectIdx,
    commentsPerTask,
    hotProjectCount,
  };
}

async function runW1(db: Db, config: ProfileConfig, state: SeedState): Promise<ScenarioResult> {
  const operationCount = Math.min(w1.operation_count, 60);
  progressLog(`W1 start operations=${operationCount}`);
  const rng = new Lcg(w1.seed ^ config.seed);
  const weights = w1.mix.map((x) => x.weight);
  const latencies: Record<string, number[]> = {};

  const wallStart = performance.now();
  for (let i = 0; i < operationCount; i += 1) {
    const step = Math.max(1, Math.floor(operationCount / 5));
    if (i > 0 && i % step === 0) progressLog(`W1 progress ${i}/${operationCount}`);
    const op = w1.mix[rng.pickWeightedIndex(weights)].operation;
    const t0 = performance.now();
    switch (op) {
      case "query_board": {
        const project = state.projects[rng.nextInt(state.hotProjectCount)];
        await db.all(
          query<TaskRow>(
            "tasks",
            [{ column: "project_id", op: "eq", value: project }],
            [["updated_at", "desc"]],
            200,
          ),
          "worker",
        );
        break;
      }
      case "query_my_work": {
        const assignee = state.users[rng.nextInt(state.users.length)];
        await db.all(
          query<TaskRow>(
            "tasks",
            [
              { column: "assignee_id", op: "eq", value: assignee },
              { column: "status", op: "eq", value: "in_progress" },
            ],
            [["updated_at", "desc"]],
            200,
          ),
          "worker",
        );
        break;
      }
      case "query_task_detail": {
        const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
        await db.all(
          query<CommentRow>(
            "task_comments",
            [{ column: "task_id", op: "eq", value: taskId }],
            [["created_at", "desc"]],
            200,
          ),
          "worker",
        );
        await db.all(
          query<ActivityRow>(
            "activity_events",
            [{ column: "task_id", op: "eq", value: taskId }],
            [["created_at", "desc"]],
            200,
          ),
          "worker",
        );
        break;
      }
      case "update_task_status": {
        const taskIdx = rng.nextInt(state.taskIds.length);
        db.update(tasksTable, state.taskIds[taskIdx], {
          status: ["todo", "in_progress", "review", "done"][rng.nextInt(4)],
          priority: 1 + rng.nextInt(4),
          assignee_id: state.users[rng.nextInt(state.users.length)],
          updated_at: nowMicros(),
        });
        break;
      }
      case "insert_comment": {
        const taskIdx = rng.nextInt(state.taskIds.length);
        db.insert(commentsTable, {
          task_id: state.taskIds[taskIdx],
          author_id: state.users[rng.nextInt(state.users.length)],
          body: `interactive comment ${i}`,
          created_at: nowMicros(),
        });
        state.commentsPerTask[taskIdx] += 1;
        break;
      }
      case "update_project_meta": {
        const projectIdx = rng.nextInt(state.projects.length);
        db.update(projectsTable, state.projects[projectIdx], {
          name: `Project ${projectIdx} v${i}`,
          updated_at: nowMicros(),
        });
        break;
      }
      default:
        throw new Error(`Unknown operation in W1 mix: ${op}`);
    }
    const elapsed = performance.now() - t0;
    (latencies[op] ||= []).push(elapsed);
  }
  const wallMs = performance.now() - wallStart;

  const operation_summaries: Record<string, OpSummary> = {};
  for (const [op, samples] of Object.entries(latencies)) {
    operation_summaries[op] = summarizeLatencies(samples);
  }

  return {
    scenario_id: w1.id,
    scenario_name: w1.name,
    profile_id: config.id,
    topology: "local_only",
    total_operations: operationCount,
    wall_time_ms: wallMs,
    throughput_ops_per_sec: operationCount / Math.max(0.001, wallMs / 1000),
    operation_summaries,
    extra: {
      hot_projects: state.hotProjectCount,
      dataset: {
        users: config.users,
        projects: config.projects,
        tasks: config.tasks,
        comments: config.comments,
      },
    },
  };
}

async function runW3(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("W3 start");
  const dbName = uniqueDbName("w3");
  const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
  const token = await signJwt("realistic-user", JWT_SECRET);
  const rng = new Lcg(w3.seed ^ config.seed);
  const offlineWrites = Math.min(w3.offline_write_count, 20);
  let offlineDb: Db | null = null;
  let onlineDb: Db | null = null;

  try {
    offlineDb = await createDb({ appId: APP_ID, dbName, logLevel: "warn" });
    const state = await seedDataset(offlineDb, config);
    const targetTaskIdx = rng.nextInt(state.taskIds.length);
    const targetTaskId = state.taskIds[targetTaskIdx];
    const baseline = state.commentsPerTask[targetTaskIdx];

    const offlineWriteStart = performance.now();
    for (let i = 0; i < offlineWrites; i += 1) {
      await offlineDb.insertPersisted(
        commentsTable,
        {
          task_id: targetTaskId,
          author_id: state.users[rng.nextInt(state.users.length)],
          body: `offline_reconnect_marker_${i}`,
          created_at: nowMicros(),
        },
        "worker",
      );
    }
    const offlineWriteMs = performance.now() - offlineWriteStart;
    await offlineDb.shutdown();
    offlineDb = null;

    const reconnectStart = performance.now();
    onlineDb = await createDb({
      appId: APP_ID,
      dbName,
      serverUrl,
      jwtToken: token,
      adminSecret: ADMIN_SECRET,
      logLevel: "warn",
    });

    const target = baseline + offlineWrites;
    const timeoutAt = performance.now() + Math.min(w3.timeout_seconds, 20) * 1000;
    let observed = 0;
    let polls = 0;
    while (performance.now() < timeoutAt) {
      const rows = await onlineDb.all(
        query<CommentRow>("task_comments", [{ column: "task_id", op: "eq", value: targetTaskId }]),
        "edge",
      );
      observed = rows.length;
      polls += 1;
      if (polls % 5 === 0) {
        progressLog(`W3 polling observed=${observed} target=${target} polls=${polls}`);
      }
      if (observed >= target) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    if (observed < target) {
      throw new Error(
        `W3 timed out waiting for edge settlement (observed=${observed}, target=${target})`,
      );
    }
    const reconnectMs = performance.now() - reconnectStart;

    return {
      scenario_id: w3.id,
      scenario_name: w3.name,
      profile_id: config.id,
      topology: "single_hop",
      total_operations: offlineWrites + 1,
      wall_time_ms: offlineWriteMs + reconnectMs,
      throughput_ops_per_sec:
        (offlineWrites + 1) / Math.max(0.001, (offlineWriteMs + reconnectMs) / 1000),
      operation_summaries: {
        offline_writes: {
          count: offlineWrites,
          avg_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p50_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p95_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p99_ms: offlineWriteMs / Math.max(1, offlineWrites),
        },
        reconnect_settlement: {
          count: 1,
          avg_ms: reconnectMs,
          p50_ms: reconnectMs,
          p95_ms: reconnectMs,
          p99_ms: reconnectMs,
        },
      },
      extra: {
        target_task_id: targetTaskId,
        baseline_comments: baseline,
        target_comments_after_reconnect: target,
        observed_comments_after_reconnect: observed,
        poll_iterations: polls,
      },
    };
  } finally {
    if (offlineDb) await offlineDb.shutdown();
    if (onlineDb) await onlineDb.shutdown();
  }
}

async function runW4(config: ProfileConfig): Promise<ScenarioResult> {
  const dbName = uniqueDbName("w4");
  const cycles = Math.min(w4.reopen_cycles, 3);
  progressLog(`W4 start cycles=${cycles}`);
  let db: Db | null = null;
  const latencies: number[] = [];

  try {
    db = await createDb({ appId: APP_ID, dbName, logLevel: "warn" });
    const state = await seedDataset(db, config);
    const hotProjectId = state.projects[0];
    await db.all(
      query<TaskRow>(
        "tasks",
        [{ column: "project_id", op: "eq", value: hotProjectId }],
        [["updated_at", "desc"]],
        200,
      ),
      "worker",
    );
    await db.shutdown();
    db = null;

    const wallStart = performance.now();
    for (let i = 0; i < cycles; i += 1) {
      progressLog(`W4 cycle ${i + 1}/${cycles}`);
      const t0 = performance.now();
      db = await createDb({ appId: APP_ID, dbName, logLevel: "warn" });
      await db.all(
        query<TaskRow>(
          "tasks",
          [{ column: "project_id", op: "eq", value: hotProjectId }],
          [["updated_at", "desc"]],
          200,
        ),
        "worker",
      );
      await db.shutdown();
      db = null;
      latencies.push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;

    return {
      scenario_id: w4.id,
      scenario_name: w4.name,
      profile_id: config.id,
      topology: "local_only",
      total_operations: cycles,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: cycles / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        cold_reopen: summarizeLatencies(latencies),
      },
      extra: { cycles },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB1(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B1 start");
  const dbName = uniqueDbName("b1");
  const rng = new Lcg(b1.seed ^ config.seed);
  const insertCount = Math.min(b1.insert_count, 96);
  const updateCount = Math.min(b1.update_count, 96);
  const deleteCount = Math.min(b1.delete_count, insertCount);
  const insertedCommentIds: string[] = [];
  const latencies: Record<string, number[]> = {};

  let db: Db | null = null;
  try {
    db = await createServerDb(dbName, "realistic-b1");
    const state = await seedDataset(db, config);
    const wallStart = performance.now();

    for (let i = 0; i < insertCount; i += 1) {
      reportLoopProgress("B1 inserts", i, insertCount);
      const taskIdx = rng.nextInt(state.taskIds.length);
      const t0 = performance.now();
      const id = db.insert(commentsTable, {
        task_id: state.taskIds[taskIdx],
        author_id: state.users[rng.nextInt(state.users.length)],
        body: `b1_insert_comment_${i}`,
        created_at: nowMicros(),
      });
      insertedCommentIds.push(id);
      (latencies.insert_sync ||= []).push(performance.now() - t0);
    }

    for (let i = 0; i < updateCount; i += 1) {
      reportLoopProgress("B1 updates", i, updateCount);
      const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
      const t0 = performance.now();
      db.update(tasksTable, taskId, {
        priority: 1 + rng.nextInt(4),
        status: ["todo", "in_progress", "review", "done"][rng.nextInt(4)],
        updated_at: nowMicros(),
      });
      (latencies.update_sync ||= []).push(performance.now() - t0);
    }

    for (let i = 0; i < deleteCount; i += 1) {
      reportLoopProgress("B1 deletes", i, deleteCount);
      const id = insertedCommentIds[i];
      const t0 = performance.now();
      db.deleteFrom(commentsTable, id);
      (latencies.delete_sync ||= []).push(performance.now() - t0);
    }

    const wallMs = performance.now() - wallStart;
    const totalOperations = insertCount + updateCount + deleteCount;
    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples);
    }

    return {
      scenario_id: b1.id,
      scenario_name: b1.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: totalOperations,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: totalOperations / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        inserts: insertCount,
        updates: updateCount,
        deletes: deleteCount,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB2(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B2 start");
  const dbName = uniqueDbName("b2");
  const rng = new Lcg(b2.seed ^ config.seed);
  const requestCount = Math.min(b2.request_count, 160);
  const weights = b2.mix.map((x) => x.weight);
  const latencies: Record<string, number[]> = {};

  let db: Db | null = null;
  try {
    db = await createServerDb(dbName, "realistic-b2");
    const state = await seedDataset(db, config);

    const wallStart = performance.now();
    for (let i = 0; i < requestCount; i += 1) {
      reportLoopProgress("B2 reads", i, requestCount);
      const op = b2.mix[rng.pickWeightedIndex(weights)].operation;
      const t0 = performance.now();
      switch (op) {
        case "query_board": {
          const project = state.projects[rng.nextInt(state.hotProjectCount)];
          await db.all(
            query<TaskRow>(
              "tasks",
              [{ column: "project_id", op: "eq", value: project }],
              [["updated_at", "desc"]],
              200,
            ),
          );
          break;
        }
        case "query_my_work": {
          const assignee = state.users[rng.nextInt(state.users.length)];
          await db.all(
            query<TaskRow>(
              "tasks",
              [
                { column: "assignee_id", op: "eq", value: assignee },
                { column: "status", op: "eq", value: "in_progress" },
              ],
              [["updated_at", "desc"]],
              200,
            ),
          );
          break;
        }
        case "query_task_detail": {
          const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
          await db.all(
            query<CommentRow>(
              "task_comments",
              [{ column: "task_id", op: "eq", value: taskId }],
              [["created_at", "desc"]],
              200,
            ),
          );
          await db.all(
            query<ActivityRow>(
              "activity_events",
              [{ column: "task_id", op: "eq", value: taskId }],
              [["created_at", "desc"]],
              200,
            ),
          );
          break;
        }
        default:
          throw new Error(`Unknown operation in B2 mix: ${op}`);
      }
      (latencies[op] ||= []).push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;

    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples);
    }

    return {
      scenario_id: b2.id,
      scenario_name: b2.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: requestCount,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: requestCount / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        read_mix: b2.mix,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB3(config: ProfileConfig): Promise<ScenarioResult> {
  const dbName = uniqueDbName("b3");
  const cycles = Math.min(b3.reopen_cycles, 4);
  const largeConfig = scaledLargeProfile(config, b3.large_multiplier);
  progressLog(
    `B3 start cycles=${cycles} tasks=${largeConfig.tasks} comments=${largeConfig.comments}`,
  );
  const latencies: number[] = [];
  let seedDb: Db | null = null;
  let cycleDb: Db | null = null;

  try {
    seedDb = await createServerDb(dbName, "realistic-b3-seed");
    const state = await seedDataset(seedDb, largeConfig);
    const hotProjectId = state.projects[0];
    await seedDb.all(
      query<TaskRow>(
        "tasks",
        [{ column: "project_id", op: "eq", value: hotProjectId }],
        [["updated_at", "desc"]],
        200,
      ),
    );
    await seedDb.shutdown();
    seedDb = null;

    const wallStart = performance.now();
    for (let i = 0; i < cycles; i += 1) {
      progressLog(`B3 cycle ${i + 1}/${cycles}`);
      const t0 = performance.now();
      cycleDb = await createServerDb(dbName, "realistic-b3-cycle");
      await cycleDb.all(
        query<TaskRow>(
          "tasks",
          [{ column: "project_id", op: "eq", value: hotProjectId }],
          [["updated_at", "desc"]],
          200,
        ),
      );
      await cycleDb.shutdown();
      cycleDb = null;
      latencies.push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;

    return {
      scenario_id: b3.id,
      scenario_name: b3.name,
      profile_id: largeConfig.id,
      topology: "single_hop_browser",
      total_operations: cycles,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: cycles / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        cold_reopen_query: summarizeLatencies(latencies),
      },
      extra: {
        cycles,
        dataset: {
          users: largeConfig.users,
          projects: largeConfig.projects,
          tasks: largeConfig.tasks,
          comments: largeConfig.comments,
          activity_events: largeConfig.activity_events,
        },
      },
    };
  } finally {
    if (seedDb) await seedDb.shutdown();
    if (cycleDb) await cycleDb.shutdown();
  }
}

async function runB4(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B4 start");
  const dbName = uniqueDbName("b4");
  const subscriberCounts = b4.subscriber_counts.map((x) => Math.max(1, Math.min(40, x)));
  const rounds = Math.min(b4.rounds, 8);
  const timeoutMs = Math.min(b4.timeout_seconds, 30) * 1000;
  const fanoutDeliveryLatencies: number[] = [];
  const operationSummaries: Record<string, OpSummary> = {};
  const fanoutElemsPerSec: Record<string, number> = {};
  let writer: Db | null = null;

  try {
    writer = await createServerDb(dbName, "realistic-b4-writer");
    const state = await seedDataset(writer, config);
    const targetTaskId = state.taskIds[0];
    writer.update(tasksTable, targetTaskId, {
      priority: 0,
      updated_at: nowMicros(),
    });

    const wallStart = performance.now();
    let totalRounds = 0;
    for (const subscriberCount of subscriberCounts) {
      progressLog(`B4 fanout subscribers=${subscriberCount}`);
      const unsubscribeFns: Array<() => void> = [];
      const seenAt = new Array<number>(subscriberCount).fill(0);
      let targetPriority = -1;

      try {
        for (let i = 0; i < subscriberCount; i += 1) {
          const unsubscribe = writer.subscribeAll(
            query<TaskRow>("tasks", [{ column: "id", op: "eq", value: targetTaskId }], [], 1),
            (delta) => {
              const row = delta.all[0];
              if (!row) return;
              if (row.priority === targetPriority && seenAt[i] === 0) {
                seenAt[i] = performance.now();
              }
            },
          );
          unsubscribeFns.push(unsubscribe);
        }

        // Warmup update confirms all subscribers are active before measured rounds.
        targetPriority = 9_000 + subscriberCount;
        seenAt.fill(0);
        writer.update(tasksTable, targetTaskId, {
          priority: targetPriority,
          updated_at: nowMicros(),
        });
        await waitForCondition(`fanout warmup n=${subscriberCount}`, timeoutMs, () =>
          seenAt.every((x) => x > 0),
        );

        const perCountLatencies: number[] = [];
        for (let round = 0; round < rounds; round += 1) {
          targetPriority = 10_000 + subscriberCount * 100 + round;
          seenAt.fill(0);

          const t0 = performance.now();
          writer.update(tasksTable, targetTaskId, {
            priority: targetPriority,
            updated_at: nowMicros(),
          });
          await waitForCondition(`fanout n=${subscriberCount} round=${round + 1}`, timeoutMs, () =>
            seenAt.every((x) => x > 0),
          );

          const deliveredAt = Math.max(...seenAt);
          const deliveryMs = Math.max(0, deliveredAt - t0);
          perCountLatencies.push(deliveryMs);
          fanoutDeliveryLatencies.push(deliveryMs);
          totalRounds += 1;
        }

        operationSummaries[`fanout_delivery_n${subscriberCount}`] =
          summarizeLatencies(perCountLatencies);
        const groupWallMs = perCountLatencies.reduce((sum, x) => sum + x, 0);
        fanoutElemsPerSec[`n${subscriberCount}`] =
          (subscriberCount * perCountLatencies.length) / Math.max(0.001, groupWallMs / 1000);
      } finally {
        while (unsubscribeFns.length > 0) {
          const unsubscribe = unsubscribeFns.pop();
          if (unsubscribe) unsubscribe();
        }
      }
    }

    const wallMs = performance.now() - wallStart;
    operationSummaries.fanout_delivery = summarizeLatencies(fanoutDeliveryLatencies);

    return {
      scenario_id: b4.id,
      scenario_name: b4.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: totalRounds,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: totalRounds / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        subscriber_counts: subscriberCounts,
        rounds_per_count: rounds,
        fanout_elems_per_sec: fanoutElemsPerSec,
      },
    };
  } finally {
    if (writer) await writer.shutdown();
  }
}

function permissionRecursiveSchema(recursiveDepth: number): WasmSchema {
  const folderSelectPolicy = {
    using: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Select",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
  };

  const folderUpdatePolicy = {
    using: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Update",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
    with_check: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Update",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
  };

  const documentSelectPolicy = {
    using: {
      type: "Inherits",
      operation: "Select",
      via_column: "folder_id",
      max_depth: recursiveDepth,
    },
  };

  const documentUpdatePolicy = {
    using: {
      type: "Inherits",
      operation: "Update",
      via_column: "folder_id",
      max_depth: recursiveDepth,
    },
    with_check: {
      type: "Inherits",
      operation: "Update",
      via_column: "folder_id",
      max_depth: recursiveDepth,
    },
  };

  return {
    tables: {
      folders: {
        columns: [
          {
            name: "parent_id",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "folders",
          },
          {
            name: "owner_id",
            column_type: { type: "Text" },
            nullable: false,
          },
          {
            name: "title",
            column_type: { type: "Text" },
            nullable: false,
          },
          {
            name: "updated_at",
            column_type: { type: "Timestamp" },
            nullable: false,
          },
        ],
        policies: {
          select: folderSelectPolicy,
          update: folderUpdatePolicy,
        },
      },
      documents: {
        columns: [
          {
            name: "folder_id",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "folders",
          },
          {
            name: "body",
            column_type: { type: "Text" },
            nullable: false,
          },
          {
            name: "revision",
            column_type: { type: "Integer" },
            nullable: false,
          },
          {
            name: "updated_at",
            column_type: { type: "Timestamp" },
            nullable: false,
          },
        ],
        policies: {
          select: documentSelectPolicy,
          update: documentUpdatePolicy,
        },
      },
    },
  };
}

interface PermissionSeedState {
  allowedDocumentIds: string[];
  deniedDocumentIds: string[];
}

async function seedPermissionDataset(
  db: Db,
  scenario: B5Scenario,
  permissionSchema: WasmSchema,
  owners: {
    allowedOwnerId: string;
    deniedOwnerId: string;
    intermediateOwnerId: string;
  },
): Promise<PermissionSeedState> {
  const folderTable = tableProxy<PermissionFolderRow, Omit<PermissionFolderRow, "id">>(
    "folders",
    permissionSchema,
  );
  const documentTable = tableProxy<PermissionDocumentRow, Omit<PermissionDocumentRow, "id">>(
    "documents",
    permissionSchema,
  );
  const rng = new Lcg(scenario.seed);
  const totalFolders = Math.max(4, scenario.folders);
  const totalDocuments = Math.max(20, scenario.documents);

  const allowedFolders: string[] = [];
  const deniedFolders: string[] = [];
  const ts = nowMicros();
  const allowedRootId = await db.insertWithAck(
    folderTable,
    {
      parent_id: null,
      owner_id: owners.allowedOwnerId,
      title: "allowed-root",
      updated_at: ts,
    },
    "worker",
  );
  const deniedRootId = await db.insertWithAck(
    folderTable,
    {
      parent_id: null,
      owner_id: owners.deniedOwnerId,
      title: "denied-root",
      updated_at: ts + 1,
    },
    "worker",
  );
  allowedFolders.push(allowedRootId);
  deniedFolders.push(deniedRootId);

  for (let i = 2; i < totalFolders; i += 1) {
    const allowedChain = i % 2 === 0;
    const parent = allowedChain
      ? allowedFolders[allowedFolders.length - 1]
      : deniedFolders[deniedFolders.length - 1];
    const id = await db.insertWithAck(
      folderTable,
      {
        parent_id: parent,
        // Allowed chain descendants intentionally use a non-session owner so
        // access relies on recursive INHERITS from the allowed root.
        owner_id: allowedChain ? owners.intermediateOwnerId : owners.deniedOwnerId,
        title: `folder-${i}`,
        updated_at: ts + i,
      },
      "worker",
    );
    if (allowedChain) {
      allowedFolders.push(id);
    } else {
      deniedFolders.push(id);
    }
  }

  const allowedDocumentIds: string[] = [];
  const deniedDocumentIds: string[] = [];
  const allowThreshold = Math.max(1, Math.min(99, Math.round(scenario.allow_fraction * 100)));
  for (let i = 0; i < totalDocuments; i += 1) {
    const useAllowed = rng.nextInt(100) < allowThreshold;
    const folderList = useAllowed ? allowedFolders : deniedFolders;
    const folderId = folderList[rng.nextInt(folderList.length)];
    const id = await db.insertWithAck(
      documentTable,
      {
        folder_id: folderId,
        body: `doc-${i}`,
        revision: 0,
        updated_at: ts + 10_000 + i,
      },
      "worker",
    );
    if (useAllowed) {
      allowedDocumentIds.push(id);
    } else {
      deniedDocumentIds.push(id);
    }
  }

  return {
    allowedDocumentIds,
    deniedDocumentIds,
  };
}

async function runB5(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B5 start");
  const dbName = uniqueDbName("b5");
  const rng = new Lcg(b5.seed ^ config.seed);
  const permissionSchema = permissionRecursiveSchema(Math.max(1, b5.recursive_depth));
  const reads = Math.min(b5.read_request_count, 160);
  const updates = Math.min(b5.update_attempt_count, 120);
  const latencies: Record<string, number[]> = {};
  let db: Db | null = null;

  try {
    const localAuthMode: "anonymous" = "anonymous";
    const allowedLocalToken = `${dbName}-b5-allowed-token`;
    const deniedLocalToken = `${dbName}-b5-denied-token`;
    const intermediateLocalToken = `${dbName}-b5-intermediate-token`;
    const allowedPrincipalId = await deriveLocalPrincipalId(
      APP_ID,
      localAuthMode,
      allowedLocalToken,
    );
    const deniedPrincipalId = await deriveLocalPrincipalId(APP_ID, localAuthMode, deniedLocalToken);
    const intermediatePrincipalId = await deriveLocalPrincipalId(
      APP_ID,
      localAuthMode,
      intermediateLocalToken,
    );

    // Keep a single DB instance so seeded data is present in the same runtime.
    // Admin secret in config is only used for catalogue payload sync; data ops
    // still use local-auth headers and are evaluated as non-admin principals.
    db = await createServerDb(
      dbName,
      "realistic-b5-allowed",
      {},
      {
        includeJwt: false,
        localAuthMode,
        localAuthToken: allowedLocalToken,
      },
    );
    const seeded = await seedPermissionDataset(db, b5, permissionSchema, {
      allowedOwnerId: allowedPrincipalId,
      deniedOwnerId: deniedPrincipalId,
      intermediateOwnerId: intermediatePrincipalId,
    });
    const deniedCount = seeded.deniedDocumentIds.length;
    const allowedCount = seeded.allowedDocumentIds.length;
    if (deniedCount === 0 || allowedCount === 0) {
      throw new Error("B5 requires both allowed and denied document populations");
    }

    const querySession: RuntimeSession = {
      user_id: allowedPrincipalId,
      claims: {
        auth_mode: "local",
        local_mode: localAuthMode,
      },
    };
    const querySessionJson = JSON.stringify(querySession);
    const client = policyClient(db, permissionSchema);
    const runtime = client.getRuntime();
    if (typeof runtime.updateWithSession !== "function") {
      throw new Error("B5 requires WasmRuntime.updateWithSession for policy-aware updates");
    }

    const documentsQueryJson = translateQuery(
      query<PermissionDocumentRow>(
        "documents",
        [],
        [["updated_at", "desc"]],
        200,
        permissionSchema,
      )._build(),
      permissionSchema,
    );
    const foldersQueryJson = translateQuery(
      query<PermissionFolderRow>(
        "folders",
        [],
        [["updated_at", "desc"]],
        200,
        permissionSchema,
      )._build(),
      permissionSchema,
    );
    const visibleDocumentsQueryJson = translateQuery(
      query<PermissionDocumentRow>(
        "documents",
        [],
        [["updated_at", "desc"]],
        1000,
        permissionSchema,
      )._build(),
      permissionSchema,
    );

    let warmedVisible = 0;
    for (let attempt = 0; attempt < 80; attempt += 1) {
      const rows = await client.queryInternal(documentsQueryJson, querySession);
      warmedVisible = rows.length;
      if (warmedVisible > 0) break;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    const initialVisibleDocuments = await client.queryInternal(
      visibleDocumentsQueryJson,
      querySession,
    );
    const initialVisibleIds = new Set(initialVisibleDocuments.map((row) => row.id));
    const allowedUpdateIds = seeded.allowedDocumentIds.filter((id) => initialVisibleIds.has(id));
    const deniedUpdateIds = seeded.deniedDocumentIds.filter((id) => !initialVisibleIds.has(id));
    if (allowedUpdateIds.length === 0) {
      throw new Error("B5 requires at least one visible allowed document for update attempts");
    }
    if (deniedUpdateIds.length === 0) {
      throw new Error("B5 requires at least one denied document that remains non-visible");
    }

    let allowedUpdateSuccess = 0;
    let deniedUpdateRejected = 0;
    let unexpectedDeniedForAllowed = 0;
    let unexpectedAllowedForDenied = 0;
    let firstAllowedUpdateError: string | null = null;
    let firstDeniedUpdateError: string | null = null;

    const wallStart = performance.now();
    for (let i = 0; i < reads; i += 1) {
      reportLoopProgress("B5 reads", i, reads);
      const t0 = performance.now();
      if (i % 2 === 0) {
        await client.queryInternal(documentsQueryJson, querySession);
      } else {
        await client.queryInternal(foldersQueryJson, querySession);
      }
      (latencies.permission_reads ||= []).push(performance.now() - t0);
    }

    for (let i = 0; i < updates; i += 1) {
      reportLoopProgress("B5 updates", i, updates);
      const shouldAllow =
        deniedUpdateIds.length === 0 || rng.nextInt(100) < Math.round(b5.allow_fraction * 100);
      const targetId = shouldAllow
        ? allowedUpdateIds[rng.nextInt(allowedUpdateIds.length)]
        : deniedUpdateIds[rng.nextInt(deniedUpdateIds.length)];
      const updatePayload = {
        body: { type: "Text", value: `b5-update-${i}` },
        revision: { type: "Integer", value: i + 1 },
        updated_at: { type: "Timestamp", value: nowMicros() },
      };
      const t0 = performance.now();
      try {
        runtime.updateWithSession(targetId, updatePayload, querySessionJson);
        if (shouldAllow) {
          allowedUpdateSuccess += 1;
          (latencies.permission_updates_allowed ||= []).push(performance.now() - t0);
        } else {
          unexpectedAllowedForDenied += 1;
          (latencies.permission_updates_denied ||= []).push(performance.now() - t0);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (shouldAllow) {
          unexpectedDeniedForAllowed += 1;
          if (firstAllowedUpdateError == null) {
            firstAllowedUpdateError = message;
          }
          (latencies.permission_updates_allowed ||= []).push(performance.now() - t0);
        } else {
          deniedUpdateRejected += 1;
          if (firstDeniedUpdateError == null) {
            firstDeniedUpdateError = message;
          }
          (latencies.permission_updates_denied ||= []).push(performance.now() - t0);
        }
      }
    }
    const wallMs = performance.now() - wallStart;

    const visibleDocuments = await client.queryInternal(visibleDocumentsQueryJson, querySession);
    const visibleIds = new Set(visibleDocuments.map((row) => row.id));
    const leakedDeniedReads = seeded.deniedDocumentIds.filter((id) => visibleIds.has(id)).length;
    const visibleAllowedReads = seeded.allowedDocumentIds.filter((id) => visibleIds.has(id)).length;

    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples);
    }

    return {
      scenario_id: b5.id,
      scenario_name: b5.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: reads + updates,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: (reads + updates) / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        recursive_depth: b5.recursive_depth,
        allowed_documents_seeded: allowedCount,
        denied_documents_seeded: deniedCount,
        allowed_updates_succeeded: allowedUpdateSuccess,
        denied_updates_rejected: deniedUpdateRejected,
        unexpected_denied_for_allowed: unexpectedDeniedForAllowed,
        unexpected_allowed_for_denied: unexpectedAllowedForDenied,
        first_allowed_update_error: firstAllowedUpdateError,
        first_denied_update_error: firstDeniedUpdateError,
        warm_visible_documents: warmedVisible,
        allowed_update_candidates: allowedUpdateIds.length,
        denied_update_candidates: deniedUpdateIds.length,
        visible_documents_total: visibleDocuments.length,
        allowed_documents_visible: visibleAllowedReads,
        denied_documents_visible: leakedDeniedReads,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB6(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B6 start");
  const dbName = uniqueDbName("b6");
  const rng = new Lcg(b6.seed ^ config.seed);
  const updateCount = Math.min(b6.update_count, 300);
  const latencies: number[] = [];
  let db: Db | null = null;

  try {
    db = await createServerDb(dbName, "realistic-b6");
    const state = await seedDataset(db, config);
    const hotTaskCount = Math.max(1, Math.min(state.taskIds.length, b6.hot_task_count));
    const hotTasks = state.taskIds.slice(0, hotTaskCount);
    const beforeBytes = await storageUsageBytes();

    const wallStart = performance.now();
    for (let i = 0; i < updateCount; i += 1) {
      reportLoopProgress("B6 hotspot updates", i, updateCount);
      const taskId = hotTasks[rng.nextInt(hotTasks.length)];
      const t0 = performance.now();
      db.update(tasksTable, taskId, {
        title: `Hotspot ${taskId.slice(0, 8)} rev ${i}`,
        priority: 1 + (i % 4),
        status: ["todo", "in_progress", "review", "done"][i % 4],
        updated_at: nowMicros(),
      });
      latencies.push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;
    const afterBytes = await storageUsageBytes();
    const storageDelta =
      beforeBytes != null && afterBytes != null ? Math.max(0, afterBytes - beforeBytes) : null;

    await db.all(query<TaskRow>("tasks", [{ column: "id", op: "eq", value: hotTasks[0] }], [], 1));

    return {
      scenario_id: b6.id,
      scenario_name: b6.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: updateCount,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: updateCount / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        hotspot_update_sync: summarizeLatencies(latencies),
      },
      extra: {
        hot_task_count: hotTaskCount,
        storage_usage_before_bytes: beforeBytes,
        storage_usage_after_bytes: afterBytes,
        storage_usage_delta_bytes: storageDelta,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

describe("realistic browser benchmark harness", () => {
  it("runs local and server-backed realistic scenarios against worker OPFS runtime", async () => {
    const restoreLogs = elevateBenchLogLevel();
    const cfg = scaledProfile(profile);
    const dbName = uniqueDbName("w1");
    progressLog(`bench start profile=${cfg.id}`);

    let db: Db | null = null;
    let w1Result: ScenarioResult;
    try {
      try {
        db = await createDb({ appId: APP_ID, dbName, logLevel: "warn" });
        const state = await seedDataset(db, cfg);
        w1Result = await runW1(db, cfg, state);
      } finally {
        if (db) await db.shutdown();
      }

      const w4Result = await runW4(cfg);
      const b1Result = await runB1(cfg);
      const b2Result = await runB2(cfg);
      const b3Result = await runB3(cfg);
      const b4Result = await runB4(cfg);
      const b5Result = await runB5(cfg);
      const b6Result = await runB6(cfg);

      const report = {
        runner: "jazz-ts-browser-opfs",
        generated_at: new Date().toISOString(),
        profile: cfg.id,
        scenarios: [w1Result, w4Result, b1Result, b2Result, b3Result, b4Result, b5Result, b6Result],
      };

      // Keeping output machine-readable makes it easy to pipe into trend tooling.
      // eslint-disable-next-line no-console
      console.log("[realistic-bench]", JSON.stringify(report));

      expect(w1Result.total_operations).toBeGreaterThan(0);
      expect(w1Result.throughput_ops_per_sec).toBeGreaterThan(0);
      expect(w4Result.operation_summaries.cold_reopen.count).toBeGreaterThan(0);
      expect(b1Result.operation_summaries.insert_sync.count).toBeGreaterThan(0);
      expect(b2Result.total_operations).toBeGreaterThan(0);
      expect(b3Result.operation_summaries.cold_reopen_query.count).toBeGreaterThan(0);
      expect(b4Result.operation_summaries.fanout_delivery.count).toBeGreaterThan(0);
      expect(b5Result.operation_summaries.permission_reads.count).toBeGreaterThan(0);
      expect(Number(b5Result.extra.allowed_documents_visible)).toBeGreaterThan(0);
      expect(Number(b5Result.extra.denied_documents_seeded)).toBeGreaterThan(0);
      expect(Number(b5Result.extra.denied_documents_visible)).toBe(0);
      expect(Number(b5Result.extra.allowed_updates_succeeded)).toBeGreaterThan(0);
      expect(Number(b5Result.extra.denied_updates_rejected)).toBeGreaterThan(0);
      expect(b6Result.operation_summaries.hotspot_update_sync.count).toBeGreaterThan(0);
    } finally {
      restoreLogs();
    }
  }, 420_000);
});

function elevateBenchLogLevel(): () => void {
  const original = {
    log: console.log,
    info: console.info,
    debug: console.debug,
    trace: console.trace,
  };

  const allow = (args: unknown[]) =>
    typeof args[0] === "string" &&
    (args[0].startsWith("[realistic-bench]") ||
      args[0].startsWith("[realistic-progress]") ||
      args[0].startsWith("[jazz-server]"));

  console.log = (...args: unknown[]) => {
    if (allow(args)) original.log(...args);
  };
  console.info = (...args: unknown[]) => {
    if (allow(args)) original.info(...args);
  };
  console.debug = () => {};
  console.trace = () => {};

  return () => {
    console.log = original.log;
    console.info = original.info;
    console.debug = original.debug;
    console.trace = original.trace;
  };
}

function base64url(input: string | Uint8Array): string {
  const str = typeof input === "string" ? btoa(input) : btoa(String.fromCharCode(...input));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function signJwt(
  sub: string,
  secret: string,
  claims: Record<string, unknown> = {},
): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    sub,
    claims,
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const enc = new TextEncoder();
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const data = enc.encode(`${headerB64}.${payloadB64}`);
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, data);
  return `${headerB64}.${payloadB64}.${base64url(new Uint8Array(sig))}`;
}
