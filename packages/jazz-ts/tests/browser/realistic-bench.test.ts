import { describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { ADMIN_SECRET, APP_ID, JWT_SECRET, TEST_PORT } from "./test-constants.js";

import schemaJson from "../../../../benchmarks/realistic/schema/project_board.schema.json";
import profileJson from "../../../../benchmarks/realistic/profiles/s.json";
import w1Json from "../../../../benchmarks/realistic/scenarios/w1_interactive.json";
import w3Json from "../../../../benchmarks/realistic/scenarios/w3_offline_reconnect.json";
import w4Json from "../../../../benchmarks/realistic/scenarios/w4_cold_start.json";

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

function tableProxy<T, Init>(table: string): TableProxy<T, Init> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as T,
    _initType: {} as Init,
  };
}

function query<T>(
  table: string,
  conditions: Array<{ column: string; op: string; value: unknown }> = [],
  orderBy: Array<[string, "asc" | "desc"]> = [],
  limit?: number,
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: schema,
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

describe("realistic browser benchmark harness", () => {
  it("runs W1/W3/W4 scenarios against worker OPFS runtime", async () => {
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

      const w3Result = await runW3(cfg);
      const w4Result = await runW4(cfg);

      const report = {
        runner: "jazz-ts-browser-opfs",
        generated_at: new Date().toISOString(),
        profile: cfg.id,
        scenarios: [w1Result, w3Result, w4Result],
      };

      // Keeping output machine-readable makes it easy to pipe into trend tooling.
      // eslint-disable-next-line no-console
      console.log("[realistic-bench]", JSON.stringify(report));

      expect(w1Result.total_operations).toBeGreaterThan(0);
      expect(w1Result.throughput_ops_per_sec).toBeGreaterThan(0);
      expect(w3Result.extra.observed_comments_after_reconnect).toBeDefined();
      expect(w4Result.operation_summaries.cold_reopen.count).toBeGreaterThan(0);
    } finally {
      restoreLogs();
    }
  }, 120_000);
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

async function signJwt(sub: string, secret: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    sub,
    claims: {},
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
