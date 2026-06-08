import { useEffect, useRef, useState } from "react";
import type { Db } from "jazz-tools";
import { useDb, useSession } from "jazz-tools/react";
import { app } from "../schema";
import {
  type BenchmarkSyncSettlementTier,
  type BenchmarkWriteHandle,
  waitForBenchmarkWriteDurability,
} from "./benchmark-utils.js";

const TOTAL_PROJECTS = 10_000;
const TOTAL_TODOS = 50_000;
const BATCH_SIZE = 500;
const YIELD_EVERY_BATCHES = 10;

type BenchmarkPhase = "write" | "reopen";

type OpfsIoCounters = {
  readCalls: number;
  readBytes: number;
  writeCalls: number;
  writeBytes: number;
  lenCalls: number;
  truncateCalls: number;
  flushCalls: number;
};

type BenchmarkResult = {
  status: "ok";
  phase: BenchmarkPhase;
  dbName: string;
  projects: number;
  todos: number;
  batchSize: number;
  enqueueMs?: number;
  localDurabilityMs?: number;
  syncSettlementMs?: number;
  syncSettlementTier?: BenchmarkSyncSettlementTier;
  reopenQueryMs?: number;
  totalMs: number;
  queriedTodos?: number;
  opfsIoCounters: OpfsIoCounters | null;
};

type BenchmarkError = {
  status: "error";
  phase: BenchmarkPhase;
  dbName: string;
  message: string;
};

declare global {
  interface Window {
    __JAZZ_TODO_BENCHMARK__?: BenchmarkResult | BenchmarkError;
  }
}

function nowMs(): number {
  return performance.now();
}

function deterministicProjectName(index: number): string {
  return `bench-project-${index.toString().padStart(5, "0")}`;
}

function deterministicTodoTitle(index: number): string {
  return `bench-todo-${index.toString().padStart(5, "0")}`;
}

function yieldToBrowser(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

function requireBenchmarkParam(name: string): string {
  const value = new URLSearchParams(location.search).get(name);
  if (!value) {
    throw new Error(`Missing benchmark query param: ${name}`);
  }
  return value;
}

function benchmarkSyncSettlementTier(): BenchmarkSyncSettlementTier | undefined {
  const params = new URLSearchParams(location.search);
  if (params.get("sync") === "off") return undefined;

  const tier = params.get("syncSettlementTier") ?? "edge";
  if (tier !== "edge" && tier !== "global") {
    throw new Error("syncSettlementTier must be either 'edge' or 'global'");
  }
  return tier;
}

function publishResult(result: BenchmarkResult | BenchmarkError): void {
  window.__JAZZ_TODO_BENCHMARK__ = result;
}

async function resetOpfsIoCounters(db: Db): Promise<void> {
  await db.debugOpfsIoCountersReset();
}

async function snapshotOpfsIoCounters(db: Db): Promise<OpfsIoCounters | null> {
  return await db.debugOpfsIoCountersSnapshot();
}

export function BenchmarkRunner({ phase }: { phase: BenchmarkPhase }) {
  const db = useDb();
  const session = useSession();
  const startedRef = useRef(false);
  const [status, setStatus] = useState("waiting");

  useEffect(() => {
    if (startedRef.current) return;
    if (!session?.user_id) return;
    startedRef.current = true;

    const dbName = requireBenchmarkParam("dbName");
    const run = phase === "write" ? runWriteBenchmark : runReopenBenchmark;

    void run(db, session.user_id, dbName, setStatus)
      .then(publishResult)
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        publishResult({ status: "error", phase, dbName, message });
        setStatus(`error: ${message}`);
      });
  }, [db, phase, session?.user_id]);

  return (
    <>
      <h1>Todo Browser Benchmark</h1>
      <p id="benchmark-status">{status}</p>
    </>
  );
}

async function runWriteBenchmark(
  db: Db,
  sessionUserId: string,
  dbName: string,
  setStatus: (status: string) => void,
): Promise<BenchmarkResult> {
  const totalStart = nowMs();
  const handles: BenchmarkWriteHandle[] = [];
  const projectIds: string[] = [];
  let batchesSinceYield = 0;
  const syncSettlementTier = benchmarkSyncSettlementTier();

  await resetOpfsIoCounters(db);
  setStatus("enqueue-projects");
  const enqueueStart = nowMs();
  for (let i = 0; i < TOTAL_PROJECTS; i += BATCH_SIZE) {
    const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_PROJECTS);
    const handle = db.batch((batch) => {
      for (let j = i; j < batchEnd; j++) {
        const row = batch.insert(app.projects, { name: deterministicProjectName(j) });
        projectIds.push(row.id);
      }
    });
    handles.push(handle);

    batchesSinceYield++;
    if (batchesSinceYield >= YIELD_EVERY_BATCHES) {
      batchesSinceYield = 0;
      await yieldToBrowser();
    }
  }

  setStatus("enqueue-todos");
  batchesSinceYield = 0;
  for (let i = 0; i < TOTAL_TODOS; i += BATCH_SIZE) {
    const batchEnd = Math.min(i + BATCH_SIZE, TOTAL_TODOS);
    const handle = db.batch((batch) => {
      for (let j = i; j < batchEnd; j++) {
        batch.insert(app.todos, {
          title: deterministicTodoTitle(j),
          done: j % 5 === 0,
          owner_id: sessionUserId,
          projectId: projectIds[j % projectIds.length],
        });
      }
    });
    handles.push(handle);

    batchesSinceYield++;
    if (batchesSinceYield >= YIELD_EVERY_BATCHES) {
      batchesSinceYield = 0;
      await yieldToBrowser();
    }
  }
  const enqueueMs = nowMs() - enqueueStart;

  const durability = await waitForBenchmarkWriteDurability(handles, syncSettlementTier, setStatus);
  const opfsIoCounters = await snapshotOpfsIoCounters(db);

  setStatus("shutdown");
  await db.shutdown();

  const totalMs = nowMs() - totalStart;
  setStatus("done");

  return {
    status: "ok",
    phase: "write",
    dbName,
    projects: TOTAL_PROJECTS,
    todos: TOTAL_TODOS,
    batchSize: BATCH_SIZE,
    enqueueMs,
    ...durability,
    totalMs,
    opfsIoCounters,
  };
}

async function runReopenBenchmark(
  db: Db,
  _sessionUserId: string,
  dbName: string,
  setStatus: (status: string) => void,
): Promise<BenchmarkResult> {
  const totalStart = nowMs();
  await resetOpfsIoCounters(db);
  setStatus("query-local");
  const queryStart = nowMs();
  const rows = await db.all(app.todos.limit(100), { tier: "local" });
  const reopenQueryMs = nowMs() - queryStart;
  const opfsIoCounters = await snapshotOpfsIoCounters(db);
  const totalMs = nowMs() - totalStart;
  setStatus("done");

  return {
    status: "ok",
    phase: "reopen",
    dbName,
    projects: TOTAL_PROJECTS,
    todos: TOTAL_TODOS,
    batchSize: BATCH_SIZE,
    reopenQueryMs,
    totalMs,
    queriedTodos: rows.length,
    opfsIoCounters,
  };
}
