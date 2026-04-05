import { readFileSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { expect, test, type Page, type TestInfo } from "@playwright/test";
import {
  ADMIN_SECRET,
  APP_ID,
  SEEDED_TODO_COUNT,
  TEST_BRANCH,
  TEST_ENV,
  TEST_PORT,
} from "./test-constants.js";

const SERVER_URL = `http://127.0.0.1:${TEST_PORT}`;
const STORAGE_KEY = "jazz-inspector-standalone-config";
const VISIBLE_ROW_COUNT = Math.min(SEEDED_TODO_COUNT, 10);
const RUNTIME_CONFIG_PATH = join(import.meta.dirname ?? __dirname, "runtime-config.json");
const INTERACTION_TIMEOUT_MS = 20_000;
const PAGE_READ_TIMEOUT_MS = 1_500;
const CLEANUP_TIMEOUT_MS = 5_000;
const GRID_LOG_PREFIX = "[inspector-grid]";
const SUBSCRIPTION_LOG_PREFIX = "[inspector-subscription]";

type ConsoleRecord = {
  type: string;
  text: string;
};

type ProfilerEntry = {
  id: string;
  phase: "mount" | "update" | "nested-update";
  actualDuration: number;
  baseDuration: number;
  startTime: number;
  commitTime: number;
  table: string;
  pageIndex: number;
  pageSize: number;
  sortColumn: string;
  sortDirection: "asc" | "desc";
  filterCount: number;
  rowCount: number;
  visibleRowCount: number;
  queryOffset: number;
  queryLimit: number;
  hasNextPage: boolean;
  query: string;
};

type GridEvent = {
  type: string;
  timestampMs: number;
  table: string;
  pageIndex: number;
  pageSize: number;
  sortColumn: string;
  sortDirection: "asc" | "desc";
  filterCount: number;
  rowCount: number;
  visibleRowCount: number;
  queryOffset: number;
  queryLimit: number;
  hasNextPage: boolean;
  query: string;
  details?: Record<string, unknown>;
};

type SubscriptionEvent = {
  type: string;
  timestampMs: number;
  key: string;
  table: string;
  query: string;
  options: string;
  listenerCount: number;
  status: "pending" | "fulfilled" | "rejected";
  dataLength: number;
  details?: Record<string, unknown>;
};

type HeartbeatStats = {
  beatCount: number;
  maxGapMs: number;
  avgGapMs: number;
};

type FreezeScenario = {
  slug: string;
  title: string;
  perform: (page: Page) => Promise<void>;
};

type InspectorClientArtifacts = {
  heartbeatValues: number[];
  profilerEntries: ProfilerEntry[];
  gridEvents: GridEvent[];
  subscriptionEvents: SubscriptionEvent[];
};

type SettledWithTimeout<T> =
  | { status: "fulfilled"; value: T }
  | { status: "rejected"; error: string }
  | { status: "timed_out" };

const FREEZE_SCENARIOS: readonly FreezeScenario[] = [
  {
    slug: "next-page",
    title: "Next page interaction",
    perform: async (page) => {
      await page.getByRole("button", { name: "Next" }).click({ timeout: 5_000 });
      await expect(page.getByText("Page 2")).toBeVisible({ timeout: 10_000 });
      await expect(page.getByText("Showing 11-20")).toBeVisible({ timeout: 10_000 });
    },
  },
  {
    slug: "page-size-change",
    title: "Rows per page interaction",
    perform: async (page) => {
      await page.getByLabel("Rows per page").selectOption("50", { timeout: 5_000 });
      await expect(page.getByText("Page 1")).toBeVisible({ timeout: 10_000 });
      await expect(page.getByText(/50 rows on page/)).toBeVisible({ timeout: 10_000 });
      await expect(page.getByText("Showing 1-50")).toBeVisible({ timeout: 10_000 });
    },
  },
  {
    slug: "sort-title",
    title: "Sort by title interaction",
    perform: async (page) => {
      await page.getByRole("columnheader", { name: "title" }).click({ timeout: 5_000 });
      await expect(page.locator("th").filter({ hasText: "title ↑" }).first()).toBeVisible({
        timeout: 10_000,
      });
    },
  },
];

test.describe("inspector pagination freeze repro harness", () => {
  for (const scenario of FREEZE_SCENARIOS) {
    test(`captures artifacts for ${scenario.title}`, async ({ page }, testInfo) => {
      test.setTimeout(60_000);
      await runFreezeScenario(page, testInfo, scenario);
    });
  }
});

async function runFreezeScenario(
  page: Page,
  testInfo: TestInfo,
  scenario: FreezeScenario,
): Promise<void> {
  const consoleRecords: ConsoleRecord[] = [];
  page.on("console", (message) => {
    consoleRecords.push({ type: message.type(), text: message.text() });
  });
  page.on("pageerror", (error) => {
    consoleRecords.push({ type: "pageerror", text: error.message });
  });

  await installDebugCapture(page);
  await openTodosTable(page);
  await clearDebugCapture(page);

  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Profiler.enable");
  await cdp.send("Profiler.start");
  await page.context().tracing.start({ screenshots: true, snapshots: true, sources: true });

  const tracePath = testInfo.outputPath(`${scenario.slug}.trace.zip`);
  const profilerPath = testInfo.outputPath(`${scenario.slug}.cpu-profile.json`);
  const heartbeatPath = testInfo.outputPath(`${scenario.slug}.heartbeat.json`);
  const gridEventsPath = testInfo.outputPath(`${scenario.slug}.grid-events.json`);
  const subscriptionEventsPath = testInfo.outputPath(`${scenario.slug}.subscription-events.json`);
  const summaryPath = testInfo.outputPath(`${scenario.slug}.summary.json`);

  const interactionResult = await settleWithTimeout(scenario.perform(page), INTERACTION_TIMEOUT_MS);
  const interactionError =
    interactionResult.status === "fulfilled"
      ? null
      : interactionResult.status === "timed_out"
        ? `interaction timed out after ${INTERACTION_TIMEOUT_MS}ms`
        : interactionResult.error;

  const clientArtifacts = hydrateArtifactsFromConsole(
    await collectClientArtifacts(page),
    consoleRecords,
  );

  const profilerStopResult = await settleWithTimeout(
    cdp.send("Profiler.stop") as Promise<{ profile: unknown }>,
    CLEANUP_TIMEOUT_MS,
  );
  const profilerStopError =
    profilerStopResult.status === "fulfilled"
      ? null
      : profilerStopResult.status === "timed_out"
        ? `Profiler.stop timed out after ${CLEANUP_TIMEOUT_MS}ms`
        : profilerStopResult.error;

  const traceStopResult = await settleWithTimeout(
    page.context().tracing.stop({ path: tracePath }),
    CLEANUP_TIMEOUT_MS,
  );
  const traceStopError =
    traceStopResult.status === "fulfilled"
      ? null
      : traceStopResult.status === "timed_out"
        ? `Tracing.stop timed out after ${CLEANUP_TIMEOUT_MS}ms`
        : traceStopResult.error;

  if (!page.isClosed() && interactionError !== null) {
    const closeResult = await settleWithTimeout(
      page.close({ runBeforeUnload: false }),
      CLEANUP_TIMEOUT_MS,
    );
    if (closeResult.status === "rejected") {
      consoleRecords.push({ type: "close-error", text: closeResult.error });
    }
    if (closeResult.status === "timed_out") {
      consoleRecords.push({
        type: "close-error",
        text: `page.close timed out after ${CLEANUP_TIMEOUT_MS}ms`,
      });
    }
  }

  const heartbeatStats = summarizeHeartbeats(clientArtifacts.heartbeatValues);
  const repeatedProfilerSnapshotCount = countRepeatedProfilerSnapshots(
    clientArtifacts.profilerEntries,
  );
  const repeatedGridStateCommitCount = countRepeatedGridStateSnapshots(clientArtifacts.gridEvents);
  const subscriptionSummary = summarizeSubscriptionEvents(clientArtifacts.subscriptionEvents);
  const profilerSummary = {
    commitCount: clientArtifacts.profilerEntries.length,
    repeatedSnapshotCount: repeatedProfilerSnapshotCount,
    updateCount: clientArtifacts.profilerEntries.filter((entry) => entry.phase !== "mount").length,
    maxActualDurationMs: Math.max(
      0,
      ...clientArtifacts.profilerEntries.map((entry) => entry.actualDuration),
    ),
  };
  const gridSummary = {
    eventCount: clientArtifacts.gridEvents.length,
    repeatedStateCommitCount: repeatedGridStateCommitCount,
    actionTypes: Array.from(
      new Set(
        clientArtifacts.gridEvents
          .filter((event) => event.type !== "state-commit")
          .map((event) => event.type),
      ),
    ),
  };

  await writeFile(heartbeatPath, JSON.stringify(clientArtifacts.heartbeatValues, null, 2));
  await writeFile(gridEventsPath, JSON.stringify(clientArtifacts.gridEvents, null, 2));
  await writeFile(
    subscriptionEventsPath,
    JSON.stringify(clientArtifacts.subscriptionEvents, null, 2),
  );
  await writeFile(
    profilerPath,
    JSON.stringify(
      profilerStopResult.status === "fulfilled" ? profilerStopResult.value.profile : null,
      null,
      2,
    ),
  );
  await writeFile(
    summaryPath,
    JSON.stringify(
      {
        scenario: scenario.slug,
        seededTodoCount: SEEDED_TODO_COUNT,
        interactionError,
        profilerStopError,
        traceStopError,
        artifactSources: {
          grid:
            clientArtifacts.gridEvents.length > 0 &&
            !consoleRecords.some((record) => record.text.startsWith(GRID_LOG_PREFIX))
              ? "window"
              : clientArtifacts.gridEvents.length > 0
                ? "window+console"
                : "none",
          subscriptions:
            clientArtifacts.subscriptionEvents.length > 0 &&
            !consoleRecords.some((record) => record.text.startsWith(SUBSCRIPTION_LOG_PREFIX))
              ? "window"
              : clientArtifacts.subscriptionEvents.length > 0
                ? "window+console"
                : "none",
        },
        heartbeatStats,
        profilerSummary,
        gridSummary,
        subscriptionSummary,
        consoleRecords,
      },
      null,
      2,
    ),
  );

  await testInfo.attach(`${scenario.slug}-summary`, {
    path: summaryPath,
    contentType: "application/json",
  });
  await testInfo.attach(`${scenario.slug}-heartbeat`, {
    path: heartbeatPath,
    contentType: "application/json",
  });
  await testInfo.attach(`${scenario.slug}-grid-events`, {
    path: gridEventsPath,
    contentType: "application/json",
  });
  await testInfo.attach(`${scenario.slug}-subscription-events`, {
    path: subscriptionEventsPath,
    contentType: "application/json",
  });
  await testInfo.attach(`${scenario.slug}-cpu-profile`, {
    path: profilerPath,
    contentType: "application/json",
  });
  if (traceStopError === null) {
    await testInfo.attach(`${scenario.slug}-trace`, {
      path: tracePath,
      contentType: "application/zip",
    });
  }
  expect(interactionError).toBeNull();
}

async function installDebugCapture(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const inspectorWindow = window as typeof window & {
      __inspectorHeartbeat?: number[];
      __inspectorProfiler?: unknown[];
      __inspectorGridEvents?: unknown[];
      __inspectorSubscriptionEvents?: unknown[];
    };
    inspectorWindow.__inspectorHeartbeat = [];
    inspectorWindow.__inspectorProfiler = [];
    inspectorWindow.__inspectorGridEvents = [];
    inspectorWindow.__inspectorSubscriptionEvents = [];
    globalThis.setInterval(() => {
      inspectorWindow.__inspectorHeartbeat?.push(performance.now());
    }, 50);
  });
}

async function openTodosTable(page: Page): Promise<void> {
  await page.goto("/");
  await page.evaluate(
    ({ key, config }) => {
      localStorage.setItem(key, JSON.stringify(config));
    },
    {
      key: STORAGE_KEY,
      config: {
        serverUrl: SERVER_URL,
        appId: APP_ID,
        adminSecret: ADMIN_SECRET,
        env: TEST_ENV,
        branch: TEST_BRANCH,
        schemaHash: readRuntimeConfig().schemaHash,
      },
    },
  );
  await page.reload();

  await expect(page.getByRole("link", { name: "Data Explorer" })).toBeVisible({
    timeout: 15_000,
  });
  await page.getByRole("link", { name: "View todos data" }).click();
  await expect(page.getByText(new RegExp(`${VISIBLE_ROW_COUNT} rows on page`))).toBeVisible({
    timeout: 15_000,
  });
}

async function clearDebugCapture(page: Page): Promise<void> {
  await page.evaluate(() => {
    const inspectorWindow = window as typeof window & {
      __inspectorHeartbeat?: number[];
      __inspectorProfiler?: unknown[];
      __inspectorGridEvents?: unknown[];
      __inspectorSubscriptionEvents?: unknown[];
    };
    inspectorWindow.__inspectorHeartbeat = [];
    inspectorWindow.__inspectorProfiler = [];
    inspectorWindow.__inspectorGridEvents = [];
    inspectorWindow.__inspectorSubscriptionEvents = [];
  });
}

async function collectClientArtifacts(page: Page): Promise<InspectorClientArtifacts> {
  const [heartbeatValues, profilerEntries, gridEvents, subscriptionEvents] = await Promise.all([
    readWindowArray<number>(page, "__inspectorHeartbeat"),
    readWindowArray<ProfilerEntry>(page, "__inspectorProfiler"),
    readWindowArray<GridEvent>(page, "__inspectorGridEvents"),
    readWindowArray<SubscriptionEvent>(page, "__inspectorSubscriptionEvents"),
  ]);

  return {
    heartbeatValues,
    profilerEntries,
    gridEvents,
    subscriptionEvents,
  };
}

async function readWindowArray<T>(page: Page, propertyName: string): Promise<T[]> {
  if (page.isClosed()) {
    return [];
  }

  const readResult = await settleWithTimeout(
    page.evaluate((windowPropertyName) => {
      const inspectorWindow = window as Record<string, unknown[] | undefined>;
      return [...(inspectorWindow[windowPropertyName] ?? [])];
    }, propertyName),
    PAGE_READ_TIMEOUT_MS,
  );

  return readResult.status === "fulfilled" ? (readResult.value as T[]) : [];
}

async function settleWithTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
): Promise<SettledWithTimeout<T>> {
  return await new Promise<SettledWithTimeout<T>>((resolve) => {
    const timeoutId = setTimeout(() => {
      resolve({ status: "timed_out" });
    }, timeoutMs);

    void promise.then(
      (value) => {
        clearTimeout(timeoutId);
        resolve({ status: "fulfilled", value });
      },
      (error) => {
        clearTimeout(timeoutId);
        resolve({ status: "rejected", error: toErrorMessage(error) });
      },
    );
  });
}

function summarizeHeartbeats(heartbeats: number[]): HeartbeatStats {
  if (heartbeats.length < 2) {
    return {
      beatCount: heartbeats.length,
      maxGapMs: 0,
      avgGapMs: 0,
    };
  }

  let maxGapMs = 0;
  let totalGapMs = 0;
  for (let index = 1; index < heartbeats.length; index += 1) {
    const gap = heartbeats[index]! - heartbeats[index - 1]!;
    maxGapMs = Math.max(maxGapMs, gap);
    totalGapMs += gap;
  }

  return {
    beatCount: heartbeats.length,
    maxGapMs,
    avgGapMs: totalGapMs / (heartbeats.length - 1),
  };
}

function countRepeatedProfilerSnapshots(entries: ProfilerEntry[]): number {
  let repeatedSnapshots = 0;
  for (let index = 1; index < entries.length; index += 1) {
    const previous = entries[index - 1]!;
    const current = entries[index]!;
    if (
      previous.table === current.table &&
      previous.pageIndex === current.pageIndex &&
      previous.pageSize === current.pageSize &&
      previous.sortColumn === current.sortColumn &&
      previous.sortDirection === current.sortDirection &&
      previous.filterCount === current.filterCount &&
      previous.rowCount === current.rowCount &&
      previous.visibleRowCount === current.visibleRowCount &&
      previous.query === current.query
    ) {
      repeatedSnapshots += 1;
    }
  }
  return repeatedSnapshots;
}

function countRepeatedGridStateSnapshots(events: GridEvent[]): number {
  const stateCommitEvents = events.filter((event) => event.type === "state-commit");
  let repeatedSnapshots = 0;

  for (let index = 1; index < stateCommitEvents.length; index += 1) {
    const previous = stateCommitEvents[index - 1]!;
    const current = stateCommitEvents[index]!;
    if (
      previous.table === current.table &&
      previous.pageIndex === current.pageIndex &&
      previous.pageSize === current.pageSize &&
      previous.sortColumn === current.sortColumn &&
      previous.sortDirection === current.sortDirection &&
      previous.filterCount === current.filterCount &&
      previous.rowCount === current.rowCount &&
      previous.visibleRowCount === current.visibleRowCount &&
      previous.query === current.query
    ) {
      repeatedSnapshots += 1;
    }
  }

  return repeatedSnapshots;
}

function summarizeSubscriptionEvents(events: SubscriptionEvent[]) {
  const byType = events.reduce<Record<string, number>>((accumulator, event) => {
    accumulator[event.type] = (accumulator[event.type] ?? 0) + 1;
    return accumulator;
  }, {});

  let repeatedDeltaCount = 0;
  for (let index = 1; index < events.length; index += 1) {
    const previous = events[index - 1]!;
    const current = events[index]!;
    if (
      previous.type === "delta" &&
      current.type === "delta" &&
      previous.key === current.key &&
      previous.dataLength === current.dataLength &&
      previous.status === current.status
    ) {
      repeatedDeltaCount += 1;
    }
  }

  return {
    eventCount: events.length,
    uniqueKeyCount: new Set(events.map((event) => event.key)).size,
    makeQueryKeyCount: byType["make-query-key"] ?? 0,
    reuseEntryCount: byType["reuse-entry"] ?? 0,
    deltaCount: byType.delta ?? 0,
    repeatedDeltaCount,
    byType,
  };
}

function hydrateArtifactsFromConsole(
  artifacts: InspectorClientArtifacts,
  consoleRecords: readonly ConsoleRecord[],
): InspectorClientArtifacts {
  return {
    ...artifacts,
    gridEvents:
      artifacts.gridEvents.length > 0
        ? artifacts.gridEvents
        : parseConsoleEvents<GridEvent>(consoleRecords, GRID_LOG_PREFIX),
    subscriptionEvents:
      artifacts.subscriptionEvents.length > 0
        ? artifacts.subscriptionEvents
        : parseConsoleEvents<SubscriptionEvent>(consoleRecords, SUBSCRIPTION_LOG_PREFIX),
  };
}

function parseConsoleEvents<T>(consoleRecords: readonly ConsoleRecord[], prefix: string): T[] {
  const events: T[] = [];

  for (const record of consoleRecords) {
    if (!record.text.startsWith(prefix)) {
      continue;
    }

    const payload = record.text.slice(prefix.length).trim();
    if (payload.length === 0) {
      continue;
    }

    try {
      events.push(JSON.parse(payload) as T);
    } catch {
      continue;
    }
  }

  return events;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function readRuntimeConfig(): { schemaHash: string } {
  return JSON.parse(readFileSync(RUNTIME_CONFIG_PATH, "utf8")) as { schemaHash: string };
}
