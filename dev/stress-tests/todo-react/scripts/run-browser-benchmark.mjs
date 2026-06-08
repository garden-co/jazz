import { chromium } from "playwright";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DEFAULT_BASE_URL = "http://localhost:5477/";
const DEFAULT_SAMPLES = 5;
const DEFAULT_WORKER_INIT_TIMEOUT_MS = 180_000;
const DEFAULT_SYNC_SETTLEMENT_TIER = "edge";
const DEFAULT_RESULT_TIMEOUT_MS = 180_000;
const OPFS_COUNTER_KEYS = [
  "readCalls",
  "readBytes",
  "writeCalls",
  "writeBytes",
  "lenCalls",
  "truncateCalls",
  "flushCalls",
];

function parseArgs(argv) {
  const args = {
    baseUrl: DEFAULT_BASE_URL,
    samples: DEFAULT_SAMPLES,
    label: "current",
    sync: "off",
    syncSettlementTier: DEFAULT_SYNC_SETTLEMENT_TIER,
    workerInitTimeoutMs: DEFAULT_WORKER_INIT_TIMEOUT_MS,
    resultTimeoutMs: DEFAULT_RESULT_TIMEOUT_MS,
  };

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--") {
      continue;
    } else if (arg === "--base-url") {
      args.baseUrl = argv[++i];
    } else if (arg === "--samples") {
      args.samples = Number(argv[++i]);
    } else if (arg === "--label") {
      args.label = argv[++i];
    } else if (arg === "--sync") {
      args.sync = argv[++i];
    } else if (arg === "--sync-settlement-tier") {
      args.syncSettlementTier = argv[++i];
    } else if (arg === "--worker-init-timeout-ms") {
      args.workerInitTimeoutMs = Number(argv[++i]);
    } else if (arg === "--result-timeout-ms") {
      args.resultTimeoutMs = Number(argv[++i]);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (!Number.isInteger(args.samples) || args.samples < 1) {
    throw new Error("--samples must be a positive integer");
  }
  if (args.sync !== "off" && args.sync !== "on") {
    throw new Error("--sync must be either 'off' or 'on'");
  }
  if (args.syncSettlementTier !== "edge" && args.syncSettlementTier !== "global") {
    throw new Error("--sync-settlement-tier must be either 'edge' or 'global'");
  }
  if (!Number.isInteger(args.workerInitTimeoutMs) || args.workerInitTimeoutMs < 1) {
    throw new Error("--worker-init-timeout-ms must be a positive integer");
  }
  if (!Number.isInteger(args.resultTimeoutMs) || args.resultTimeoutMs < 1) {
    throw new Error("--result-timeout-ms must be a positive integer");
  }

  return args;
}

function percentile(values, p) {
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
}

function percentileSummary(values) {
  const numbers = values.filter((value) => typeof value === "number" && Number.isFinite(value));
  if (numbers.length === 0) return undefined;
  return {
    p50: percentile(numbers, 50),
    p95: percentile(numbers, 95),
  };
}

function counterSummary(samples, phase) {
  const counters = samples
    .map((sample) => sample[phase].opfsIoCounters)
    .filter((counter) => counter && typeof counter === "object");
  if (counters.length === 0) return undefined;

  return Object.fromEntries(
    OPFS_COUNTER_KEYS.map((key) => [
      key,
      percentileSummary(counters.map((counter) => counter[key])),
    ]),
  );
}

async function waitForBenchmarkResult(page, phase, resultTimeoutMs) {
  await page.waitForFunction(
    (expectedPhase) => {
      const result = window.__JAZZ_TODO_BENCHMARK__;
      return (
        result?.phase === expectedPhase && (result.status === "ok" || result.status === "error")
      );
    },
    phase,
    { timeout: resultTimeoutMs },
  );

  const result = await page.evaluate(() => window.__JAZZ_TODO_BENCHMARK__);
  if (!result || result.status !== "ok") {
    throw new Error(`Benchmark ${phase} failed: ${JSON.stringify(result)}`);
  }
  return result;
}

async function openBenchmarkPage(context, url, pageErrors, consoleErrors) {
  const page = await context.newPage();

  page.on("pageerror", (error) => {
    pageErrors.push(error.message);
  });
  page.on("console", (message) => {
    if (message.type() === "error") {
      consoleErrors.push(message.text());
    }
  });

  await page.goto(url.href);
  return page;
}

function configureBenchmarkUrl(url, phase, dbName, args) {
  url.searchParams.set("benchmark", phase);
  url.searchParams.set("dbName", dbName);
  url.searchParams.set("sync", args.sync);
  if (args.sync === "on") {
    url.searchParams.set("syncSettlementTier", args.syncSettlementTier);
  }
  url.searchParams.set("workerInitTimeoutMs", String(args.workerInitTimeoutMs));
}

async function runSample(args, sampleIndex) {
  const { baseUrl, label } = args;
  const dbName = `todo-react-bench-${label}-${Date.now()}-${sampleIndex}`;
  const userDataDir = await mkdtemp(join(tmpdir(), "jazz-todo-browser-bench-"));
  const pageErrors = [];
  const consoleErrors = [];

  try {
    const writeContext = await chromium.launchPersistentContext(userDataDir, { headless: true });
    const writeUrl = new URL(baseUrl);
    configureBenchmarkUrl(writeUrl, "write", dbName, args);
    const page = await openBenchmarkPage(writeContext, writeUrl, pageErrors, consoleErrors);
    const write = await waitForBenchmarkResult(page, "write", args.resultTimeoutMs);
    await writeContext.close();

    const reopenContext = await chromium.launchPersistentContext(userDataDir, { headless: true });
    const reopenUrl = new URL(baseUrl);
    configureBenchmarkUrl(reopenUrl, "reopen", dbName, args);
    const reopenPage = await openBenchmarkPage(reopenContext, reopenUrl, pageErrors, consoleErrors);
    const reopen = await waitForBenchmarkResult(reopenPage, "reopen", args.resultTimeoutMs);
    await reopenContext.close();

    return {
      label,
      sampleIndex,
      dbName,
      write,
      reopen,
      pageErrors,
      consoleErrors,
    };
  } finally {
    await rm(userDataDir, { recursive: true, force: true });
  }
}

const args = parseArgs(process.argv.slice(2));
const samples = [];

for (let i = 0; i < args.samples; i++) {
  const sample = await runSample(args, i + 1);
  samples.push(sample);
  console.log(JSON.stringify(sample));
}

const summary = {
  label: args.label,
  sync: args.sync,
  syncSettlementTier: args.sync === "on" ? args.syncSettlementTier : undefined,
  workerInitTimeoutMs: args.workerInitTimeoutMs,
  resultTimeoutMs: args.resultTimeoutMs,
  samples: samples.length,
  writeTotalMs: percentileSummary(samples.map((sample) => sample.write.totalMs)),
  enqueueMs: percentileSummary(samples.map((sample) => sample.write.enqueueMs)),
  localDurabilityMs: percentileSummary(samples.map((sample) => sample.write.localDurabilityMs)),
  syncSettlementMs: percentileSummary(samples.map((sample) => sample.write.syncSettlementMs)),
  reopenQueryMs: percentileSummary(samples.map((sample) => sample.reopen.reopenQueryMs)),
  writeOpfsIoCounters: counterSummary(samples, "write"),
  reopenOpfsIoCounters: counterSummary(samples, "reopen"),
  pageErrors: samples.flatMap((sample) => sample.pageErrors),
  consoleErrors: samples.flatMap((sample) => sample.consoleErrors),
};

console.log(JSON.stringify({ summary }, null, 2));
