import { Worker } from "node:worker_threads";

import type { LocalNode } from "cojson";
import { cojsonInternals } from "cojson";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { startSyncServer } from "jazz-run/startSyncServer";

import type { ParsedArgs } from "../../utils/args.ts";
import {
  getFlagBoolean,
  getFlagNumber,
  getFlagString,
} from "../../utils/args.ts";
import { setupMetrics } from "../../metrics.ts";
import { calculateStats, minMedianMax } from "../../utils/stats.ts";
import { readConfigId, loadSeedConfig } from "../../utils/loadSeedConfig.ts";
import {
  assertNonEmptyString,
  getConfigFilePath,
} from "../../utils/seedHelpers.ts";
import {
  updateScenarioState,
  clearScenarioState,
} from "../../scenarioMetrics.ts";
import type { BatchSeedConfig } from "../../schema.ts";
import type { BatchWorkerResult, BatchWorkerData } from "./types.ts";

type ServerHandle = {
  localNode: LocalNode;
  close: () => void;
} | null;

/**
 * Clear all loaded coValues from the server's LocalNode cache.
 * This simulates a cold cache for each benchmark run.
 */
function clearCoValuesCache(server: { localNode: LocalNode }): number {
  const localNode = server.localNode;
  for (const value of localNode.allCoValues()) {
    value.dependant = new Set();
    value.unmount();
  }
  return 0;
}

type WorkerMessage =
  | { type: "hello"; workerId: number }
  | BatchWorkerResult
  | { type: "done"; workerId: number };

type RunResult = {
  runIndex: number;
  totalTimeMs: number;
  latencies: number[];
  mapsLoaded: number;
  errors: number;
};

/**
 * Run a single benchmark run with all workers.
 */
async function executeRun(
  runIndex: number,
  workerCount: number,
  mapIds: string[],
  peer: string,
  workerScript: URL,
): Promise<RunResult[]> {
  const results: RunResult[] = [];
  let doneCount = 0;

  // Divide maps among workers
  const mapsPerWorker = Math.ceil(mapIds.length / workerCount);

  const allDone = new Promise<void>((resolve, reject) => {
    for (let workerId = 0; workerId < workerCount; workerId++) {
      const startIdx = workerId * mapsPerWorker;
      const endIdx = Math.min(startIdx + mapsPerWorker, mapIds.length);
      const workerMaps = mapIds.slice(startIdx, endIdx);

      if (workerMaps.length === 0) {
        doneCount++;
        if (doneCount >= workerCount) resolve();
        continue;
      }

      const workerData: BatchWorkerData = {
        workerId,
        peer,
        mapIds: workerMaps,
        runIndex,
      };

      const w = new Worker(workerScript, {
        execArgv: ["--experimental-strip-types"],
        workerData,
      });

      w.on("message", (msg: WorkerMessage) => {
        if (msg.type === "result") {
          results.push({
            runIndex: msg.runIndex,
            totalTimeMs: msg.totalTimeMs,
            latencies: msg.latencies,
            mapsLoaded: msg.mapsLoaded,
            errors: msg.errors,
          });
        } else if (msg.type === "done") {
          w.terminate();
          doneCount++;
          if (doneCount >= workerCount) resolve();
        }
      });

      w.on("error", (e) => {
        reject(e);
      });
    }
  });

  await allDone;
  return results;
}

/**
 * Batch Benchmark Scenario
 *
 * Load a set of X maps with sizes between minSize and maxSize on N workers concurrently.
 * Run N runs and calculate median and percentiles.
 * Loads the SeedConfig CoValue first to get the IDs to use.
 *
 * Can run against:
 * - Local server (default): starts a local sync server using --db
 * - Remote server: connects to --peer URL (requires --config-id)
 */
export async function run(args: ParsedArgs): Promise<void> {
  // Local mode: start a local sync server
  const dbPath = assertNonEmptyString(
    getFlagString(args, "db") ?? "./batch.db",
    "--db",
  );
  const remotePeer = getFlagString(args, "peer");
  const isRemote = !!remotePeer;

  const workerCount = getFlagNumber(args, "workers") ?? 3;
  const runs = getFlagNumber(args, "runs") ?? 50;
  const mapsLimit = getFlagNumber(args, "maps");

  let peer: string;
  let metrics: string | undefined;
  let server: ServerHandle = null;
  let configId: string;

  const host = getFlagString(args, "host") ?? "127.0.0.1";
  const randomPort = getFlagBoolean(args, "random-port");
  const port = getFlagString(args, "port") ?? (randomPort ? "0" : "4000");

  // Read the seed config ID from the config file
  const localConfigId = readConfigId(dbPath);
  if (!localConfigId) {
    throw new Error(
      `No seed config found. Expected config file at: ${getConfigFilePath(dbPath)}. Run seed first.`,
    );
  }
  configId = localConfigId;

  // Start the sync server
  const localServer = await startSyncServer({
    host,
    port,
    inMemory: false,
    db: dbPath,
    crypto: await NapiCrypto.create(),
    middleware: setupMetrics().middleware,
  });

  const addr = localServer.address();
  if (!addr || typeof addr === "string") {
    throw new Error("Unexpected server address()");
  }
  peer = `ws://${addr.address}:${addr.port}`;
  metrics = `http://${addr.address}:${addr.port}/metrics`;
  server = localServer;

  if (isRemote) {
    // Remote mode: connect to existing sync server
    peer = remotePeer;
    console.log(`Using remote sync server: ${peer}`);
  }

  // Load the seed config from the server to get IDs
  console.log("Loading seed configuration...");
  const seedConfig = await loadSeedConfig(peer, configId as any);

  if (seedConfig.scenario !== "batch") {
    server?.close();
    throw new Error(
      `Database was seeded for "${seedConfig.scenario}" scenario, but "batch" was requested`,
    );
  }

  const config = seedConfig as BatchSeedConfig;
  let mapIds = config.mapIds as string[];

  if (mapIds.length === 0) {
    server?.close();
    throw new Error("No CoMap targets found in seed config");
  }

  // Limit maps if requested
  if (mapsLimit !== undefined && mapsLimit < mapIds.length) {
    mapIds = mapIds.slice(0, mapsLimit);
  }

  console.log(
    JSON.stringify(
      {
        scenario: "batch",
        mode: isRemote ? "remote" : "local",
        peer,
        metrics,
        workers: workerCount,
        runs,
        warmupRun: isRemote,
        mapsToLoad: mapIds.length,
        totalMapsAvailable: config.mapIds.length,
        minSize: config.minSize,
        maxSize: config.maxSize,
        configId,
      },
      null,
      2,
    ),
  );

  const workerScript = new URL("./worker.ts", import.meta.url);

  const runTimes: number[] = [];
  const throughputs: number[] = [];
  const allLatencies: number[] = [];

  // Track last run results
  let lastMapsLoaded = 0;
  let lastErrors = 0;

  // Initialize batch metrics
  updateScenarioState({
    scenario: "batch",
    workers: workerCount,
    runCurrent: 0,
    runsTotal: runs,
    mapsLoaded: 0,
    mapsTotal: mapIds.length,
    timeMsMin: 0,
    timeMsMedian: 0,
    timeMsMax: 0,
    throughputMin: 0,
    throughputMedian: 0,
    throughputMax: 0,
    errors: 0,
  });

  try {
    // For remote mode, run a warmup run first (results discarded)
    if (isRemote) {
      console.log("\n=== WARMUP RUN (results discarded) ===");
      const warmupStartTime = performance.now();
      const warmupResults = await executeRun(
        -1, // warmup run index
        1,
        mapIds,
        peer,
        workerScript,
      );
      const warmupTime = performance.now() - warmupStartTime;
      const warmupMaps = warmupResults.reduce((s, r) => s + r.mapsLoaded, 0);
      console.log(
        JSON.stringify({
          run: "warmup",
          totalTimeMs: warmupTime,
          mapsLoaded: warmupMaps,
          note: "Results discarded - cache warming",
        }),
      );
    }

    for (let runIdx = 0; runIdx < runs; runIdx++) {
      // Clear coValues cache before each run to simulate cold cache (only for local server)
      if (!isRemote) {
        clearCoValuesCache(server);
      }
      console.log(`\nStarting run ${runIdx + 1}/${runs}...`);

      // Calculate current stats (from previous runs)
      const timeStats = minMedianMax(runTimes);
      const throughputStats = minMedianMax(throughputs);

      // Update metrics for run start - show stats from completed runs
      updateScenarioState({
        scenario: "batch",
        workers: workerCount,
        runCurrent: runIdx + 1,
        runsTotal: runs,
        mapsLoaded: lastMapsLoaded,
        mapsTotal: mapIds.length,
        timeMsMin: timeStats.min,
        timeMsMedian: timeStats.median,
        timeMsMax: timeStats.max,
        throughputMin: throughputStats.min,
        throughputMedian: throughputStats.median,
        throughputMax: throughputStats.max,
        errors: lastErrors,
      });

      const runStartTime = performance.now();
      const results = await executeRun(
        runIdx,
        workerCount,
        mapIds,
        peer,
        workerScript,
      );
      const runEndTime = performance.now();

      const runTime = runEndTime - runStartTime;
      runTimes.push(runTime);

      // Collect latencies from all workers
      for (const r of results) {
        allLatencies.push(...r.latencies);
      }

      // Summary for this run
      const totalMaps = results.reduce((s, r) => s + r.mapsLoaded, 0);
      const totalErrors = results.reduce((s, r) => s + r.errors, 0);
      const throughput = totalMaps / (runTime / 1000);

      // Add to tracked values for stats
      throughputs.push(throughput);

      // Store for next iteration
      lastMapsLoaded = totalMaps;
      lastErrors = totalErrors;

      // Calculate updated stats
      const updatedTimeStats = minMedianMax(runTimes);
      const updatedThroughputStats = minMedianMax(throughputs);

      // Update metrics with run results
      updateScenarioState({
        scenario: "batch",
        workers: workerCount,
        runCurrent: runIdx + 1,
        runsTotal: runs,
        mapsLoaded: totalMaps,
        mapsTotal: mapIds.length,
        timeMsMin: updatedTimeStats.min,
        timeMsMedian: updatedTimeStats.median,
        timeMsMax: updatedTimeStats.max,
        throughputMin: updatedThroughputStats.min,
        throughputMedian: updatedThroughputStats.median,
        throughputMax: updatedThroughputStats.max,
        errors: totalErrors,
      });

      console.log(
        JSON.stringify({
          run: runIdx + 1,
          totalTimeMs: runTime,
          mapsLoaded: totalMaps,
          errors: totalErrors,
          throughput,
        }),
      );
    }

    // Calculate statistics
    const runTimeStats = calculateStats(runTimes);
    const latencyStats = calculateStats(allLatencies);

    console.log("\n=== FINAL RESULTS ===");
    console.log(
      JSON.stringify(
        {
          runs,
          workers: workerCount,
          mapsPerRun: mapIds.length,
          totalMapsLoaded: allLatencies.length,
          runTimeStats,
          latencyStats,
        },
        null,
        2,
      ),
    );
  } finally {
    clearScenarioState();
    server?.close();
  }

  console.log("\nDone");
  process.exit(0);
}
