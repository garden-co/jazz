import { Worker } from "node:worker_threads";

import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { startSyncServer } from "jazz-run/startSyncServer";

import type { ParsedArgs } from "../../utils/args.ts";
import {
  getFlagBoolean,
  getFlagNumber,
  getFlagString,
} from "../../utils/args.ts";
import { parseMixSpec } from "../../utils/mix.ts";
import { setupMetrics } from "../../metrics.ts";
import { readConfigId, loadSeedConfig } from "../../utils/loadSeedConfig.ts";
import {
  assertNonEmptyString,
  getConfigFilePath,
} from "../../utils/seedHelpers.ts";
import {
  updateScenarioState,
  clearScenarioState,
} from "../../scenarioMetrics.ts";
import type { DurationSeedConfig } from "../../schema.ts";
import type { DurationWorkerStats, DurationWorkerData } from "./types.ts";

type WorkerHello = { type: "hello"; workerId: number };
type WorkerDone = { type: "done"; workerId: number };
type WorkerMessage = WorkerHello | DurationWorkerStats | WorkerDone;

/**
 * Duration Load Scenario
 *
 * Concurrently load files and comaps for X seconds.
 * Loads the SeedConfig CoValue first to get the IDs to use.
 */
export async function run(args: ParsedArgs): Promise<void> {
  const dbPath = assertNonEmptyString(
    getFlagString(args, "db") ?? "./duration.db",
    "--db",
  );

  const workerCount = getFlagNumber(args, "workers") ?? 8;
  const durationMs = getFlagNumber(args, "durationMs") ?? 60_000;
  const inflight = getFlagNumber(args, "inflight") ?? 4;
  const host = getFlagString(args, "host") ?? "127.0.0.1";
  const randomPort = getFlagBoolean(args, "random-port");
  const port = getFlagString(args, "port") ?? (randomPort ? "0" : "4000");

  const mix = getFlagString(args, "mix") ?? "1f:1m";
  const mixModeRaw = (getFlagString(args, "mixMode") ?? "round_robin") as
    | "round_robin"
    | "randomized";
  const mixSpec = parseMixSpec(mix, mixModeRaw);

  // Read the seed config ID from the config file
  const configId = readConfigId(dbPath);
  if (!configId) {
    throw new Error(
      `No seed config found. Expected config file at: ${getConfigFilePath(dbPath)}. Run seed first.`,
    );
  }

  // Start the sync server
  const server = await startSyncServer({
    host,
    port,
    inMemory: false,
    db: dbPath,
    crypto: await NapiCrypto.create(),
    middleware: setupMetrics().middleware,
  });

  const addr = server.address();
  if (!addr || typeof addr === "string") {
    throw new Error("Unexpected server address()");
  }
  const peer = `ws://${addr.address}:${addr.port}`;

  // Load the seed config from the server to get IDs
  console.log("Loading seed configuration...");
  const seedConfig = await loadSeedConfig(peer, configId);

  if (seedConfig.scenario !== "duration") {
    server.close();
    throw new Error(
      `Database was seeded for "${seedConfig.scenario}" scenario, but "duration" was requested`,
    );
  }

  const config = seedConfig as DurationSeedConfig;
  const fileIds = config.fileIds;
  const mapIds = config.mapIds;

  if (fileIds.length === 0) {
    server.close();
    throw new Error("No FileStream targets found in seed config");
  }
  if (mapIds.length === 0) {
    server.close();
    throw new Error("No CoMap targets found in seed config");
  }

  console.log(
    JSON.stringify(
      {
        scenario: "duration",
        metrics: `http://${addr.address}:${addr.port}/metrics`,
        db: dbPath,
        peer,
        workers: workerCount,
        durationMs,
        inflight,
        mix: `${mixSpec.files}f:${mixSpec.maps}m`,
        mixMode: mixSpec.mode,
        targets: { files: fileIds.length, maps: mapIds.length },
        configId,
      },
      null,
      2,
    ),
  );

  const workerScript = new URL("./worker.ts", import.meta.url);

  const byId = new Map<number, DurationWorkerStats>();
  const startedAt = Date.now();
  let doneCount = 0;

  const ws: Worker[] = [];

  const allDone = new Promise<void>((resolve, reject) => {
    for (let workerId = 0; workerId < workerCount; workerId++) {
      const workerData: DurationWorkerData = {
        workerId,
        peer,
        durationMs,
        inflight,
        mixSpec,
        seed: startedAt ^ (workerId * 2654435761),
        targets: {
          fileIds: fileIds as string[],
          mapIds: mapIds as string[],
        },
      };

      const w = new Worker(workerScript, {
        execArgv: ["--experimental-strip-types"],
        workerData,
      });

      w.on("message", (msg: WorkerMessage) => {
        if (msg.type === "stats") {
          byId.set(msg.workerId, msg);
        } else if (msg.type === "done") {
          doneCount++;
          if (doneCount === workerCount) resolve();
        }
      });

      w.on("error", (e) => {
        reject(e);
      });

      ws.push(w);
    }
  });

  const updateStats = () => {
    let ops = 0;
    let fileOpsCount = 0;
    let fullFileOpsCount = 0;
    let mapOpsCount = 0;
    let unavailableCount = 0;

    for (const s of byId.values()) {
      ops += s.opsDone;
      fileOpsCount += s.fileOpsDone;
      fullFileOpsCount += s.fullFileOpsDone;
      mapOpsCount += s.mapOpsDone;
      unavailableCount += s.unavailable;
    }

    const elapsedMs = Date.now() - startedAt;
    const opsPerSec = elapsedMs > 0 ? ops / (elapsedMs / 1000) : 0;

    // Update Prometheus metrics
    updateScenarioState({
      scenario: "duration",
      workers: workerCount,
      elapsedMs,
      targetMs: durationMs,
      opsTotal: ops,
      opsPerSecond: opsPerSec,
      fileOps: fileOpsCount,
      mapOps: mapOpsCount,
      fullFileOps: fullFileOpsCount,
      unavailable: unavailableCount,
    });

    console.log(
      JSON.stringify({
        opsPerSec: Number(opsPerSec.toFixed(5)),
        duration: (elapsedMs / 1000).toFixed(0) + "s",
        totalOps: ops,
        fileOps: fileOpsCount,
        fullFileOps: fullFileOpsCount,
        mapOps: mapOpsCount,
        unavailable: unavailableCount,
      }),
    );
  };

  // Initialize metrics state
  updateScenarioState({
    scenario: "duration",
    workers: workerCount,
    elapsedMs: 0,
    targetMs: durationMs,
    opsTotal: 0,
    opsPerSecond: 0,
    fileOps: 0,
    mapOps: 0,
    fullFileOps: 0,
    unavailable: 0,
  });

  const statsInterval = setInterval(updateStats, 2_000);

  try {
    await allDone;
  } finally {
    clearInterval(statsInterval);
    updateStats();
    clearScenarioState();
    for (const w of ws) {
      await w.terminate();
    }
    server.close();
  }

  console.log("Done");
  process.exit(0);
}
