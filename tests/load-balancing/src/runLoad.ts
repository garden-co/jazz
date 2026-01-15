import { Worker } from "node:worker_threads";

import type { CoValueHeader } from "cojson";
import { NapiCrypto } from "cojson/crypto/NapiCrypto";
import { startSyncServer } from "jazz-run/startSyncServer";

import type { ParsedArgs } from "./utils/args.ts";
import { getFlagNumber, getFlagString } from "./utils/args.ts";
import { parseMixSpec } from "./utils/mix.ts";
import { readAllCoValues } from "./utils/sqliteCoValues.ts";
import { setupMetrics } from "./metrics.ts";

type HeaderRow = { id: string; header: CoValueHeader };

function assertNonEmptyString(
  value: string | undefined,
  label: string,
): string {
  if (!value || value.trim() === "") {
    throw new Error(`Missing required ${label}`);
  }
  return value;
}

function classifyTargets(rows: HeaderRow[]) {
  const fileIds: string[] = [];
  const mapIds: string[] = [];

  for (const row of rows) {
    if (row.header.type === "costream" && row.header.meta?.type === "binary") {
      fileIds.push(row.id);
    } else if (row.header.type === "comap") {
      mapIds.push(row.id);
    }
  }

  return { fileIds, mapIds };
}

type WorkerHello = { type: "hello"; workerId: number };
type WorkerStats = {
  type: "stats";
  workerId: number;
  opsDone: number;
  fileOpsDone: number;
  mapOpsDone: number;
  errors: number;
};
type WorkerDone = { type: "done"; workerId: number };
type WorkerMessage = WorkerHello | WorkerStats | WorkerDone;

export async function runLoad(args: ParsedArgs): Promise<void> {
  const dbPath = assertNonEmptyString(
    getFlagString(args, "db") ?? "./seed.db",
    "--db",
  );

  const workers = getFlagNumber(args, "workers") ?? 8;
  const durationMs = getFlagNumber(args, "durationMs") ?? 60_000;
  const inflight = getFlagNumber(args, "inflight") ?? 4;
  const host = getFlagString(args, "host") ?? "127.0.0.1";
  const port = getFlagString(args, "port") ?? "0";

  const mix = getFlagString(args, "mix") ?? "1f:1m";
  const mixModeRaw = (getFlagString(args, "mixMode") ?? "round_robin") as
    | "round_robin"
    | "randomized";
  const mixSpec = parseMixSpec(mix, mixModeRaw);

  const rows = readAllCoValues(dbPath).map(
    (r) => ({ id: r.id, header: r.header }) satisfies HeaderRow,
  );
  const { fileIds, mapIds } = classifyTargets(rows);

  if (fileIds.length === 0) {
    throw new Error(
      "No FileStream targets found (expected header.type=costream and meta.type=binary)",
    );
  }
  if (mapIds.length === 0) {
    throw new Error("No CoMap targets found (expected header.type=comap)");
  }

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

  console.log(
    JSON.stringify(
      {
        metrics: `http://${addr.address}:${addr.port}/metrics`,
        db: dbPath,
        peer,
        workers,
        durationMs,
        inflight,
        mix: `${mixSpec.files}f:${mixSpec.maps}m`,
        mixMode: mixSpec.mode,
        targets: { files: fileIds.length, maps: mapIds.length },
      },
      null,
      2,
    ),
  );

  const workerScript = new URL("./worker.ts", import.meta.url);

  const byId = new Map<number, WorkerStats>();
  const startedAt = Date.now();
  let doneCount = 0;

  const ws: Worker[] = [];

  const allDone = new Promise<void>((resolve, reject) => {
    for (let workerId = 0; workerId < workers; workerId++) {
      const w = new Worker(workerScript, {
        // Ensure workers can execute TS too (even if the parent was started differently).
        execArgv: ["--experimental-strip-types"],
        workerData: {
          workerId,
          peer,
          durationMs,
          inflight,
          mixSpec,
          seed: startedAt ^ (workerId * 2654435761),
          targets: { fileIds, mapIds },
        },
      });

      w.on("message", (msg: WorkerMessage) => {
        if (msg.type === "stats") {
          byId.set(msg.workerId, msg);
        } else if (msg.type === "done") {
          doneCount++;
          if (doneCount === workers) resolve();
        }
      });

      w.on("error", (e) => {
        reject(e);
      });

      ws.push(w);
    }
  });

  const printStats = () => {
    let ops = 0;
    let fileOps = 0;
    let mapOps = 0;
    let errors = 0;

    for (const s of byId.values()) {
      ops += s.opsDone;
      fileOps += s.fileOpsDone;
      mapOps += s.mapOpsDone;
      errors += s.errors;
    }

    const elapsedMs = Date.now() - startedAt;
    const opsPerSec = elapsedMs > 0 ? ops / (elapsedMs / 1000) : 0;

    console.log(
      JSON.stringify(
        {
          elapsedMs,
          workers: { total: workers, done: doneCount },
          ops: { total: ops, fileOps, mapOps, errors, opsPerSec },
        },
        null,
        2,
      ),
    );
  };

  const printInterval = setInterval(printStats, 2_000);

  try {
    await allDone;
  } finally {
    clearInterval(printInterval);
    printStats();
    for (const w of ws) {
      // Ensure workers are not left running if the parent exits early.
      await w.terminate();
    }
    server.close();
  }

  console.log("Done");
  process.exit(0);
}
