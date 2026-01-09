import { parentPort, workerData } from "node:worker_threads";

import { LocalNode } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WebSocket } from "ws";

import type { MixSpec, OpKind } from "./utils/mix.ts";
import { makeMixCycle } from "./utils/mix.ts";

type WorkerData = {
  workerId: number;
  peer: string;
  durationMs: number;
  inflight: number;
  mixSpec: MixSpec;
  seed: number;
  targets: {
    fileIds: string[];
    mapIds: string[];
  };
};

type WorkerStats = {
  type: "stats";
  workerId: number;
  opsDone: number;
  fileOpsDone: number;
  mapOpsDone: number;
  errors: number;
};

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function makeRng(seed: number): () => number {
  // Mulberry32
  let t = seed >>> 0;
  return () => {
    t += 0x6d2b79f5;
    let x = t;
    x = Math.imul(x ^ (x >>> 15), x | 1);
    x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
}

function pick<T>(arr: T[], rng: () => number): T {
  return arr[Math.floor(rng() * arr.length)]!;
}

class Semaphore {
  private permits: number;
  private waiters: (() => void)[] = [];
  constructor(permits: number) {
    this.permits = Math.max(1, Math.floor(permits));
  }
  async acquire() {
    if (this.permits > 0) {
      this.permits--;
      return;
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.permits--;
  }
  release() {
    this.permits++;
    const w = this.waiters.shift();
    if (w) w();
  }
}

async function main() {
  const data = workerData as WorkerData;
  const rng = makeRng(data.seed);

  const crypto = await WasmCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);
  const node = new LocalNode(
    agentSecret,
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const wsPeer = new WebSocketPeerWithReconnection({
    peer: data.peer,
    reconnectionTimeout: 100,
    pingTimeout: 60_000,
    addPeer: (peer) => node.syncManager.addPeer(peer),
    removePeer: () => {},
    WebSocketConstructor: WebSocket as unknown as typeof globalThis.WebSocket,
  });

  wsPeer.enable();
  await wsPeer.waitUntilConnected();

  parentPort?.postMessage({ type: "hello", workerId: data.workerId });

  const sem = new Semaphore(data.inflight);

  const cycle: OpKind[] = makeMixCycle(data.mixSpec, rng);
  let cycleIdx = 0;

  let opsDone = 0;
  let fileOpsDone = 0;
  let mapOpsDone = 0;
  let errors = 0;

  const startedAt = Date.now();
  const deadline = startedAt + data.durationMs;

  const inFlight = new Set<Promise<void>>();

  async function doOneOp(kind: OpKind) {
    try {
      if (kind === "file") {
        const id = pick(data.targets.fileIds, rng);
        // IDs are stored as strings (from SQLite); cast for the load API.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const v = await node.load(id as any);
        if (v === "unavailable") {
          throw new Error("file unavailable");
        }
        // Verify we have at least the stream info, and optionally a first chunk.
        // (We avoid materializing the entire PDF into memory.)
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const raw: any = v;
        const info = raw.getBinaryStreamInfo?.();
        if (!info) {
          throw new Error("missing binary stream info");
        }
        const chunks = raw.getBinaryChunks?.(true);
        if (
          chunks &&
          Array.isArray(chunks.chunks) &&
          chunks.chunks.length > 0
        ) {
          // Touch the first chunk length to force decode path.
          void chunks.chunks[0]?.byteLength;
        }
        fileOpsDone++;
      } else {
        const id = pick(data.targets.mapIds, rng);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const v = await node.load(id as any);
        if (v === "unavailable") {
          throw new Error("map unavailable");
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const map: any = v;
        map.set(`w${data.workerId}`, opsDone, "trusting");
        mapOpsDone++;
      }
      opsDone++;
    } catch {
      errors++;
      // Backoff slightly on errors to avoid hot-looping.
      await sleep(50 + Math.floor(rng() * 50));
    }
  }

  while (Date.now() < deadline) {
    await sem.acquire();

    const kind = cycle[cycleIdx]!;
    cycleIdx = (cycleIdx + 1) % cycle.length;

    const p = doOneOp(kind).finally(() => {
      sem.release();
    });

    inFlight.add(p);
    p.finally(() => inFlight.delete(p));

    // Periodically emit stats.
    if (opsDone % 20 === 0) {
      const msg: WorkerStats = {
        type: "stats",
        workerId: data.workerId,
        opsDone,
        fileOpsDone,
        mapOpsDone,
        errors,
      };
      parentPort?.postMessage(msg);
    }
  }

  await Promise.allSettled(Array.from(inFlight));

  parentPort?.postMessage({
    type: "stats",
    workerId: data.workerId,
    opsDone,
    fileOpsDone,
    mapOpsDone,
    errors,
  } satisfies WorkerStats);

  wsPeer.disable();
  node.gracefulShutdown();

  parentPort?.postMessage({ type: "done", workerId: data.workerId });
}

await main();
