import { parentPort, workerData } from "node:worker_threads";

import { LocalNode, type RawCoID } from "cojson";
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

  async function doOneOp() {
    const kind = cycle[cycleIdx]!;
    cycleIdx = (cycleIdx + 1) % cycle.length;

    const id =
      kind === "file"
        ? pick(data.targets.fileIds, rng)
        : pick(data.targets.mapIds, rng);

    const v = await node.loadCoValueCore(id as RawCoID);

    opsDone++;
    if (!v.isAvailable()) {
      console.error("coValue unavailable", id);
      await sleep(50 + Math.floor(rng() * 50));
    } else {
      if (kind === "file") {
        fileOpsDone++;
      } else {
        mapOpsDone++;
      }
    }

    v.unmount();
  }

  while (Date.now() < deadline) {
    await sem.acquire();

    const p = doOneOp().finally(() => {
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
