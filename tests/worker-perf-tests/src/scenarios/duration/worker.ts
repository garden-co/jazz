import { parentPort, workerData } from "node:worker_threads";

import { LocalNode, type RawCoID } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WebSocket } from "ws";

import { makeMixCycle } from "../../utils/mix.ts";
import { makeRng, pick } from "../../utils/rng.ts";
import { Semaphore } from "../../utils/semaphore.ts";
import type {
  DurationWorkerData,
  DurationWorkerStats,
  OpKind,
} from "./types.ts";

async function main() {
  const data = workerData as DurationWorkerData;
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
  let fullFileOpsDone = 0;
  let mapOpsDone = 0;
  let unavailable = 0;

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

    const v = await node.loadCoValueCore(id as RawCoID, undefined, true);

    opsDone++;
    if (v.isAvailable()) {
      if (kind === "file") {
        v.waitForFullStreaming().finally(() => {
          fullFileOpsDone++;
          v.unmount();
        });
        fileOpsDone++;
      } else {
        v.unmount();
        mapOpsDone++;
      }
    } else {
      unavailable++;
    }
  }

  while (Date.now() < deadline) {
    await sem.acquire();

    const p = doOneOp().finally(() => {
      sem.release();
      inFlight.delete(p);
    });

    inFlight.add(p);

    // Periodically emit stats.
    if (opsDone % 20 === 0) {
      const msg: DurationWorkerStats = {
        type: "stats",
        workerId: data.workerId,
        opsDone,
        fileOpsDone,
        fullFileOpsDone,
        mapOpsDone,
        unavailable,
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
    unavailable,
    fullFileOpsDone,
  } satisfies DurationWorkerStats);

  wsPeer.disable();
  node.gracefulShutdown();

  parentPort?.postMessage({ type: "done", workerId: data.workerId });
}

await main();
