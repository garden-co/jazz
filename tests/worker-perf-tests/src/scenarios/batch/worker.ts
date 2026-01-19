import { parentPort, workerData } from "node:worker_threads";

import { LocalNode, type RawCoID } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WebSocket } from "ws";

import { Semaphore } from "../../utils/semaphore.ts";
import type { BatchWorkerData, BatchWorkerResult } from "./types.ts";

async function main() {
  const data = workerData as BatchWorkerData;

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

  const latencies: number[] = [];
  let errors = 0;

  const startTime = performance.now();

  const loadPromises: Promise<void>[] = [];

  for (const mapId of data.mapIds) {
    const p = (async () => {
      const opStart = performance.now();
      try {
        const v = await node.loadCoValueCore(mapId as RawCoID, undefined, true);
        const opEnd = performance.now();

        if (v.isAvailable()) {
          latencies.push(opEnd - opStart);
          v.unmount();
        } else {
          errors++;
        }
      } catch {
        errors++;
      }
    })();

    loadPromises.push(p);
  }

  await Promise.allSettled(loadPromises);

  const endTime = performance.now();

  wsPeer.disable();
  node.gracefulShutdown();

  const result: BatchWorkerResult = {
    type: "result",
    workerId: data.workerId,
    runIndex: data.runIndex,
    totalTimeMs: endTime - startTime,
    latencies,
    mapsLoaded: latencies.length,
    errors,
  };

  parentPort?.postMessage(result);
  parentPort?.postMessage({ type: "done", workerId: data.workerId });
}

await main();
