/**
 * Browser integration test for the Rust `attach-follower-port` handler
 * (`crates/jazz-wasm/src/worker_host.rs::handle_attach_follower_port`).
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright, against a
 * real dedicated jazz Worker with an open runtime (the same dedicated-Worker
 * bootstrap that `worker-bridge.test.ts` exercises — we reuse `createDb`
 * rather than inventing a parallel worker harness).
 *
 * Flow:
 *   1. `createDb` (persistent driver) spawns a dedicated Worker and becomes
 *      leader. `ensureBridgeReady()` resolves once the worker posted its
 *      postcard `InitOk` (runtime open).
 *   2. We attach our OWN `message` listener on the worker so we can observe the
 *      raw postcard bytes the worker posts back — the `WorkerBridge` installs
 *      `worker.onmessage` and also consumes the ack, but `addEventListener`
 *      coexists with `onmessage`, so both receive every message.
 *   3. We transfer a `MessagePort` via the `attach-follower-port` JS-object
 *      message and assert the worker replies with the postcard
 *      `WorkerToMainWire::FollowerPortAttached { follower_tab_id, generation }`
 *      carrying our exact `followerTabId` + `generation`.
 *   4. We then push a follower-sync payload through the port and assert the
 *      worker does NOT post an `Error` back (the runtime route is async; sync
 *      application is covered by later tasks — here we only verify the attach
 *      accepted the port and the route did not blow up synchronously).
 */

import { describe, expect, it, afterEach } from "vitest";
import { decodeWorkerToMainJs } from "jazz-wasm";
import { createDb, Db } from "../../src/runtime/db.js";
import { schema as s } from "../../src/";
import { TestCleanup, uniqueDbName, waitForCondition } from "./support.js";

// Minimal schema — enough to open a real runtime in the worker.
const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
const app = s.defineApp(schema);

async function supportsDedicatedWorkerPersistentRuntime(): Promise<boolean> {
  let db: Db | null = null;
  try {
    db = (await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("attach-follower-port-probe") },
    })) as Db;
    await db.all(app.todos, { tier: "local" });
    await (db as unknown as { ensureBridgeReady(): Promise<void> }).ensureBridgeReady();
    return Boolean((db as unknown as { worker: Worker | null }).worker);
  } catch {
    return false;
  } finally {
    await db?.shutdown().catch(() => undefined);
  }
}

const dedicatedWorkerPersistentRuntimeSupported = await supportsDedicatedWorkerPersistentRuntime();

interface DecodedFollowerPortAttached {
  type: "follower-port-attached";
  followerTabId: string;
  generation: number;
}

/** Attempt to decode a worker→main message via `decodeWorkerToMainJs`. Returns
 * the decoded object if it succeeds, or null if the variant is not supported or
 * the bytes are not a valid postcard envelope. */
function tryDecode(bytes: Uint8Array): { type?: string; message?: string } | null {
  if (!(bytes instanceof Uint8Array)) return null;
  try {
    return decodeWorkerToMainJs(bytes) as { type?: string; message?: string };
  } catch {
    return null;
  }
}

describe.skipIf(!dedicatedWorkerPersistentRuntimeSupported)(
  "worker-host attach-follower-port (dedicated Worker)",
  () => {
    const ctx = new TestCleanup();

    afterEach(async () => {
      await ctx.cleanup();
    });

    it("accepts a transferred port and acks with FollowerPortAttached", async () => {
      const db = ctx.track(
        await createDb({
          appId: "test-app",
          driver: { type: "persistent", dbName: uniqueDbName("attach-follower-port") },
        }),
      ) as Db;

      // A query forces the worker runtime open + attaches the WorkerBridge (the
      // bridge is created lazily on first client use). The `local` tier waits for
      // the worker's settled snapshot, so by the time it resolves the worker has
      // posted `InitOk`. `app.todos` carries the schema the runtime opens with.
      await db.all(app.todos, { tier: "local" });
      await (db as unknown as { ensureBridgeReady(): Promise<void> }).ensureBridgeReady();
      const worker = (db as unknown as { worker: Worker | null }).worker;
      expect(worker).toBeTruthy();
      if (!worker) throw new Error("dedicated worker not available after ensureBridgeReady");

      // Observe the raw postcard bytes the worker posts back. `addEventListener`
      // coexists with the WorkerBridge's `worker.onmessage`, so we see the ack
      // even though the bridge also consumes it.
      const attachedAcks: DecodedFollowerPortAttached[] = [];
      const errorMessages: string[] = [];
      const listener = (event: MessageEvent) => {
        const data = event.data;
        if (!(data instanceof Uint8Array)) return;
        const decoded = tryDecode(data);
        if (!decoded) return;
        if (decoded.type === "follower-port-attached") {
          attachedAcks.push(decoded as DecodedFollowerPortAttached);
        } else if (decoded.type === "error") {
          errorMessages.push(decoded.message ?? "(no message)");
        }
      };
      worker.addEventListener("message", listener);

      const followerTabId = `tab-${Math.random().toString(36).slice(2, 10)}`;
      const leaderTabId = `leader-${Math.random().toString(36).slice(2, 10)}`;
      const generation = 1;

      const mc = new MessageChannel();
      try {
        worker.postMessage(
          { type: "attach-follower-port", followerTabId, leaderTabId, generation },
          [mc.port1],
        );

        await waitForCondition(
          async () =>
            attachedAcks.some(
              (ack) => ack.followerTabId === followerTabId && ack.generation === generation,
            ),
          10000,
          `worker should post FollowerPortAttached for ${followerTabId}; ` +
            `acks=${JSON.stringify(attachedAcks)}; errors=${JSON.stringify(errorMessages)}`,
        );

        const ack = attachedAcks.find(
          (a) => a.followerTabId === followerTabId && a.generation === generation,
        );
        expect(ack).toEqual({ type: "follower-port-attached", followerTabId, generation });
        // The attach path must not have produced an Error envelope.
        expect(errorMessages).toEqual([]);

        // Step 6 of the plan: push a follower-sync payload through the attached
        // port. The runtime route is async; we assert the worker doesn't post an
        // Error back (sync application is covered by later tasks).
        mc.port2.start();
        mc.port2.postMessage({ type: "follower-sync", payload: [new Uint8Array([1, 2, 3])] });

        // Give the worker a window to (mis)handle the payload, then assert quiet.
        await new Promise((resolve) => setTimeout(resolve, 300));
        expect(errorMessages).toEqual([]);
      } finally {
        worker.removeEventListener("message", listener);
        mc.port1.close();
        mc.port2.close();
      }
    });
  },
);
