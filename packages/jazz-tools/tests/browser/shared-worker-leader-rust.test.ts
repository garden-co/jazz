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
 *
 * ## Why we hand-decode the postcard ack instead of `decodeWorkerToMainJs`
 *
 * `jazz-wasm` exports `decodeWorkerToMainJs`, but its match arm only covers a
 * subset of variants (`Error`, `ShutdownFailed`, `DebugSchemaStateOk`,
 * `DebugSeedLiveSchemaOk`, `InitOk`) and throws `unsupported variant` for
 * everything else — including `FollowerPortAttached` /
 * `FollowerPortAttachFailed` (verified in `worker_protocol.rs` and in the built
 * `crates/jazz-wasm/pkg/jazz_wasm_bg.wasm`: it contains `attach-follower-port`
 * and `followerTabId` but not `follower-port-attached`). So we decode the
 * postcard envelope for this one variant shape directly. postcard (1.1.x)
 * encodes an enum as `varint(discriminant)` followed by the variant fields;
 * here the fields are a length-prefixed UTF-8 `String` (`follower_tab_id`) and
 * a `varint(u32)` (`generation`). We validate by matching the unique
 * `followerTabId` we sent, which unambiguously identifies the variant.
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

interface VarintResult {
  value: number;
  next: number;
}

/** Read a postcard/LEB128 unsigned varint starting at `offset`. */
function readVarint(bytes: Uint8Array, offset: number): VarintResult | null {
  let result = 0;
  let shift = 0;
  let pos = offset;
  while (pos < bytes.length) {
    const byte = bytes[pos];
    result |= (byte & 0x7f) << shift;
    pos += 1;
    if ((byte & 0x80) === 0) {
      // `>>> 0` coerces back to an unsigned 32-bit value.
      return { value: result >>> 0, next: pos };
    }
    shift += 7;
    if (shift > 35) return null; // malformed (u32 varint is <= 5 bytes)
  }
  return null;
}

interface DecodedFollowerPortAttached {
  followerTabId: string;
  generation: number;
}

/**
 * Decode a postcard `WorkerToMainWire::FollowerPortAttached` envelope.
 *
 * Returns `null` if `bytes` is not a clean encoding of exactly that variant
 * (wrong field shapes, or trailing bytes), so iterating every worker→main
 * message and keeping only the clean match is safe — the unique
 * `followerTabId` string we send makes a false positive impossible.
 */
function tryDecodeFollowerPortAttached(bytes: Uint8Array): DecodedFollowerPortAttached | null {
  // discriminant varint
  const tag = readVarint(bytes, 0);
  if (!tag) return null;
  // follower_tab_id: length-prefixed UTF-8 string
  const len = readVarint(bytes, tag.next);
  if (!len) return null;
  const strEnd = len.next + len.value;
  if (strEnd > bytes.length) return null;
  let followerTabId: string;
  try {
    followerTabId = new TextDecoder("utf-8", { fatal: true }).decode(
      bytes.subarray(len.next, strEnd),
    );
  } catch {
    return null;
  }
  // generation: varint(u32)
  const gen = readVarint(bytes, strEnd);
  if (!gen) return null;
  // A clean FollowerPortAttached has no trailing bytes after `generation`.
  if (gen.next !== bytes.length) return null;
  return { followerTabId, generation: gen.value };
}

/** Decode a worker→main message into `{type,...}` if `decodeWorkerToMainJs` supports it. */
function tryDecodeKnownJs(bytes: Uint8Array): { type?: string; message?: string } | null {
  try {
    return decodeWorkerToMainJs(bytes) as { type?: string; message?: string };
  } catch {
    return null;
  }
}

describe("worker-host attach-follower-port (dedicated Worker)", () => {
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
      const attached = tryDecodeFollowerPortAttached(data);
      if (attached) {
        attachedAcks.push(attached);
        return;
      }
      const known = tryDecodeKnownJs(data);
      if (known?.type === "error") {
        errorMessages.push(known.message ?? "(no message)");
      }
    };
    worker.addEventListener("message", listener);

    const followerTabId = `tab-${Math.random().toString(36).slice(2, 10)}`;
    const leaderTabId = `leader-${Math.random().toString(36).slice(2, 10)}`;
    const generation = 1;

    const mc = new MessageChannel();
    try {
      worker.postMessage({ type: "attach-follower-port", followerTabId, leaderTabId, generation }, [
        mc.port1,
      ]);

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
      expect(ack).toEqual({ followerTabId, generation });
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
});
