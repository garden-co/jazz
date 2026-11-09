import { describe, expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { createOpenTransactionId } from "../client.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadNapiModule } from "../testing/napi-runtime-test-utils.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

type PendingRead = { poll(): Uint8Array | null; cancel(): void };
type TickCallback = (first: Error | string | null, second?: string) => void;
type RawDb = {
  all(query: Uint8Array, opts: Record<string, unknown>, id?: string): Uint8Array | PendingRead;
  setTickScheduler(callback: TickCallback): void;
  setRemoteLinkHint(hint: string): void;
  tick(): unknown;
  connectUpstream(): unknown;
  close(): unknown;
};
const app = s.defineApp({ notes: s.table({ body: s.string() }, {}) }, { migrations: "none" });
const author = testAuthorBytes("native-read-scheduling:alice");
const node = new Uint8Array(16).fill(7);
const query = queryFromTable("notes");
const bounded = { tier: "local-first", first_load_remote_wait_ms: 70 };

async function open(host: "NAPI" | "WASM"): Promise<RawDb> {
  const ctor =
    host === "NAPI" ? (await loadNapiModule()).NapiDb : (await loadWasmModuleForTest()).WasmDb;
  return ctor.openMemory(encodeSchema(app.wasmSchema), openConfig(node, author, 1, false));
}

function pending(read: Uint8Array | PendingRead): PendingRead {
  expect(read).not.toBeInstanceOf(Uint8Array);
  return read as PendingRead;
}

// Only the exercised boundary is observed. Every other method is bound to the
// real NAPI/WASM receiver, including its scheduler, evaluator and cancellation.
function observe(raw: RawDb, overrides: Partial<RawDb>): RawDb {
  return new Proxy(raw, {
    get(target, key) {
      if (key in overrides) return Reflect.get(overrides, key);
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
function runtime(db: RawDb): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(null, app.wasmSchema, node, author, 1, false, {
    db: db as never,
  });
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}

for (const host of ["NAPI", "WASM"] as const) {
  describe(`${host} core read scheduling`, () => {
    for (const installAfterRead of [false, true]) {
      it(`finishes a quiet ordinary deadline with the existing SDK timer (${installAfterRead ? "late" : "early"} hook)`, async () => {
        const raw = await open(host);
        await raw.connectUpstream();
        raw.setRemoteLinkHint("live");
        const done = deferred<Uint8Array>();
        const wakes: string[] = [];
        let read: PendingRead | undefined;
        let polls = 0;
        let adapter: NativeRuntimeAdapter | undefined;
        const start = performance.now();
        const watched = observe(raw, {
          setTickScheduler(callback) {
            raw.setTickScheduler((first, second) => {
              const wake = typeof first === "string" ? first : second;
              if (wake) wakes.push(wake);
              callback(first, second);
            });
          },
          async tick() {
            await raw.tick();
            if (read) {
              try {
                polls += 1;
                const bytes = read.poll();
                if (bytes !== null) done.resolve(bytes);
              } catch (error) {
                done.reject(error);
              }
            }
          },
        });
        const watchdog = setTimeout(
          () => done.reject(new Error("core deadline did not wake host")),
          2000,
        );
        try {
          if (!installAfterRead) adapter = runtime(watched);
          read = pending(raw.all(query, bounded));
          if (installAfterRead) adapter = runtime(watched);
          // No transport frames, polling loop or test-driven tick. Re-polling
          // occurs only after the production adapter handles a core callback.
          expect(await done.promise).toBeInstanceOf(Uint8Array);
          expect(performance.now() - start).toBeGreaterThanOrEqual(65);
          expect(wakes.some((wake) => wake.startsWith("after:"))).toBe(true);
          expect(polls).toBeGreaterThan(0);
          expect(polls).toBeLessThan(20);
        } finally {
          clearTimeout(watchdog);
          read?.cancel();
          await adapter?.close();
        }
      });
    }

    for (const stop of ["close", "discard"] as const) {
      it(`${stop} cancels real issued and queued transaction reads before replacement`, async () => {
        const raw = await open(host);
        const reads: PendingRead[] = [];
        const cancelled = new Set<PendingRead>();
        const admitted = deferred<void>();
        const watched = observe(raw, {
          all(bytes, opts, id) {
            const read = pending(raw.all(bytes, opts, id));
            reads.push(read);
            if (reads.length === 2) admitted.resolve();
            return {
              poll: () => read.poll(),
              cancel: () => {
                read.cancel();
                cancelled.add(read);
              },
            };
          },
        });
        const adapter = runtime(watched);
        const transport = await adapter.connectUpstreamPeer();
        raw.setRemoteLinkHint("live");
        const id = createOpenTransactionId();
        adapter.beginTransaction("serializable", id);
        const opts = JSON.stringify({ transaction_id: id });
        // #4588 admits the first read and queues the second in the same batch.
        const outcomes = [0, 1].map(() =>
          adapter.query(JSON.stringify({ table: "notes" }), null, "remote", opts).then(
            () => "answered",
            (error: unknown) => String(error),
          ),
        );
        const watchdog = setTimeout(
          () => admitted.reject(new Error("reads were not admitted")),
          2000,
        );
        try {
          await admitted.promise;
          // Let both SDK continuations register in the existing cancel set.
          await new Promise((resolve) => setTimeout(resolve, 0));
          expect(reads.every((read) => read.poll() === null)).toBe(true);
          await transport.tick();
          // Read=0 and Unsubscribe=1 are pinned by sqlite_sync_wire_v1.json.
          // Exactly one request crossed the real host boundary: the second
          // read is still queued behind the first in the transaction.
          const issued = transport.recvWireFrames() as Uint8Array[];
          const requests = issued.filter((frame) => frame[0] === 0);
          expect(requests).toHaveLength(1);
          await adapter[stop]();
          expect(cancelled.size).toBe(2);
          const results = await Promise.all(outcomes);
          expect(results.every((result) => result.includes("runtime shutdown"))).toBe(true);
          for (const read of reads) expect(() => read.poll()).toThrow(/complete|cancelled/i);
          await transport.tick();
          const cleanup = transport.recvWireFrames() as Uint8Array[];
          expect(cleanup.filter((frame) => frame[0] === 0)).toHaveLength(0);
          // Remote issued-request retirement is supplied by the accepted
          // transport dependency; this slice establishes local cancellation.
          // Reusing the opaque transaction id on a replacement owner cannot
          // adopt either old read; its local query completes independently.
          const replacement = runtime(await open(host));
          try {
            replacement.beginTransaction("serializable", id);
            expect(
              await replacement.query(JSON.stringify({ table: "notes" }), null, "local-only", opts),
            ).toEqual([]);
            await replacement.rollbackTransaction(id);
          } finally {
            await replacement.close();
          }
        } finally {
          clearTimeout(watchdog);
          await adapter.close();
          await raw.close();
        }
      });
    }
  });
}
