// Alice sends fragmented queries; Mallory's partial/malformed requests cannot
// consume unbounded authority memory or prevent Bob's ordinary requests.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { tsImport } from "tsx/esm/api";
import { createServerHarness } from "./server-runtime.mjs";
const { createJazz, remote } = await tsImport("../../src/backend/index.ts", import.meta.url);
const { schema: s } = await tsImport("../../src/index.ts", import.meta.url);
const wire = await tsImport("../../src/runtime/native-runtime/websocket.ts", import.meta.url);
const { PostcardReader, PostcardWriter } = await tsImport(
  "../../src/runtime/native-runtime/native-codec.ts",
  import.meta.url,
);
let padNextRequest = false;
const thin = await tsImport("../../src/thin/wire.ts", import.meta.url);
const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.never();
  policy.notes.allowInsert.never();
});
const appId = randomUUID(),
  secret = "thin-parts-test-secret";
const mf = createServerHarness(appId, secret);
let jazz, core, opening;
const sockets = [];
async function bounded(promise, ms = 10000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("thin parts timed out")), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
async function connection() {
  const frames = [],
    waiters = [];
  const socket = core.openSocket(
    opening.headers,
    (error, event) => {
      if (error || event.kind === "close") {
        for (const waiter of waiters.splice(0))
          waiter.reject(error ?? new Error(`socket closed: ${event.code} ${event.reason}`));
      } else if (event.kind === "message") {
        for (const frame of wire.decodeWebSocketFrameBatch(event.data)) {
          const waiter = waiters.shift();
          if (waiter) waiter.resolve(frame);
          else frames.push(frame);
        }
      }
    },
    opening.access,
  );
  sockets.push(socket);
  const next = (ms) =>
    bounded(
      frames.length
        ? Promise.resolve(frames.shift())
        : new Promise((resolve, reject) => waiters.push({ resolve, reject })),
      ms,
    );
  for (const frame of opening.frames) socket.send(frame);
  assert.ok(wire.isWireHello(await next()));
  return {
    next,
    close: () => socket.close(),
    send: (frame) => socket.send(wire.encodeWebSocketFrameBatch([frame])),
  };
}
const parts = (size) =>
  thin.thinWireFrames(
    thin.encodeThinRequest(42, {
      type: "query",
      query: new Uint8Array(size),
      options: {},
    }),
  );
const whole = thin.encodeThinRequest(43, { type: "captureSnapshot" });
async function refused(socket, pattern) {
  const frame = await socket.next();
  assert.ok(wire.isWireError(frame));
  assert.match(wire.decodeWireError(frame).message, pattern);
}
try {
  const url = String(await mf.ready).replace(/\/$/, "");
  core = await remote({ url, appId, secret }).open();
  const provider = {
    async open() {
      return {
        ...core,
        openSocket(headers, receive, access) {
          const socket = core.openSocket(headers, receive, access);
          const captured = { headers, access, frames: [] };
          opening = captured;
          return {
            ...socket,
            async sendWithRoom(frame) {
              return this.send(frame);
            },
            send(frame) {
              if (captured.frames.length < 2) captured.frames.push(frame.slice());
              else if (padNextRequest) {
                padNextRequest = false;
                const [request] = wire.decodeWebSocketFrameBatch(frame);
                const reader = new PostcardReader(request);
                assert.equal(reader.u64(), 4);
                const id = reader.u64();
                assert.equal(reader.u64(), 18);
                const context = new TextDecoder().decode(reader.bytes());
                const inner = reader.bytes();
                const writer = new PostcardWriter();
                writer.u64(4);
                writer.u64(id);
                writer.u64(18);
                // Legal JSON whitespace exercises fragmentation without requiring
                // oversized SQL literals or deferred large-value storage.
                writer.bytes(new TextEncoder().encode(" ".repeat(3 * 1024 * 1024) + context));
                writer.bytes(inner);
                for (const part of thin.thinWireFrames(writer.finish()))
                  socket.send(wire.encodeWebSocketFrameBatch([part]));
                return true;
              }
              return socket.send(frame);
            },
          };
        },
      };
    },
  };
  jazz = await createJazz({ app, permissions, store: provider }).ready();
  await jazz.db.insert(app.notes, { title: "kept" }).wait({ tier: "global" });
  padNextRequest = true;
  assert.deepEqual(await jazz.db.all(app.notes.where({ title: "absent" })), []);
  // A large inner query also reaches the query engine, which refuses its
  // oversized literal independently of the transport's frame limit.
  await assert.rejects(
    () => jazz.db.all(app.notes.where({ title: "x".repeat(3 * 1024 * 1024) })),
    /literal over 65536 bytes/,
  );
  assert.equal((await jazz.db.all(app.notes)).length, 1);
  console.log("Large request succeeded; checking invalid fragments.");
  const interleaved = await connection();
  interleaved.send(parts(3 * 1024 * 1024)[0]);
  interleaved.send(whole);
  await refused(interleaved, /inside a split/);
  const malformed = await connection();
  malformed.send(parts(3 * 1024 * 1024)[0]);
  malformed.send(parts(4 * 1024 * 1024)[0]);
  await refused(malformed, /part|split|length/);
  const oversized = await connection();
  oversized.send(parts(9 * 1024 * 1024)[0]);
  await refused(oversized, /budget/);
  console.log("Invalid fragments refused; checking aggregate budget.");
  const alice = await connection(),
    bob = await connection(),
    mallory = await connection();
  alice.send(parts(7 * 1024 * 1024)[0]);
  bob.send(parts(7 * 1024 * 1024)[0]);
  // Let the first two reservations reach the owner before the third.
  await delay(100);
  mallory.send(parts(7 * 1024 * 1024)[0]);
  await refused(mallory, /budget/);
  assert.equal((await jazz.db.all(app.notes)).length, 1);
  alice.close();
  bob.close();
  await delay(100);
  // Released reservations permit another complete large query.
  padNextRequest = true;
  assert.deepEqual(await jazz.db.all(app.notes.where({ title: "absent again" })), []);
  console.log("Budget released after disconnect; checking 90-second timeout.");
  const stalled = await connection();
  stalled.send(parts(3 * 1024 * 1024)[0]);
  const started = Date.now();
  const expired = await stalled.next(100000);
  assert.ok(wire.isWireError(expired));
  assert.match(wire.decodeWireError(expired).message, /stalled/);
  assert.ok(Date.now() - started >= 89000, "production inactivity deadline must be preserved");
  assert.equal((await jazz.db.all(app.notes)).length, 1);
  console.log("Fragmented query, invalid parts, budgets, disconnect and timeout passed.");
} finally {
  for (const socket of sockets) socket.close();
  await jazz?.close();
  await core?.close();
  await mf.dispose();
}
