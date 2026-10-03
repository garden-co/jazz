#!/usr/bin/env node
// Synthetic Node -> local Jazz Core benchmark. The proxy delays both directions
// over real TCP; it changes neither Jazz messages nor admission/durability rules.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer, connect } from "node:net";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    "sdk-root": { type: "string" },
    "rtt-ms": { type: "string", default: "0" },
    repeats: { type: "string", default: "5" },
    "client-storage": { type: "string", default: "memory" },
    "server-storage": { type: "string", default: "memory" },
    "output-dir": { type: "string" },
    trace: { type: "boolean", default: false },
    "pump-debounce-ms": { type: "string" },
    tier: { type: "string", default: "global" },
    "select-only": { type: "boolean", default: false },
    "matching-rows": { type: "string", default: "1000" },
    "unrelated-rows": { type: "string", default: "0" },
    "deleted-rows": { type: "string", default: "0" },
    "composite-index": { type: "boolean", default: false },
    "ordered-select": { type: "boolean", default: false },
    "fresh-reader": { type: "boolean", default: false },
  },
});
if (!values["sdk-root"])
  throw new Error("Pass --sdk-root /absolute/path/to/jazz-tools (built SDK)");
const sdkRoot = resolve(values["sdk-root"]);
const load = (path) => import(pathToFileURL(join(sdkRoot, path)).href);
const [
  { schema: s },
  { createJazzContext },
  { startLocalJazzServer },
  { WebSocketCarrier, decodeWebSocketFrameBatch },
  { PostcardReader },
  { EXPECTED_NAPI_ARTIFACT_FINGERPRINT },
] = await Promise.all([
  load("dist/index.js"),
  load("dist/backend/create-jazz-context.js"),
  load("dist/dev/dev-server.js"),
  load("dist/runtime/native-runtime/websocket.js"),
  load("dist/runtime/native-runtime/native-codec.js"),
  load("dist/runtime/native-artifact-fingerprint-napi.js"),
]);
const rttMs = Number(values["rtt-ms"]),
  repeats = Number(values.repeats);
assert(Number.isFinite(rttMs) && rttMs >= 0);
assert(Number.isInteger(repeats) && repeats > 0);
const matchingRows = Number(values["matching-rows"]),
  unrelatedRows = Number(values["unrelated-rows"]),
  deletedRows = Number(values["deleted-rows"]);
assert(Number.isInteger(matchingRows) && matchingRows >= 10);
assert(Number.isInteger(unrelatedRows) && unrelatedRows >= 0);
assert(Number.isInteger(deletedRows) && deletedRows >= 0);
for (const storage of [values["client-storage"], values["server-storage"]])
  assert(["memory", "persistent"].includes(storage));
assert(["local", "global"].includes(values.tier));
const outputDir = values["output-dir"]
  ? resolve(values["output-dir"])
  : mkdtempSync(join(tmpdir(), "jazz-sync-latency-"));
mkdirSync(outputDir, { recursive: true });
let items = s
  .table({ runId: s.string(), ordinal: s.int(), value: s.string(), createdAt: s.int() }, {})
  .indexOnly(["runId", "ordinal"]);
if (values["composite-index"]) items = items.compositeIndex(["runId", "ordinal"]);
const app = s.defineApp({ items });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.items.allowRead.always();
  policy.items.allowInsert.always();
  policy.items.allowUpdate.always();
  policy.items.allowDelete.always();
});
const events = [],
  results = [];
let operation = "setup",
  sample = -1,
  startedAt = performance.now();
function event(name, details = {}) {
  if (values.trace)
    events.push({ operation, sample, name, ms: performance.now() - startedAt, ...details });
}
function frameInfo(frame) {
  const reader = new PostcardReader(frame),
    tag = reader.u64();
  if (tag !== 4)
    return {
      tag: ["Hello", "Message", "Error", "Fragment", "Channel", "Credit"][tag] ?? tag,
      bytes: frame.length,
    };
  reader.u64();
  const features = reader.u64();
  reader.option((session) => {
    session.string();
    session.u64BigInt();
    session.option((identity) => identity.string());
  });
  const channel = reader.u64(),
    generation = reader.u64(),
    sequence = reader.u64(),
    classId = reader.u64();
  const first = reader.bool(),
    last = reader.bool(),
    messageBytes = reader.u64(),
    decodedBytes = reader.u64(),
    payload = reader.bytes();
  return {
    tag: "Channel",
    channel,
    generation,
    sequence,
    class: ["Control", "Requests", "Delivery", "Writes", "LargeValue", "Auxiliary", "Progress"][
      classId
    ],
    first,
    last,
    messageBytes,
    decodedBytes,
    bytes: frame.length,
    payloadBytes: payload.length,
    compressed: Boolean(features & 16),
  };
}
const originalSend = WebSocketCarrier.prototype.sendBatch,
  originalReceive = WebSocketCarrier.prototype.handleMessage;
if (values.trace) {
  WebSocketCarrier.prototype.sendBatch = function (frames) {
    event("send", { frames: frames.map(frameInfo) });
    return originalSend.call(this, frames);
  };
  WebSocketCarrier.prototype.handleMessage = function (data) {
    // This Node probe fixes the carrier to binaryType=arraybuffer.
    const bytes = ArrayBuffer.isView(data)
      ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
      : new Uint8Array(data);
    event("receive", { frames: decodeWebSocketFrameBatch(bytes).map(frameInfo) });
    return originalReceive.call(this, data);
  };
}
const server = await startLocalJazzServer({
  appId: randomUUID(),
  schema: app,
  permissions,
  inMemory: values["server-storage"] === "memory",
});
let proxy, context, clientDir;
const peers = new Set(),
  timers = new Set();
async function timed(name, fn) {
  operation = name;
  startedAt = performance.now();
  event("begin");
  const result = await fn();
  const ms = performance.now() - startedAt;
  event("end");
  results.push({ operation: name, sample, ms });
  return result;
}
try {
  let url = server.url;
  if (rttMs > 0) {
    proxy = createServer((socket) => {
      const upstream = connect({ port: server.port, host: "127.0.0.1" });
      socket.setNoDelay(true);
      upstream.setNoDelay(true);
      peers.add(socket);
      peers.add(upstream);
      for (const [source, destination] of [
        [socket, upstream],
        [upstream, socket],
      ]) {
        source.on("data", (chunk) => {
          const timer = setTimeout(() => {
            timers.delete(timer);
            if (!destination.destroyed) destination.write(chunk);
          }, rttMs / 2);
          timers.add(timer);
        });
        source.on("error", () => destination.destroy());
        source.on("close", () => {
          peers.delete(source);
          destination.destroy();
        });
      }
    });
    await new Promise((resolve, reject) => {
      proxy.once("error", reject);
      proxy.listen(0, "127.0.0.1", resolve);
    });
    url = `http://127.0.0.1:${proxy.address().port}/`;
  }
  if (values["client-storage"] === "persistent")
    clientDir = mkdtempSync(join(tmpdir(), "jazz-sync-client-"));
  const openContext = () =>
    createJazzContext({
      appId: server.appId,
      app,
      permissions,
      driver: clientDir
        ? { type: "persistent", dataPath: join(clientDir, "db") }
        : { type: "memory" },
      serverUrl: url,
      backendSecret: server.backendSecret,
      adminSecret: server.adminSecret,
    });
  context = openContext();
  let db = context.asBackend();
  function configureRuntime() {
    const runtime = context.coreSource.currentRuntime;
    if (values["pump-debounce-ms"] !== undefined) {
      const delayMs = Number(values["pump-debounce-ms"]);
      assert(Number.isFinite(delayMs) && delayMs >= 0);
      // Trial confined to this owner. Keep the existing single-pump, generation,
      // tick, permission and storage paths; alter only its scheduling delay.
      runtime.scheduleServerPump = function () {
        if (this.closed || !this.serverTransport) return;
        if (this.serverPumpRunning) {
          this.serverPumpAgain = true;
          return;
        }
        if (this.serverPumpScheduled) return;
        this.serverPumpScheduled = true;
        setTimeout(() => {
          this.serverPumpScheduled = false;
          if (!this.closed) this.startServerPump();
        }, delayMs);
      };
    }
    if (values.trace)
      for (const name of [
        "commitTransaction",
        "runCoreTick",
        "routePendingInboundServerFrames",
        "startRowsForContext",
        "awaitNativeRead",
      ]) {
        const original = runtime[name];
        assert.equal(typeof original, "function", `Missing profiling hook ${name}`);
        runtime[name] = function (...args) {
          const begin = performance.now();
          event(`${name}.start`);
          const result = original.apply(this, args);
          const finish = () => event(`${name}.end`, { durationMs: performance.now() - begin });
          if (result?.then) return result.finally(finish);
          finish();
          return result;
        };
      }
  }
  configureRuntime();
  async function create(count, runId, tier = values.tier, ordinalStart = 0) {
    const ids = Array.from({ length: count }, () => randomUUID());
    const commit = await db.transaction((tx) => {
      for (let index = 0; index < count; index++)
        tx.insert(
          app.items,
          {
            runId,
            ordinal: ordinalStart + index,
            value: `item-${ordinalStart + index}`,
            createdAt: Math.floor(Date.now() / 1000),
          },
          { id: ids[index] },
        );
    });
    event("transaction.returned");
    await commit.wait({ tier });
    event("wait.returned");
    context.flush();
    return ids;
  }
  const warmupIds = await timed("warmup", () => create(1, "warmup", "global"));
  if (values["select-only"]) {
    operation = "seed";
    async function seed(count, runId, remove = false) {
      const allIds = [];
      for (let start = 0; start < count; start += 1000) {
        const ids = await create(Math.min(1000, count - start), runId, "global", start);
        if (remove) {
          const commit = await db.transaction((tx) =>
            ids.forEach((id) => tx.delete(app.items, id)),
          );
          await commit.wait({ tier: "global" });
          context.flush();
        } else allIds.push(...ids);
      }
      return allIds;
    }
    const runId = `selected-${randomUUID()}`;
    const ids = await seed(matchingRows, runId);
    await seed(unrelatedRows, `unrelated-${randomUUID()}`);
    await seed(deletedRows, `deleted-${randomUUID()}`, true);
    if (values["fresh-reader"]) {
      await context.shutdown();
      if (clientDir) {
        rmSync(clientDir, { recursive: true, force: true });
        clientDir = mkdtempSync(join(tmpdir(), "jazz-sync-reader-"));
      }
      context = openContext();
      db = context.asBackend();
      configureRuntime();
      // Exclude connection/authentication startup, while keeping every timed
      // query on the ordinary Global durability and sync path.
      await db.one(app.items.where({ id: warmupIds[0] }), { tier: "global" });
    }
    console.log(JSON.stringify({ ready: process.pid, matchingRows, unrelatedRows, deletedRows }));
    const opts = { tier: values.tier };
    const expectedIds = new Set(ids);
    const expectedPageIds = values["ordered-select"]
      ? ids.slice(0, 10)
      : ids.toSorted().slice(0, 10);
    const firstPage = values["ordered-select"]
      ? app.items.where({ runId }).orderBy("ordinal", "asc").limit(10)
      : app.items.where({ runId }).limit(10);
    for (sample = 0; sample < repeats; sample++) {
      const rows = await timed("select10", () => db.all(firstPage, opts));
      assert.equal(rows.length, 10);
      assert.deepEqual(
        rows.map((row) => row.id),
        expectedPageIds,
      );
      assert.equal(new Set(rows.map((row) => row.id)).size, 10);
      for (const row of rows) {
        assert(expectedIds.has(row.id));
        assert.equal(row.runId, runId);
        assert.equal(row.value, `item-${row.ordinal}`);
      }
      const top = await timed("selectTopN", () =>
        db.all(app.items.where({ runId }).orderBy("ordinal", "desc").limit(10), opts),
      );
      assert.deepEqual(
        top.map((row) => row.ordinal),
        Array.from({ length: 10 }, (_, i) => matchingRows - 1 - i),
      );
      assert.deepEqual(
        top.map((row) => row.id),
        ids.slice(-10).reverse(),
      );
      const row = await timed("getById", () => db.one(app.items.where({ id: ids[0] }), opts));
      assert.equal(row.id, ids[0]);
      assert.equal(row.ordinal, 0);
      assert.equal(row.value, "item-0");
    }
  } else
    for (sample = 0; sample < repeats; sample++) {
      const runId = `probe-${randomUUID()}`;
      const one = await timed("createOne", () => create(1, runId));
      const ids = await timed("create1k", () => create(1000, runId));
      const query = app.items.where({ runId }).orderBy("ordinal", "desc").limit(10);
      const opts = { tier: values.tier };
      const read10 = await timed("select10", () =>
        db.all(app.items.where({ runId }).limit(10), opts),
      );
      assert.equal(read10.length, 10);
      const top = await timed("selectTopN", () => db.all(query, opts));
      assert.equal(top.length, 10);
      assert.equal(top[0].ordinal, 999);
      const row = await timed("getById", () => db.one(app.items.where({ id: ids[0] }), opts));
      assert.equal(row.id, ids[0]);
      await timed("updateById", async () => {
        await db.update(app.items, ids[0], { value: "updated" }).wait(opts);
        context.flush();
      });
      await timed("updateTopN", async () => {
        const rows = await db.all(query, opts);
        event("read.returned");
        assert.equal(rows.length, 10);
        const commit = await db.transaction((tx) =>
          rows.forEach((item) => tx.update(app.items, item.id, { value: "updated" })),
        );
        event("transaction.returned");
        await commit.wait(opts);
        event("wait.returned");
        context.flush();
      });
      // Drain all writes through the real authority before starting another sample.
      await timed("cleanup", async () => {
        const commit = await db.transaction((tx) =>
          [...one, ...ids].forEach((id) => tx.delete(app.items, id)),
        );
        await commit.wait({ tier: "global" });
      });
    }
  await db.delete(app.items, warmupIds[0]).wait({ tier: "global" });
  const median = (xs) => {
    const sorted = xs.toSorted((a, b) => a - b),
      middle = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  };
  const mediansMs = Object.fromEntries(
    [...new Set(results.map((x) => x.operation))]
      .filter((x) => !["warmup", "cleanup"].includes(x))
      .map((op) => [op, median(results.filter((x) => x.operation === op).map((x) => x.ms))]),
  );
  const receipt = {
    sdkRoot,
    sdkVersion: JSON.parse(readFileSync(join(sdkRoot, "package.json"))).version,
    nativeArtifactFingerprint: EXPECTED_NAPI_ARTIFACT_FINGERPRINT,
    nodeVersion: process.version,
    rttMs,
    repeats,
    tier: values.tier,
    workload: values["select-only"] ? "fixed-select" : "crud-with-cleanup",
    compositeIndex: values["composite-index"],
    orderedSelect: values["ordered-select"],
    freshReader: values["fresh-reader"],
    ...(values["select-only"] ? { matchingRows, unrelatedRows, deletedRows } : {}),
    clientStorage: values["client-storage"],
    serverStorage: values["server-storage"],
    trace: values.trace,
    pumpDebounceMs: values["pump-debounce-ms"] ?? "default",
    results,
    mediansMs,
    events,
  };
  const workload = values["select-only"]
    ? `select-${matchingRows}-${unrelatedRows}-${deletedRows}-`
    : "";
  const indexLabel = `${values["composite-index"] ? "composite-" : ""}${values["ordered-select"] ? "ordinal-" : ""}`;
  const filename = `${workload}${indexLabel}${values["fresh-reader"] ? "fresh-" : ""}rtt-${rttMs}-${values["client-storage"]}-${values["server-storage"]}-${values.tier}-${values.trace ? "trace" : "timing"}-${values["pump-debounce-ms"] ?? "default"}.json`;
  const outputPath = join(outputDir, filename);
  writeFileSync(outputPath, JSON.stringify(receipt, null, 2));
  console.log(JSON.stringify({ outputPath, mediansMs }, null, 2));
} finally {
  await context?.shutdown();
  for (const timer of timers) clearTimeout(timer);
  for (const peer of peers) peer.destroy();
  if (proxy) await new Promise((resolve) => proxy.close(resolve));
  await server.stop();
  if (clientDir) rmSync(clientDir, { recursive: true, force: true });
  WebSocketCarrier.prototype.sendBatch = originalSend;
  WebSocketCarrier.prototype.handleMessage = originalReceive;
}
