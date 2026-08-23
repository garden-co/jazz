#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, "../../..");
const fixturePath = requiredEnv("JAZZ_WASM_INGEST_FIXTURE");
const outFile = resolve(
  process.env.JAZZ_WASM_INGEST_RECEIPT ??
    join(dirname(fixturePath), `wasm-ingest-replay-${timestamp()}.json`),
);
const maxWaitMs = Number(process.env.JAZZ_WASM_INGEST_WAIT_MS ?? "120000");
const microtaskRounds = Number(process.env.JAZZ_WASM_INGEST_MICROTASK_ROUNDS ?? "4");
const feedMode = process.env.JAZZ_WASM_INGEST_FEED_MODE ?? "coalesced";

const [{ encodeSchema }, { openConfig }, websocketCodec, adapterModule, wasmModule] =
  await Promise.all([
    import(
      pathToFileURL(
        join(repoRoot, "packages/jazz-tools/dist/runtime/native-runtime/schema-codec.js"),
      )
    ),
    import(
      pathToFileURL(
        join(repoRoot, "packages/jazz-tools/dist/runtime/native-runtime/native-codec.js"),
      )
    ),
    import(
      pathToFileURL(join(repoRoot, "packages/jazz-tools/dist/runtime/native-runtime/websocket.js"))
    ),
    import(
      pathToFileURL(
        join(repoRoot, "packages/jazz-tools/dist/runtime/native-runtime/native-runtime-adapter.js"),
      )
    ),
    import(pathToFileURL(join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm.js"))),
  ]);

const fixture = JSON.parse(await readFile(fixturePath, "utf8"));
const open = fixture.workerCapture?.open ?? fixture.open;
if (!open?.schema) throw new Error("fixture is missing workerCapture.open.schema");

const wasmBytes = await readFile(join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm"));
const loadStart = performance.now();
await wasmModule.default(wasmBytes);
const loadMs = performance.now() - loadStart;

const schema = open.schema;
const node = base64Bytes(envOr(process.env.JAZZ_WASM_INGEST_NODE_B64, open.node));
const author = base64Bytes(envOr(process.env.JAZZ_WASM_INGEST_AUTHOR_B64, open.author));
const sourceId = Number(process.env.JAZZ_WASM_INGEST_SOURCE_ID ?? "1");
const historyComplete = process.env.JAZZ_WASM_INGEST_HISTORY_COMPLETE !== "0";

const openStart = performance.now();
const db = wasmModule.WasmDb.openMemory(
  encodeSchema(schema),
  openConfig(node, author, sourceId, historyComplete),
);
const openMs = performance.now() - openStart;

const adapter = adapterModule.NativeRuntimeAdapter.fromDb(
  db,
  schema,
  node,
  author,
  sourceId,
  historyComplete,
);
const transport = adapter.connectUpstreamPeer();
const discardedOutbound = [];
adapter.serverTransport = transport;
adapter.serverCarrier = {
  sendBatch: async (frames) => {
    discardedOutbound.push(...frames);
  },
};
adapter.serverCarrierPromise = Promise.resolve(adapter.serverCarrier);

const subscriptions = fixture.workerCapture?.subscriptions ?? fixture.subscriptions ?? [];
const subscriptionCallbacks = new Map();
const errors = [];
let callbackSelfMs = 0;
let replayStart = performance.now();
const subscribeStart = performance.now();
for (const subscription of subscriptions) {
  const handle = adapter.createSubscription(
    subscription.queryJson,
    subscription.sessionJson,
    subscription.tier,
    subscription.optionsJson,
  );
  subscriptionCallbacks.set(handle, {
    handle,
    ownerHandle: subscription.ownerHandle,
    callbackCount: 0,
    firstCallbackAtMs: null,
    addedCount: 0,
    removedCount: 0,
    updatedCount: 0,
  });
  adapter.executeSubscription(handle, (deltaOrError) => {
    const callbackStart = performance.now();
    try {
      const state = subscriptionCallbacks.get(handle);
      if (!state) return;
      if (deltaOrError instanceof Error) {
        errors.push({ handle, message: deltaOrError.message, stack: deltaOrError.stack });
        return;
      }
      state.callbackCount += 1;
      state.firstCallbackAtMs ??= performance.now() - replayStart;
      state.addedCount += Number(deltaOrError?.addedCount ?? 0);
      state.removedCount += Number(deltaOrError?.removedCount ?? 0);
      state.updatedCount += Number(deltaOrError?.updatedCount ?? 0);
    } finally {
      callbackSelfMs += performance.now() - callbackStart;
    }
  });
}
const subscribeMs = performance.now() - subscribeStart;

const decodeStart = performance.now();
const decodedFrames = decodeFixtureServerFrames(fixture, websocketCodec);
const decodeMs = performance.now() - decodeStart;
replayStart = performance.now();
const frameTimings = [];
let fedFrames = 0;

if (feedMode === "single") {
  for (let index = 0; index < decodedFrames.length; index += 1) {
    const frame = decodedFrames[index];
    const before = performance.now();
    adapter.pendingInboundServerFrames.push(frame);
    fedFrames += 1;
    const pumpStart = performance.now();
    adapter.pumpServerTransport();
    const pumpMs = performance.now() - pumpStart;
    const drainStart = performance.now();
    await drainMicrotasks(adapter);
    const drainMs = performance.now() - drainStart;
    frameTimings.push({
      index,
      frames: 1,
      bytes: frame.byteLength,
      elapsedMs: performance.now() - before,
      pumpMs,
      drainMs,
      callbacks: readySubscriptionCount(subscriptionCallbacks),
    });
  }
} else {
  const before = performance.now();
  adapter.pendingInboundServerFrames.push(...decodedFrames);
  fedFrames = decodedFrames.length;
  const pumpStart = performance.now();
  adapter.pumpServerTransport();
  const pumpMs = performance.now() - pumpStart;
  const drainStart = performance.now();
  await drainMicrotasks(adapter);
  const drainMs = performance.now() - drainStart;
  frameTimings.push({
    index: 0,
    frames: decodedFrames.length,
    bytes: decodedFrames.reduce((sum, frame) => sum + frame.byteLength, 0),
    elapsedMs: performance.now() - before,
    pumpMs,
    drainMs,
    callbacks: readySubscriptionCount(subscriptionCallbacks),
  });
}

const settled = await waitForCallbacks(adapter, subscriptionCallbacks, maxWaitMs);
const ingestMs = settled.elapsedMs;
await closeAdapter(adapter);

const receipt = {
  version: 1,
  fixturePath,
  outFile,
  feedMode,
  wasm: {
    packageJs: join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm.js"),
    packageWasm: join(repoRoot, "crates/jazz-wasm/pkg/jazz_wasm_bg.wasm"),
  },
  counts: {
    subscriptions: subscriptions.length,
    fedFrames,
    fedBytes: decodedFrames.reduce((sum, frame) => sum + frame.byteLength, 0),
    discardedOutboundFrames: discardedOutbound.length,
    callbacksReady: readySubscriptionCount(subscriptionCallbacks),
    errors: errors.length,
  },
  timingMs: {
    loadWasm: round(loadMs),
    openMemory: round(openMs),
    subscribe: round(subscribeMs),
    decodeWebsocketBatches: round(decodeMs),
    replayToAllCallbacks: round(ingestMs),
    framePump: round(frameTimings.reduce((sum, frame) => sum + frame.pumpMs, 0)),
    microtaskDrain: round(frameTimings.reduce((sum, frame) => sum + frame.drainMs, 0)),
    callbackSelf: round(callbackSelfMs),
    wall: round(loadMs + openMs + subscribeMs + decodeMs + ingestMs),
  },
  frameTimingSummary: summarizeFrames(frameTimings),
  slowFrames: frameTimings
    .filter((frame) => frame.elapsedMs >= 50)
    .sort((left, right) => right.elapsedMs - left.elapsedMs)
    .slice(0, 20)
    .map((frame) => ({ ...frame, elapsedMs: round(frame.elapsedMs) })),
  subscriptions: Array.from(subscriptionCallbacks.values()).map((state) => ({
    ...state,
    firstCallbackAtMs: state.firstCallbackAtMs == null ? null : round(state.firstCallbackAtMs),
  })),
  errors,
};

await writeFile(outFile, `${JSON.stringify(receipt, null, 2)}\n`);
console.log(
  JSON.stringify({
    ok: errors.length === 0 && settled.ok,
    receipt: outFile,
    ...receipt.counts,
    timingMs: receipt.timingMs,
  }),
);

function decodeFixtureServerFrames(rawFixture, codec) {
  if (Array.isArray(rawFixture.receivedFrames)) {
    return rawFixture.receivedFrames.map((frame) =>
      base64Bytes(typeof frame === "string" ? frame : frame.base64),
    );
  }

  const sockets = rawFixture.websockets ?? [];
  const binaryBatches = sockets.flatMap((socket) =>
    (socket.received ?? []).filter((frame) => frame.kind === "binary" && frame.base64),
  );
  if (binaryBatches.length === 0 && Array.isArray(rawFixture.receivedBatches)) {
    binaryBatches.push(
      ...rawFixture.receivedBatches.filter((frame) => frame.kind === "binary" && frame.base64),
    );
  }
  if (binaryBatches.length === 0) {
    throw new Error("fixture has no received binary websocket batches or receivedFrames");
  }

  const frames = [];
  for (const batch of binaryBatches) {
    for (const frame of codec.decodeWebSocketFrameBatch(base64Bytes(batch.base64))) {
      if (codec.isWireHello(frame)) continue;
      if (codec.isWireError(frame)) {
        const error = codec.decodeWireError(frame);
        throw new Error(`captured wire error: ${error.code} ${error.retry} ${error.message}`);
      }
      frames.push(frame);
    }
  }
  return frames;
}

async function drainMicrotasks(adapter) {
  for (let i = 0; i < microtaskRounds; i += 1) {
    await new Promise((resolveMicrotask) => setImmediate(resolveMicrotask));
    adapter.pumpSubscriptions?.();
    adapter.pumpServerTransport?.();
  }
}

async function waitForCallbacks(adapter, states, timeoutMs) {
  const start = performance.now();
  while (performance.now() - start < timeoutMs) {
    if (readySubscriptionCount(states) >= states.size) {
      return { ok: true, elapsedMs: performance.now() - replayStart };
    }
    await drainMicrotasks(adapter);
  }
  return { ok: false, elapsedMs: performance.now() - replayStart };
}

async function closeAdapter(adapter) {
  try {
    await adapter.close?.();
  } catch {
    // Profiling harness shutdown should not mask the receipt.
  }
}

function readySubscriptionCount(states) {
  let ready = 0;
  for (const state of states.values()) {
    if (state.callbackCount > 0) ready += 1;
  }
  return ready;
}

function summarizeFrames(frames) {
  if (frames.length === 0) return { count: 0 };
  const elapsed = frames.map((frame) => frame.elapsedMs).sort((a, b) => a - b);
  return {
    count: frames.length,
    totalMs: round(elapsed.reduce((sum, value) => sum + value, 0)),
    maxMs: round(elapsed.at(-1)),
    p50Ms: round(percentile(elapsed, 0.5)),
    p90Ms: round(percentile(elapsed, 0.9)),
    p99Ms: round(percentile(elapsed, 0.99)),
  };
}

function percentile(sorted, p) {
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.floor(sorted.length * p));
  return sorted[index];
}

function base64Bytes(value) {
  if (!value) throw new Error("missing base64 bytes");
  return new Uint8Array(Buffer.from(value, "base64"));
}

function envOr(envValue, fallback) {
  return envValue == null || envValue === "" ? fallback : envValue;
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return resolve(value);
}

function round(value) {
  return Math.round(value * 10) / 10;
}

function timestamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z");
}
