import cronometro from "cronometro";
import * as localTools from "jazz-tools";
import * as latestPublishedTools from "jazz-tools-latest";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import { WasmCrypto as LatestPublishedWasmCrypto } from "cojson-latest/crypto/WasmCrypto";

// --- Test data (pre-generated, not measured) ---

const CHUNK_SIZE = 4096;

function makeChunks(totalBytes: number, chunkSize: number): Uint8Array[] {
  const chunks: Uint8Array[] = [];
  let remaining = totalBytes;
  while (remaining > 0) {
    const size = Math.min(chunkSize, remaining);
    const chunk = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      chunk[i] = Math.floor(Math.random() * 256);
    }
    chunks.push(chunk);
    remaining -= size;
  }
  return chunks;
}

const chunks100k = makeChunks(100 * 1024, CHUNK_SIZE);
const chunks1m = makeChunks(1024 * 1024, CHUNK_SIZE);
const chunks5m = makeChunks(5 * 1024 * 1024, CHUNK_SIZE);

// --- Context setup helper ---

type Tools = typeof localTools;
type WasmCryptoClass = typeof LocalWasmCrypto;

interface BenchContext {
  account: InstanceType<Tools["Account"]>;
  node: ReturnType<Tools["createJazzContextForNewAccount"]> extends Promise<
    infer R
  >
    ? R extends { node: infer N }
      ? N
      : never
    : never;
  FileStream: Tools["FileStream"];
}

async function createContext(
  tools: Tools,
  wasmCrypto: WasmCryptoClass,
): Promise<BenchContext> {
  const ctx = await tools.createJazzContextForNewAccount({
    creationProps: { name: "Bench Account" },
    peers: [],
    crypto: await wasmCrypto.create(),
    sessionProvider: new tools.MockSessionProvider(),
  });
  return {
    account: ctx.account as InstanceType<Tools["Account"]>,
    node: ctx.node as BenchContext["node"],
    FileStream: tools.FileStream,
  };
}

// --- Stream population helper ---

function populateStream(ctx: BenchContext, chunks: Uint8Array[]) {
  let totalBytes = 0;
  for (const c of chunks) totalBytes += c.length;

  const stream = ctx.FileStream.create({ owner: ctx.account });
  stream.start({
    mimeType: "application/octet-stream",
    totalSizeBytes: totalBytes,
  });
  for (const chunk of chunks) {
    stream.push(chunk);
  }
  stream.end();
  return stream;
}

const benchOptions = {
  iterations: 20,
  warmup: true,
  print: {
    colors: true,
    compare: true,
  },
  onTestError: (testName: string, error: unknown) => {
    console.error(`\nError in test "${testName}":`);
    console.error(error);
  },
};

// =============================================================
// Write 100KB
// =============================================================

let ctx: BenchContext;

await cronometro(
  {
    "Write 100KB - @latest": {
      async before() {
        ctx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
      },
      test() {
        populateStream(ctx, chunks100k);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
    "Write 100KB - @workspace": {
      async before() {
        ctx = await createContext(localTools, LocalWasmCrypto);
      },
      test() {
        populateStream(ctx, chunks100k);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// Write 1MB
// =============================================================

await cronometro(
  {
    "Write 1MB - @latest": {
      async before() {
        ctx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
      },
      test() {
        populateStream(ctx, chunks1m);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
    "Write 1MB - @workspace": {
      async before() {
        ctx = await createContext(localTools, LocalWasmCrypto);
      },
      test() {
        populateStream(ctx, chunks1m);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// Write 5MB
// =============================================================

await cronometro(
  {
    "Write 5MB - @latest": {
      async before() {
        ctx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
      },
      test() {
        populateStream(ctx, chunks5m);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
    "Write 5MB - @workspace": {
      async before() {
        ctx = await createContext(localTools, LocalWasmCrypto);
      },
      test() {
        populateStream(ctx, chunks5m);
      },
      async after() {
        (ctx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// getChunks 1MB
// =============================================================

let readCtx: BenchContext;
let readStream: InstanceType<Tools["FileStream"]>;

await cronometro(
  {
    "getChunks 1MB - @latest": {
      async before() {
        readCtx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
        readStream = populateStream(readCtx, chunks1m);
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
    "getChunks 1MB - @workspace": {
      async before() {
        readCtx = await createContext(localTools, LocalWasmCrypto);
        readStream = populateStream(readCtx, chunks1m);
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// getChunks 5MB
// =============================================================

await cronometro(
  {
    "getChunks 5MB - @latest": {
      async before() {
        readCtx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
        readStream = populateStream(readCtx, chunks5m);
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
    "getChunks 5MB - @workspace": {
      async before() {
        readCtx = await createContext(localTools, LocalWasmCrypto);
        readStream = populateStream(readCtx, chunks5m);
      },
      test() {
        readStream.getChunks();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// asBase64 1MB
// =============================================================

await cronometro(
  {
    "asBase64 1MB - @latest": {
      async before() {
        readCtx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
        readStream = populateStream(readCtx, chunks1m);
      },
      test() {
        readStream.asBase64();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
    "asBase64 1MB - @workspace": {
      async before() {
        readCtx = await createContext(localTools, LocalWasmCrypto);
        readStream = populateStream(readCtx, chunks1m);
      },
      test() {
        readStream.asBase64();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);

// =============================================================
// asBase64 5MB
// =============================================================

await cronometro(
  {
    "asBase64 5MB - @latest": {
      async before() {
        readCtx = await createContext(
          // @ts-expect-error version mismatch
          latestPublishedTools,
          LatestPublishedWasmCrypto,
        );
        readStream = populateStream(readCtx, chunks5m);
      },
      test() {
        readStream.asBase64();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
    "asBase64 5MB - @workspace": {
      async before() {
        readCtx = await createContext(localTools, LocalWasmCrypto);
        readStream = populateStream(readCtx, chunks5m);
      },
      test() {
        readStream.asBase64();
      },
      async after() {
        (readCtx.node as any).gracefulShutdown();
      },
    },
  },
  benchOptions,
);
