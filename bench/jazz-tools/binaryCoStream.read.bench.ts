import cronometro from "cronometro";
import * as localTools from "jazz-tools";
import * as latestPublishedTools from "jazz-tools-latest";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import { cojsonInternals } from "cojson";
import { WasmCrypto as LatestPublishedWasmCrypto } from "cojson-latest/crypto/WasmCrypto";

// --- Test data (pre-generated, not measured) ---

const CHUNK_SIZE = cojsonInternals.TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE;

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
  iterations: 500,
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
        readStream = populateStream(
          readCtx,
          makeChunks(1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(5 * 1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(5 * 1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(5 * 1024 * 1024, CHUNK_SIZE),
        );
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
        readStream = populateStream(
          readCtx,
          makeChunks(5 * 1024 * 1024, CHUNK_SIZE),
        );
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
