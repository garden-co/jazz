import cronometro from "cronometro";
import * as localCojson from "cojson";
import { cojsonInternals, LocalNode, RawBinaryCoStream } from "cojson";
import { WasmCrypto as LocalWasmCrypto } from "cojson/crypto/WasmCrypto";
import * as latestCojson from "cojson-latest";
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

type CojsonModule = typeof import("cojson");
type WasmCryptoClass = typeof LocalWasmCrypto;

interface BenchContext {
  node: LocalNode;
  stream: RawBinaryCoStream;
}

async function createContext(
  cojson: CojsonModule,
  wasmCrypto: WasmCryptoClass,
): Promise<BenchContext> {
  const crypto = await wasmCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const sessionID = crypto.newRandomSessionID(crypto.getAgentID(agentSecret));
  const node = new cojson.LocalNode(agentSecret, sessionID, crypto);

  const coValue = node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: { type: "binary" },
    ...crypto.createdNowUnique(),
  });

  const content = coValue.getCurrentContent();
  if (!(content instanceof cojson.RawBinaryCoStream)) {
    throw new Error("Expected binary stream");
  }

  return {
    node,
    stream: content,
  };
}

// --- Stream population helper ---

function populateStream(
  ctx: BenchContext,
  chunks: Uint8Array[],
  cojson: CojsonModule,
) {
  let totalBytes = 0;
  for (const c of chunks) totalBytes += c.length;

  const coValue = ctx.node.createCoValue({
    type: "costream",
    ruleset: { type: "unsafeAllowAll" },
    meta: { type: "binary" },
    ...ctx.node.crypto.createdNowUnique(),
  });

  const stream = coValue.getCurrentContent();
  if (!(stream instanceof cojson.RawBinaryCoStream)) {
    throw new Error("Expected binary stream");
  }

  stream.startBinaryStream(
    { mimeType: "application/octet-stream", totalSizeBytes: totalBytes },
    "trusting",
  );
  for (const chunk of chunks) {
    stream.pushBinaryStreamChunk(chunk, "trusting");
  }
  stream.endBinaryStream("trusting");
  return stream;
}

const benchOptions = {
  iterations: 50,
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

const TOTAL_BYTES = 5 * 1024 * 1024;

let ctx: BenchContext;
let chunks: Uint8Array[];

await cronometro(
  {
    "Write - @latest": {
      async before() {
        ctx = await createContext(latestCojson, LatestPublishedWasmCrypto);
        chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
      },
      test() {
        populateStream(ctx, chunks, latestCojson);
      },
      async after() {
        await ctx.node.gracefulShutdown();
      },
    },
    "Write - @workspace": {
      async before() {
        ctx = await createContext(localCojson, LocalWasmCrypto);
        chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
      },
      test() {
        populateStream(ctx, chunks, localCojson);
      },
      async after() {
        await ctx.node.gracefulShutdown();
      },
    },
  },
  benchOptions,
);
