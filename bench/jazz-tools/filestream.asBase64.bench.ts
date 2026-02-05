import cronometro from "cronometro";
import { cojsonInternals } from "cojson";

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

function asBase64Old(chunks: Uint8Array[]): string | undefined {
  // Using String.fromCharCode.apply with batches of bytes is significantly faster
  // than building the string byte-by-byte (e.g., `result += String.fromCharCode(byte)`).
  // Each string concatenation creates a new string object, making byte-by-byte O(n²).
  // With apply, we convert many bytes at once, reducing string allocations.
  //
  // We limit batch size to 32KB because V8 has ~64k argument limit for function calls.
  const BATCH_SIZE = 32768;
  const parts: string[] = [];

  // Process each chunk directly without merging into a single buffer
  for (const chunk of chunks) {
    for (let i = 0; i < chunk.length; i += BATCH_SIZE) {
      parts.push(
        String.fromCharCode.apply(
          null,
          chunk.subarray(
            i,
            Math.min(i + BATCH_SIZE, chunk.length),
          ) as unknown as number[],
        ),
      );
    }
  }

  return btoa(parts.join(""));
}

function asBase64New(chunks: Uint8Array[]) {
  // Calculate actual loaded bytes (may differ from totalSizeBytes when allowUnfinished)
  let loadedBytes = 0;
  for (const chunk of chunks) {
    loadedBytes += chunk.length;
  }

  // Merge all chunks into a single Uint8Array
  const merged = new Uint8Array(loadedBytes);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }

  const base64 = cojsonInternals.bytesToBase64url(merged);

  return base64;
}

function asBase64Native(chunks: Uint8Array[]) {
  // Calculate actual loaded bytes (may differ from totalSizeBytes when allowUnfinished)
  let loadedBytes = 0;
  for (const chunk of chunks) {
    loadedBytes += chunk.length;
  }

  // Merge all chunks into a single Uint8Array
  const merged = new Uint8Array(loadedBytes);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.length;
  }

  // @ts-expect-error - toBase64 is not a method of Uint8Array
  const base64 = merged.toBase64({ alphabet: "base64url" });

  return base64;
}

const benchOptions = {
  iterations: 100,
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
let chunks: Uint8Array[];

await cronometro(
  {
    "asBase64 - old (btoa + String.fromCharCode)": {
      async before() {
        chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
      },
      test() {
        asBase64Old(chunks);
      },
    },
    "asBase64 - new (bytesToBase64url)": {
      async before() {
        chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
      },
      test() {
        asBase64New(chunks);
      },
    },
    "asBase64 - native (toBase64)": {
      async before() {
        chunks = makeChunks(TOTAL_BYTES, CHUNK_SIZE);
      },
      test() {
        asBase64Native(chunks);
      },
    },
  },
  benchOptions,
);
