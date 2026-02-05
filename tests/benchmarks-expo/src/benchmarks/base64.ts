import {
  bytesToBase64url as nativeBytesToBase64url,
  base64urlToBytes as nativeBase64urlToBytes,
} from "cojson-core-rn";
import { BenchmarkSuite, BenchmarkConfig, ComparisonResult } from "./types";
import { generateRandomBytes, runBenchmark, pause } from "./runner";

// ============================================================================
// JavaScript Fallback Implementation (from cojson/base64url.ts)
// ============================================================================

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const alphabet =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const lookup = new Uint8Array(128);
for (const [i, a] of Array.from(alphabet).entries()) {
  lookup[a.charCodeAt(0)] = i;
}
lookup["=".charCodeAt(0)] = 0;

const encodeLookup = new Uint8Array(64);
for (const [i, a] of Array.from(alphabet).entries()) {
  encodeLookup[i] = a.charCodeAt(0);
}

function jsBytesToBase64url(bytes: Uint8Array): string {
  const m = bytes.length;
  const k = m % 3;
  const n = Math.floor(m / 3) * 4 + (k && k + 1);
  const N = Math.ceil(m / 3) * 4;
  const encoded = new Uint8Array(N);

  for (let i = 0, j = 0; j < m; i += 4, j += 3) {
    const y = (bytes[j]! << 16) + (bytes[j + 1]! << 8) + (bytes[j + 2]! | 0);
    encoded[i] = encodeLookup[y >> 18]!;
    encoded[i + 1] = encodeLookup[(y >> 12) & 0x3f]!;
    encoded[i + 2] = encodeLookup[(y >> 6) & 0x3f]!;
    encoded[i + 3] = encodeLookup[y & 0x3f]!;
  }

  let base64 = decoder.decode(new Uint8Array(encoded.buffer, 0, n));
  if (k === 1) base64 += "==";
  if (k === 2) base64 += "=";

  return base64;
}

function jsBase64urlToBytes(base64: string): Uint8Array {
  base64 = base64.replace(/=/g, "");
  const n = base64.length;
  const rem = n % 4;
  const k = rem && rem - 1;
  const m = (n >> 2) * 3 + k;

  const encoded = new Uint8Array(n + 3);
  encoder.encodeInto(base64 + "===", encoded);

  for (let i = 0, j = 0; i < n; i += 4, j += 3) {
    const x =
      (lookup[encoded[i]!]! << 18) +
      (lookup[encoded[i + 1]!]! << 12) +
      (lookup[encoded[i + 2]!]! << 6) +
      lookup[encoded[i + 3]!]!;
    encoded[j] = x >> 16;
    encoded[j + 1] = (x >> 8) & 0xff;
    encoded[j + 2] = x & 0xff;
  }
  return new Uint8Array(encoded.buffer, 0, m);
}

// ============================================================================
// Native Rust Wrappers
// ============================================================================

function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  if (
    view.byteOffset === 0 &&
    view.byteLength === view.buffer.byteLength &&
    view.buffer instanceof ArrayBuffer
  ) {
    return view.buffer;
  }
  const buffer = new ArrayBuffer(view.byteLength);
  new Uint8Array(buffer).set(view);
  return buffer;
}

function rustBytesToBase64url(bytes: Uint8Array): string {
  return nativeBytesToBase64url(toArrayBuffer(bytes));
}

function rustBase64urlToBytes(base64: string): Uint8Array {
  return new Uint8Array(nativeBase64urlToBytes(base64));
}

// ============================================================================
// Benchmark Suite
// ============================================================================

const config: BenchmarkConfig = {
  name: "Base64 Encoding/Decoding",
  description: "Compare JavaScript fallback vs Native Rust implementation",
  iterations: 100,
  sizes: [
    { label: "1 KB", bytes: 1024 },
    { label: "10 KB", bytes: 10 * 1024 },
    { label: "100 KB", bytes: 100 * 1024 },
    { label: "500 KB", bytes: 500 * 1024 },
    { label: "1 MB", bytes: 1024 * 1024 },
  ],
};

/**
 * Verify that native produces same output as JS
 */
function verifyCorrectness(): { ok: boolean; error?: string } {
  try {
    const testBytes = new Uint8Array([72, 101, 108, 108, 111]); // "Hello"

    const jsEncoded = jsBytesToBase64url(testBytes);
    const rustEncoded = rustBytesToBase64url(testBytes);

    if (jsEncoded !== rustEncoded) {
      return {
        ok: false,
        error: `Encode mismatch: JS="${jsEncoded}" Rust="${rustEncoded}"`,
      };
    }

    const rustDecoded = rustBase64urlToBytes(rustEncoded);
    if (rustDecoded.length !== testBytes.length) {
      return { ok: false, error: "Decode length mismatch" };
    }

    for (let i = 0; i < testBytes.length; i++) {
      if (rustDecoded[i] !== testBytes[i]) {
        return { ok: false, error: `Byte mismatch at index ${i}` };
      }
    }

    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

export const base64Benchmark: BenchmarkSuite = {
  config,

  async run(onProgress, onResult) {
    // Verify correctness first
    onProgress("Verifying native implementation correctness...");
    await pause(100);

    const verification = verifyCorrectness();
    if (!verification.ok) {
      onProgress(`❌ Verification failed: ${verification.error}`);
      return;
    }
    onProgress("✓ Native implementation verified correct");
    await pause(100);

    const { iterations, sizes } = config;

    for (const { label, bytes } of sizes) {
      onProgress(`[${label}] Generating test data...`);
      await pause(50);

      const testBytes = generateRandomBytes(bytes);
      const testBase64 = jsBytesToBase64url(testBytes);

      // Encode benchmarks
      onProgress(`[${label}] Running encode benchmarks...`);
      await pause(50);

      const encodeJS = runBenchmark(
        `JS Encode ${label}`,
        () => jsBytesToBase64url(testBytes),
        iterations,
        bytes,
      );

      const encodeRust = runBenchmark(
        `Rust Encode ${label}`,
        () => rustBytesToBase64url(testBytes),
        iterations,
        bytes,
      );

      onResult({
        size: `${label} Encode`,
        baseline: encodeJS,
        optimized: encodeRust,
        speedup: encodeJS.avgMs / encodeRust.avgMs,
      });

      // Decode benchmarks
      onProgress(`[${label}] Running decode benchmarks...`);
      await pause(50);

      const decodeJS = runBenchmark(
        `JS Decode ${label}`,
        () => jsBase64urlToBytes(testBase64),
        iterations,
        bytes,
      );

      const decodeRust = runBenchmark(
        `Rust Decode ${label}`,
        () => rustBase64urlToBytes(testBase64),
        iterations,
        bytes,
      );

      onResult({
        size: `${label} Decode`,
        baseline: decodeJS,
        optimized: decodeRust,
        speedup: decodeJS.avgMs / decodeRust.avgMs,
      });

      onProgress(`[${label}] Complete`);
      await pause(100);
    }

    onProgress("All benchmarks complete!");
  },
};
