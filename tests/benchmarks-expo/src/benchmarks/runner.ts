import { BenchmarkResult } from "./types";

/**
 * Generate random bytes for testing
 */
export function generateRandomBytes(size: number): Uint8Array {
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    bytes[i] = Math.floor(Math.random() * 256);
  }
  return bytes;
}

/**
 * Run a benchmark and return results
 */
export function runBenchmark(
  name: string,
  fn: () => void,
  iterations: number,
  bytesProcessed?: number,
): BenchmarkResult {
  // Warmup (10% of iterations, min 3)
  const warmupCount = Math.max(3, Math.floor(iterations * 0.1));
  for (let i = 0; i < warmupCount; i++) {
    fn();
  }

  // Actual benchmark
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    fn();
  }
  const end = performance.now();

  const totalMs = end - start;
  const avgMs = totalMs / iterations;
  const opsPerSec = 1000 / avgMs;

  const result: BenchmarkResult = {
    name,
    iterations,
    totalMs,
    avgMs,
    opsPerSec,
  };

  if (bytesProcessed !== undefined) {
    result.bytesProcessed = bytesProcessed;
    result.throughputMBps = bytesProcessed / 1024 / 1024 / (avgMs / 1000);
  }

  return result;
}

/**
 * Format a number for display
 */
export function formatNumber(n: number): string {
  if (n >= 1000000) return `${(n / 1000000).toFixed(2)}M`;
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  if (n >= 1) return n.toFixed(2);
  if (n >= 0.01) return n.toFixed(3);
  return n.toFixed(4);
}

/**
 * Format bytes for display
 */
export function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${bytes} B`;
}

/**
 * Pause to allow UI updates
 */
export function pause(ms: number = 50): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
