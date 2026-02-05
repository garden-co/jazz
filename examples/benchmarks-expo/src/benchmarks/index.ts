export * from "./types";
export * from "./runner";
export { base64Benchmark } from "./base64";

// ============================================================================
// Benchmark Registry
// ============================================================================
// Add new benchmarks here to make them available in the app

import { BenchmarkSuite } from "./types";
import { base64Benchmark } from "./base64";

export const benchmarks: BenchmarkSuite[] = [
  base64Benchmark,
  // Add more benchmarks here:
  // cryptoBenchmark,
  // hashBenchmark,
  // etc.
];
