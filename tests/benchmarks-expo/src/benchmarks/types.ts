/**
 * Result of a single benchmark run
 */
export interface BenchmarkResult {
  name: string;
  iterations: number;
  totalMs: number;
  avgMs: number;
  opsPerSec: number;
  bytesProcessed?: number;
  throughputMBps?: number;
}

/**
 * Comparison between two implementations
 */
export interface ComparisonResult {
  size: string;
  baseline: BenchmarkResult;
  optimized: BenchmarkResult;
  speedup: number;
}

/**
 * Configuration for a benchmark suite
 */
export interface BenchmarkConfig {
  /** Name of the benchmark suite */
  name: string;
  /** Description of what is being tested */
  description: string;
  /** Number of iterations per test */
  iterations: number;
  /** Data sizes to test */
  sizes: { label: string; bytes: number }[];
}

/**
 * A benchmark suite that can be run
 */
export interface BenchmarkSuite {
  config: BenchmarkConfig;
  run: (
    onProgress: (message: string) => void,
    onResult: (result: ComparisonResult) => void,
  ) => Promise<void>;
}
