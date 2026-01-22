import type { Page } from "@playwright/test";

/**
 * Result of a single benchmark run
 */
export interface ScenarioResult {
  /** Metrics collected from this run, e.g., { loadTimeMs: 1234 } */
  metrics: Record<string, number>;
}

/**
 * Definition of a benchmark scenario
 */
export interface ScenarioDefinition {
  /** Unique name of the scenario */
  name: string;

  /**
   * Generate a new fixture and return its CoValue ID.
   * Called once before benchmark runs if no --id is provided.
   */
  generate: (page: Page, config: Record<string, unknown>) => Promise<string>;

  /**
   * Run the benchmark on an existing CoValue ID.
   * Called for each benchmark iteration.
   */
  run: (page: Page, coValueId: string) => Promise<ScenarioResult>;
}

/**
 * Configuration for a benchmark session
 */
export interface BenchmarkConfig {
  /** Name of the scenario to run (e.g., "grid" or "todo") */
  scenario: string;

  /** Number of benchmark runs */
  runs: number;

  /** Optional: existing CoValue ID to reuse (skips generation) */
  id?: string;

  /** Optional: sync server URL */
  sync?: string;

  /** Optional: run in headless mode */
  headless?: boolean;

  /**
   * If true, create a fresh context for each run (cold load).
   * If false (default), reuse the same browser context across runs (warm storage).
   */
  coldStorage?: boolean;

  /** Scenario-specific options (only used if no id provided) */
  scenarioOptions: Record<string, unknown>;

  timeout?: number;
}

/**
 * Statistical results from benchmark analysis
 */
export interface BenchmarkStats {
  /** Number of samples */
  count: number;

  /** Arithmetic mean */
  mean: number;

  /** Standard deviation */
  stdDev: number;

  /** 95% confidence interval lower bound */
  ciLower: number;

  /** 95% confidence interval upper bound */
  ciUpper: number;

  /** Margin of error as percentage of mean */
  marginOfErrorPercent: number;

  /** Minimum value */
  min: number;

  /** Maximum value */
  max: number;

  /** Median (p50) */
  median: number;

  /** 75th percentile */
  p75: number;

  /** 90th percentile */
  p90: number;

  /** 95th percentile */
  p95: number;

  /** 99th percentile */
  p99: number;
}

/**
 * Complete benchmark results
 */
export interface BenchmarkResults {
  /** Scenario name */
  scenario: string;

  /** CoValue ID used for the benchmark */
  coValueId: string;

  /** Number of runs performed */
  runs: number;

  /** Raw metric values from each run */
  rawMetrics: Record<string, number[]>;

  /** Statistical analysis for each metric */
  stats: Record<string, BenchmarkStats>;
}
