// @ts-expect-error - jstat doesn't have type definitions
import jStat from "jstat";
import type { BenchmarkStats } from "./scenarios/types.ts";

/**
 * Calculate percentile from sorted array of numbers.
 * @param sorted - Array of numbers, already sorted in ascending order
 * @param p - Percentile to calculate (0-100)
 */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  if (sorted.length === 1) return sorted[0]!;

  const index = (p / 100) * (sorted.length - 1);
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  const weight = index - lower;

  if (lower === upper) return sorted[lower]!;
  return sorted[lower]! * (1 - weight) + sorted[upper]! * weight;
}

/**
 * Calculate median from sorted array of numbers.
 */
export function median(sorted: number[]): number {
  return percentile(sorted, 50);
}

/**
 * Calculate standard deviation of an array of numbers.
 */
export function standardDeviation(values: number[], mean: number): number {
  if (values.length <= 1) return 0;

  const squaredDiffs = values.map((v) => Math.pow(v - mean, 2));
  const variance =
    squaredDiffs.reduce((a, b) => a + b, 0) / (values.length - 1);
  return Math.sqrt(variance);
}

/**
 * Calculate 95% confidence interval using Student's t-distribution.
 * This is the same approach used by Google's Tachometer.
 */
export function confidenceInterval(
  values: number[],
  confidenceLevel: number = 0.95,
): { lower: number; upper: number; marginOfError: number } {
  if (values.length < 2) {
    const value = values[0] ?? 0;
    return { lower: value, upper: value, marginOfError: 0 };
  }

  const n = values.length;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const stdDev = standardDeviation(values, mean);
  const standardError = stdDev / Math.sqrt(n);

  // Get t-critical value for the given confidence level and degrees of freedom
  const alpha = 1 - confidenceLevel;
  const degreesOfFreedom = n - 1;

  // jStat.studentt.inv gives the inverse CDF (quantile function)
  // We want the two-tailed critical value
  const tCritical = jStat.studentt.inv(1 - alpha / 2, degreesOfFreedom);

  const marginOfError = tCritical * standardError;

  return {
    lower: mean - marginOfError,
    upper: mean + marginOfError,
    marginOfError,
  };
}

/**
 * Calculate comprehensive statistics for benchmark results.
 * Inspired by Google's Tachometer statistical analysis.
 */
export function calculateBenchmarkStats(values: number[]): BenchmarkStats {
  if (values.length === 0) {
    return {
      count: 0,
      mean: 0,
      stdDev: 0,
      ciLower: 0,
      ciUpper: 0,
      marginOfErrorPercent: 0,
      min: 0,
      max: 0,
      median: 0,
      p75: 0,
      p90: 0,
      p95: 0,
      p99: 0,
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  const mean = sum / sorted.length;
  const stdDev = standardDeviation(values, mean);
  const ci = confidenceInterval(values);

  const marginOfErrorPercent = mean !== 0 ? (ci.marginOfError / mean) * 100 : 0;

  return {
    count: values.length,
    mean,
    stdDev,
    ciLower: ci.lower,
    ciUpper: ci.upper,
    marginOfErrorPercent,
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
    median: median(sorted),
    p75: percentile(sorted, 75),
    p90: percentile(sorted, 90),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
  };
}
