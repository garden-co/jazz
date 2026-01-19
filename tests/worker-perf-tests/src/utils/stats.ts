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

export type PercentileStats = {
  min: number;
  max: number;
  median: number;
  p50: number;
  p75: number;
  p90: number;
  p95: number;
  p99: number;
  mean: number;
  count: number;
};

/**
 * Calculate min, median, and max from an array of numbers.
 */
export function minMedianMax(values: number[]): {
  min: number;
  median: number;
  max: number;
} {
  if (values.length === 0) {
    return { min: 0, median: 0, max: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  return {
    min: sorted[0]!,
    median: median(sorted),
    max: sorted[sorted.length - 1]!,
  };
}

/**
 * Calculate comprehensive percentile statistics from an array of numbers.
 */
export function calculateStats(values: number[]): PercentileStats {
  if (values.length === 0) {
    return {
      min: 0,
      max: 0,
      median: 0,
      p50: 0,
      p75: 0,
      p90: 0,
      p95: 0,
      p99: 0,
      mean: 0,
      count: 0,
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);

  return {
    min: sorted[0]!,
    max: sorted[sorted.length - 1]!,
    median: median(sorted),
    p50: percentile(sorted, 50),
    p75: percentile(sorted, 75),
    p90: percentile(sorted, 90),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    mean: sum / sorted.length,
    count: sorted.length,
  };
}
