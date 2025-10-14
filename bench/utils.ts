/**
 * Utility functions for benchmark visualization
 */

/**
 * Format time in a human-readable way (µs, ms, or s)
 */
export function formatTime(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(2)}µs`;
  if (ms < 1000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

/**
 * Display benchmark results in a formatted table with percentiles
 * @param bench - Tinybench instance with completed results
 * @param includeP999 - Whether to include p99.9 percentile (default: false)
 */
export function displayBenchmarkResults(
  bench: any,
  includeP999: boolean = false,
) {
  const tableData = bench.tasks.map((task: any) => {
    const r = task.result;
    const data: Record<string, string> = {
      Task: task.name,
      Mean: formatTime(r?.mean || 0),
      p75: formatTime(r?.p75 || 0),
      p99: formatTime(r?.p99 || 0),
      "p99.5": formatTime(r?.p995 || 0),
    };

    if (includeP999) {
      data["p99.9"] = formatTime(r?.p999 || 0);
    }

    data["ops/sec"] = Math.round(r?.hz || 0).toLocaleString();
    return data;
  });

  console.table(tableData);
}
