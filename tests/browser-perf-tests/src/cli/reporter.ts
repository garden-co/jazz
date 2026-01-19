import type { BenchmarkResults, BenchmarkStats } from "./scenarios/types.ts";

/**
 * Format a number with appropriate precision
 */
function formatNumber(value: number, decimals: number = 1): string {
  return value.toFixed(decimals);
}

/**
 * Format a duration in milliseconds
 */
function formatMs(ms: number): string {
  return `${formatNumber(ms, 1)}ms`;
}

/**
 * Format percentage
 */
function formatPercent(value: number): string {
  return `±${formatNumber(value, 1)}%`;
}

/**
 * Create a horizontal line for the table
 */
function horizontalLine(width: number, char: string = "─"): string {
  return char.repeat(width);
}

/**
 * Print a progress bar to stdout
 */
export function printProgress(current: number, total: number): void {
  const width = 50;
  const progress = Math.floor((current / total) * width);
  const bar = "=".repeat(progress) + "-".repeat(width - progress);
  process.stdout.write(`\r[${bar}] ${current}/${total}`);
  if (current === total) {
    process.stdout.write("\n");
  }
}

/**
 * Print that a fixture was generated
 */
export function printGeneratedFixture(
  scenario: string,
  coValueId: string,
): void {
  console.log(`\nGenerated ${scenario}: ${coValueId}`);
  console.log(`(reuse with: pnpm bench ${scenario} --id ${coValueId})\n`);
}

/**
 * Print benchmark results in a formatted table
 */
export function printResults(results: BenchmarkResults): void {
  const boxWidth = 67;
  const innerWidth = boxWidth - 4; // Account for "│  " and "  │"

  const topBorder = `┌${horizontalLine(boxWidth - 2)}┐`;
  const midBorder = `├${horizontalLine(boxWidth - 2)}┤`;
  const bottomBorder = `└${horizontalLine(boxWidth - 2)}┘`;

  const padLine = (content: string): string => {
    const padding = innerWidth - content.length;
    return `│  ${content}${" ".repeat(Math.max(0, padding))}  │`;
  };

  console.log("");
  console.log(topBorder);
  console.log(
    padLine(
      `${capitalize(results.scenario)} Benchmark Results (${results.runs} runs)`,
    ),
  );
  console.log(midBorder);
  console.log(padLine(`CoValue ID: ${results.coValueId}`));

  // Print stats for each metric
  for (const [metricName, stats] of Object.entries(results.stats)) {
    console.log(padLine(`Metric: ${metricName}`));
    console.log(padLine(""));
    printMetricStats(stats, padLine);
  }

  console.log(bottomBorder);
  console.log("");
}

/**
 * Print statistics for a single metric
 */
function printMetricStats(
  stats: BenchmarkStats,
  padLine: (content: string) => string,
): void {
  // Mean
  console.log(padLine(`Mean:     ${formatMs(stats.mean)}`));

  // 95% Confidence Interval
  const ciStr = `${formatMs(stats.ciLower)} - ${formatMs(stats.ciUpper)} (${formatPercent(stats.marginOfErrorPercent)})`;
  console.log(padLine(`95% CI:   ${ciStr}`));

  // Median
  console.log(padLine(`Median:   ${formatMs(stats.median)}`));

  // p95
  console.log(padLine(`p95:      ${formatMs(stats.p95)}`));

  // Min/Max
  console.log(
    padLine(`Min/Max:  ${formatMs(stats.min)} - ${formatMs(stats.max)}`),
  );
}

/**
 * Capitalize first letter of a string
 */
function capitalize(str: string): string {
  return str.charAt(0).toUpperCase() + str.slice(1);
}

/**
 * Print an error message
 */
export function printError(message: string): void {
  console.error(`\nError: ${message}\n`);
}

/**
 * Print warmup run info
 */
export function printWarmupStart(): void {
  console.log("\nPerforming warmup run (not included in results)...");
}

/**
 * Print warmup complete
 */
export function printWarmupComplete(timeMs: number): void {
  console.log(`Warmup complete (${timeMs.toFixed(0)}ms)\n`);
}

/**
 * Print usage information
 */
export function printUsage(): void {
  console.log(`
Usage: pnpm bench <scenario> [options]

Scenarios:
  grid    Run the pixel grid load benchmark

Options:
  --runs, -n <n>       Number of benchmark runs (default: 50)
  --id <covalue-id>    Use existing CoValue ID (skips fixture generation)
  --sync, -s <url>     Sync server URL (default: ws://localhost:4200)
  --headful            Run browser with visible window (default: headless)
  --cold-storage       Clear browser storage between runs (default: false)

Grid scenario options (when not using --id):
  --size <n>           Grid size NxN (default: 10)
  --min-padding <n>    Minimum padding bytes (default: 0)
  --max-padding <n>    Maximum padding bytes (default: 100)

Examples:
  pnpm bench grid --size 20 --runs 50
  pnpm bench grid --id co_z1234567890abcdef --runs 100
  pnpm bench grid --id co_z1234567890abcdef --cold-storage --runs 50
`);
}
