import {
  getBenchmarkMetadata,
  throughput,
  type BenchmarkMetadata,
} from "../../../dev/benchmarks/metadata/index.ts";
import { formatTime } from "./model.ts";
export { getBenchmarkMetadata, metadataRevision } from "../../../dev/benchmarks/metadata/index.ts";

/**
 * CodSpeed's de-noised runner is about this many times slower than a normal
 * CPU; displayed times are divided by it.
 */
export const ESTIMATE_DIVISOR = 5;
export function estimatedSeconds(seconds: number): number {
  return seconds / ESTIMATE_DIVISOR;
}
/** The native runner estimate has not been calibrated for browser user flows. */
export function usesEstimatedTime(name: string): boolean {
  return getBenchmarkMetadata(name)?.harness !== "vitest";
}
export function benchmarkSeconds(name: string, seconds: number): number {
  return usesEstimatedTime(name) ? estimatedSeconds(seconds) : seconds;
}
export function displayedTime(seconds: number, estimated: boolean): string {
  return formatTime(estimated ? estimatedSeconds(seconds) : seconds);
}
export function formatThroughput(
  seconds: number,
  metadata: BenchmarkMetadata,
  estimated = false,
): string {
  const rate = throughput(estimated ? estimatedSeconds(seconds) : seconds, metadata.work);
  return rate === null
    ? "—"
    : `${rate.toLocaleString("en-US", { maximumFractionDigits: 1 })} ${metadata.work.unit}`;
}
