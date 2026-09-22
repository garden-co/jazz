import { throughput, type BenchmarkMetadata } from "../../../dev/benchmarks/metadata/index.ts";
import { formatTime } from "./model.ts";
export { getBenchmarkMetadata, metadataRevision } from "../../../dev/benchmarks/metadata/index.ts";

/** User-selected rough estimate, deliberately NOT a hardware calibration. */
export const ESTIMATE_DIVISOR = 5;
export function estimatedSeconds(seconds: number): number {
  return seconds / ESTIMATE_DIVISOR;
}
export function displayedTime(seconds: number, estimated: boolean): string {
  return `${formatTime(estimated ? estimatedSeconds(seconds) : seconds)}${estimated ? "*" : ""}`;
}
export function formatThroughput(
  seconds: number,
  metadata: BenchmarkMetadata,
  estimated = false,
): string {
  const rate = throughput(estimated ? estimatedSeconds(seconds) : seconds, metadata.work);
  return rate === null
    ? "—"
    : `${rate.toLocaleString("en-US", { maximumFractionDigits: 1 })} ${metadata.work.unit}${estimated ? "*" : ""}`;
}
