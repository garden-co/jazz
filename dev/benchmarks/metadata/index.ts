import { stagePlanBenchmarks } from "../../../examples/stage-plan/benchmarks/metadata.ts";
import { bandChatBenchmarks } from "../../../examples/band-chat/benchmarks/metadata.ts";
import { bandBookBenchmarks } from "../../../examples/band-book/benchmarks/metadata.ts";
import { worldTourBenchmarks } from "../../../examples/world-tour/benchmarks/metadata.ts";
import { wequencerBenchmarks } from "../../../examples/wequencer/benchmarks/metadata.ts";
import { posterShopBenchmarks } from "../../../examples/poster-shop/benchmarks/metadata.ts";
import { recordPlayerBenchmarks } from "../../../examples/record-player/benchmarks/metadata.ts";
import { epicDropBenchmarks } from "../../../examples/epic-drop/benchmarks/metadata.ts";
import { jamazonBenchmarks } from "../../../examples/jamazon-warehouse/benchmarks/metadata.ts";
import { musicAgentBenchmarks } from "../../../examples/music-agent/benchmarks/metadata.ts";
import { bigLabelBenchmarks } from "../../../examples/big-label/benchmarks/metadata.ts";
import { permissionedBenchmarks } from "../../../examples/permissioned-resources/benchmarks/metadata.ts";
export { metadataRevision, throughput, type BenchmarkMetadata } from "./types.ts";
export { formerBenchmarkNames } from "./former-names.ts";

// Hero examples in the docs page's order, then the anonymized adopter
// workload. Engine benchmarks (crates/groove) carry no per-case metadata.
export const benchmarkMetadata = [
  ...stagePlanBenchmarks,
  ...bandChatBenchmarks,
  ...bandBookBenchmarks,
  ...worldTourBenchmarks,
  ...wequencerBenchmarks,
  ...posterShopBenchmarks,
  ...recordPlayerBenchmarks,
  ...epicDropBenchmarks,
  ...jamazonBenchmarks,
  ...musicAgentBenchmarks,
  ...bigLabelBenchmarks,
  ...permissionedBenchmarks,
];
const byName = new Map(benchmarkMetadata.map((metadata) => [metadata.name, metadata]));
if (byName.size !== benchmarkMetadata.length) throw new Error("Duplicate benchmark metadata name");

/** Exact names only. Unknown cases get no invented description or work count. */
export function getBenchmarkMetadata(name: string) {
  return byName.get(name) ?? null;
}
