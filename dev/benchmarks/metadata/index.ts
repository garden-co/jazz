import { todoBenchmarks } from "../../../examples/todo-client-localfirst-ts/benchmarks/metadata.ts";
import { permissionedBenchmarks } from "../../../examples/permissioned-resources/benchmarks/metadata.ts";
import { bigLabelBenchmarks } from "../../../examples/big-label/benchmarks/metadata.ts";
import { w1Benchmarks } from "../../../examples/benchmarks/w1/metadata.ts";
import { coreBenchmarks } from "../../../crates/jazz/benches/metadata.ts";
import { policyDocumentBenchmarks } from "../../../examples/policy-scoped-documents/benchmarks/metadata.ts";
export { metadataRevision, throughput, type BenchmarkMetadata } from "./types.ts";

export const benchmarkMetadata = [
  ...todoBenchmarks,
  ...permissionedBenchmarks,
  ...bigLabelBenchmarks,
  ...w1Benchmarks,
  ...coreBenchmarks,
  ...policyDocumentBenchmarks,
];
const byName = new Map(benchmarkMetadata.map((metadata) => [metadata.name, metadata]));
if (byName.size !== benchmarkMetadata.length) throw new Error("Duplicate benchmark metadata name");

/** Exact names only. Unknown cases get no invented description or work count. */
export function getBenchmarkMetadata(name: string) {
  return byName.get(name) ?? null;
}
