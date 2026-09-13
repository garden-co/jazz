import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const bigLabelBenchmarks: BenchmarkMetadata[] = [10000, 100000].map((count) => ({
  name: `ingest_walltime_${count === 10000 ? "10k" : "100k"}`,
  title: `BigLabel import · ${count.toLocaleString("en-US")} releases`,
  description:
    "Construct and insert record-label release rows in transactions of 1,000 rows, referencing pre-seeded labels and artists.",
  fixture: `${count.toLocaleString("en-US")} new releases; ${count / 1000} batches of 1,000.`,
  storage: "In-memory Jazz database",
  includes: ["Release-row construction and insertion", "Batch transaction processing"],
  excludes: ["Schema compilation and database opening", "Label/artist dimension seeding"],
  work: {
    count,
    unit: "rows inserted/s",
    explanation: `${count.toLocaleString("en-US")} release rows per import, not ${count.toLocaleString("en-US")} transactions.`,
  },
  source: "examples/big-label/benchmarks/benches/ingest_walltime.rs",
}));
