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

const loads = "examples/big-label/benchmarks/benches/loads.rs";
for (const [kind, noun, share] of [
  ["label", "label", 8],
  ["artist", "artist", 32],
  ["catalog", "catalogue", 4],
] as const) {
  for (const releases of [512, 4096]) {
    bigLabelBenchmarks.push({
      name: `big_label_${kind}_load[${releases}]`,
      title: `BigLabel · open a ${noun}'s releases`,
      description: `Load every release of one ${noun}, newest first, as the ${noun} page does.`,
      fixture: `4 catalogues, 8 labels, 32 artists and ${releases.toLocaleString("en-US")} releases, one transaction each; the ${noun} owns ${releases / share}.`,
      storage: "In-memory Jazz database",
      includes: [`One prepared read returning ${releases / share} releases, release-date order`],
      excludes: ["Schema compilation, database opening and seeding", "Query preparation"],
      work: {
        count: 1,
        unit: "page loads/s",
        explanation: `One ${noun} page (${releases / share} releases) per iteration.`,
      },
      source: loads,
    });
  }
}
for (const batch of [1, 10, 100, 1000]) {
  bigLabelBenchmarks.push({
    name: `big_label_ingest_batch_amortization[${batch}]`,
    title: `BigLabel import · 1,000 releases in batches of ${batch.toLocaleString("en-US")}`,
    description:
      "Insert 1,000 release rows in transactions of the given size, to show how batching amortizes per-transaction work.",
    fixture: `1,000 new releases; ${(1000 / batch).toLocaleString("en-US")} transactions of ${batch.toLocaleString("en-US")}.`,
    storage: "In-memory Jazz database",
    includes: ["Release-row construction and insertion", "Batch transaction processing"],
    excludes: ["Schema compilation and database opening", "Label/artist dimension seeding"],
    work: {
      count: 1000,
      unit: "rows inserted/s",
      explanation: `1,000 release rows per import, in ${(1000 / batch).toLocaleString("en-US")} transactions.`,
    },
    source: loads,
  });
}
