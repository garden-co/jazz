import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const bigLabelBenchmarks: BenchmarkMetadata[] = [100000].map((count) => ({
  name: "ingest_walltime_100k",
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
for (const [kind, noun, share] of [["label", "label", 8]] as const) {
  for (const releases of [4096]) {
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
for (const batch of [1, 100, 1000]) {
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
bigLabelBenchmarks.push({
  name: "big_label_releases_live_view_100k",
  title: "BigLabel · open a label's live release view",
  description:
    "Open one maintained subscription on a label's newest 50 published releases and consume its initial result, from a persisted table holding every tenant's releases. Hydration must follow the label index: logical read counters are asserted outside timing.",
  fixture:
    "100,000 releases in one table: 100 belong to the viewed label, the rest to other tenants. Reopened from RocksDB after seeding; LIMIT 50.",
  storage: "RocksDB, reopened after seeding",
  includes: [
    "Subscription creation and initial result consumption",
    "Row digest and logical read-counter collection",
  ],
  excludes: [
    "Seeding, reopening and query preparation",
    "Validation pass and deferred subscription retirement",
  ],
  work: {
    count: 1,
    unit: "hydrations/s",
    explanation:
      "One initial live-view hydration per iteration. The 100k table size is NOT the processed-row count.",
  },
  source: loads,
});
for (const desks of [4, 6]) {
  const branches = 2 ** desks;
  bigLabelBenchmarks.push({
    name: `big_label_sign_off_first_edit[${desks}]`,
    title: `BigLabel · first release-plan edit under a ${branches}-branch sign-off policy`,
    description: `Compile and hydrate the update authorization-support view a fresh authority needs for a session's first release-plan edit. The update policy requires a lead or a deputy grant on each of ${desks} sign-off desks (correlated exists checks), so it normalizes to ${branches} branches of ${desks} joins each.`,
    fixture: `One release-plan table and ${desks * 2} empty grant tables; a fresh in-memory node per sample.`,
    storage: "In-memory Jazz node",
    includes: [
      "Update authorization-support scope compilation",
      "Hydration of every support subscription",
    ],
    excludes: ["Schema compilation and node opening", "Grant rows (tables are empty)"],
    work: {
      count: 1,
      unit: "first edits/s",
      explanation: `One support scope (${branches} policy branches) compiled and hydrated per iteration.`,
    },
    source: loads,
  });
}
