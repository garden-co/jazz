import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

export const coreBenchmarks: BenchmarkMetadata[] = [
  ...[10000, 100000].map((count) => ({
    name: `maintained_subscription_hydration_${count === 10000 ? "10k" : "100k"}`,
    title: `Selective subscription hydration · ${count.toLocaleString("en-US")} table rows`,
    description:
      "Create one maintained subscription and consume its initial result from a selectively queried RocksDB table.",
    fixture: `${count.toLocaleString("en-US")} total table rows; 100 matching candidates and LIMIT 50.`,
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
        "One initial subscription hydration per iteration. The 10k/100k table size is NOT the processed-row count.",
    },
    source: "crates/jazz/benches/selective_global_hydration.rs",
  })),
  ...[4, 6].map((factors) => ({
    name: `update_support_branches[${factors}]`,
    title: `Update authorization support · ${2 ** factors} policy branches`,
    description:
      "Compile and hydrate the authorization-support view an authority opens for a session's first update of a table whose update policy has many branches.",
    fixture: `Update policy is an AND of ${factors} ORs of two correlated exists grants: ${2 ** factors} branches of ${factors} joins each. Empty tables.`,
    storage: "In-memory Jazz node",
    includes: [
      "Support-scope policy compilation for the update's using and check clauses",
      "Query-program lowering, Groove graph compilation and initial hydration of both support subscriptions",
    ],
    excludes: ["Schema compilation and node opening"],
    work: {
      count: 1,
      unit: "support views/s",
      explanation:
        "One update support scope per iteration on a fresh node, so no compiled program is reused. Compare the 16- and 64-branch rungs for branch scaling.",
    },
    source: "crates/jazz/benches/authorization_support_branches.rs",
  })),
  {
    name: "attach_route_bindings[100]",
    title: "Attach 100 route subscriptions",
    description:
      "Prepare and attach 100 distinct parameter bindings of the same query shape, consuming their initial subscription results.",
    fixture:
      "100 routes; 1,001 teams and 2,000 documents. One hot team has 1,000 documents; page size 100.",
    storage: "In-memory Jazz database",
    includes: [
      "Per-route query preparation, binding, subscription and initial hydration",
      "Initial-result validation and runtime/retained-state receipt collection",
    ],
    excludes: ["Fixture seeding and teardown"],
    work: {
      count: 100,
      unit: "subscriptions attached/s",
      explanation: "100 route subscriptions attached per timed iteration.",
    },
    source: "crates/jazz/benches/route_subscription_curve.rs",
  },
  {
    name: "matching_write_fanout[100]",
    title: "Matching write with 100 live routes",
    description:
      "Insert one document into the hot team with 100 route subscriptions already hydrated; process its maintained update.",
    fixture: "100 live route bindings. The write affects the hot route, not every route.",
    storage: "In-memory Jazz database",
    includes: [
      "One matching document write and resulting maintained work",
      "Event draining and delta assertions",
    ],
    excludes: ["Fixture seeding, initial route attachment/hydration and teardown"],
    work: {
      count: 1,
      unit: "matching writes/s",
      explanation:
        "One write per iteration. The 100 bindings are load context, NOT 100 writes or 100 delivered deltas.",
    },
    source: "crates/jazz/benches/route_subscription_curve.rs",
  },
];
