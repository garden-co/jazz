import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const cases = [
  [
    "policy_free_owner_page50",
    "Unrestricted owner page",
    "literal owner equality; explicit allow-all document SELECT policy",
  ],
  ["owner_policy_page50", "Permissioned owner page", "literal owner equality; owner policy"],
  [
    "owner_or_org_policy_owner_page50",
    "Shared-policy owner page",
    "literal owner equality; owner OR inherited organization policy",
  ],
  [
    "policy_free_org_page50",
    "Unrestricted organization page",
    "literal organization equality; explicit allow-all document SELECT policy",
  ],
  [
    "owner_or_org_policy_org_page50",
    "Permissioned organization page",
    "literal organization equality; owner OR inherited organization policy",
  ],
] as const;

export const policyDocumentBenchmarks: BenchmarkMetadata[] = cases.flatMap(
  ([name, title, predicate]) =>
    [10000, 100000].flatMap((rows) =>
      [false, true].map((subscription) => ({
        name: `${subscription ? "subscribe_" : ""}${name}[${rows}]`,
        title: `${subscription ? "Subscription: " : ""}${title}`,
        description: `First descending-timestamp page of 50 documents: ${predicate}. Admitted non-SYSTEM identity, ${subscription ? "Local subscription first result" : "Global one-shot"}, no network.`,
        fixture: `Revision 2 (explicit unrestricted SELECT control): ${rows.toLocaleString("en-US")} documents, 100 owners, 25 organizations; ${rows / 100} documents per owner. Separate single-column indexes, not a compound ordering.`,
        storage: "RocksDB WalNoSync; fresh runtime over seeded store, OS cache not flushed",
        includes: [
          subscription
            ? "subscribe_for_identity opening, runtime progress and first published result"
            : "First all_for_identity execution and result construction",
        ],
        excludes: [
          "Seed, database reopen, public query preparation, teardown, counter extraction",
          subscription
            ? "Network, subsequent updates and subscription finalization"
            : "Network and subscription delivery",
        ],
        work: {
          count: 1,
          unit: subscription ? "subscriptions/s" : "queries/s",
          explanation: subscription
            ? "One subscription opened through its first published page; not update throughput."
            : "One complete page query per iteration; not scanned-row throughput.",
        },
        source: "examples/policy-scoped-documents/benchmarks/benches/walltime.rs",
      })),
    ),
);
