import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/band-book/benchmarks/benches/walltime.rs";
const fixture =
  "100,000 pages, 100 members, 25 workspaces of four members (1,000 pages per member); the reader is admitted to workspace 0, whose newest pages belong to another member. Ordered (owner, updated) and (workspace, updated) indexes.";
const storage = "RocksDB WalNoSync; fresh runtime over seeded store, OS cache not flushed";
const excludes = [
  "Seed, database reopen, public query preparation, teardown, counter extraction",
  "Network and subscription delivery",
];

const oneShot = (
  name: string,
  title: string,
  predicate: string,
): BenchmarkMetadata => ({
  name: `${name}[100000]`,
  title,
  description: `First page of 50 pages, newest first: ${predicate}. Admitted non-SYSTEM member, Global one-shot read, no network.`,
  fixture,
  storage,
  includes: ["First all_for_identity execution and result construction"],
  excludes,
  work: {
    count: 1,
    unit: "page lists/s",
    explanation: "One complete 50-page list per iteration; not scanned-row throughput.",
  },
  source,
});

export const bandBookBenchmarks: BenchmarkMetadata[] = [
  oneShot(
    "band_book_workspace_pages_unrestricted",
    "BandBook · workspace pages, allow-all policy",
    "workspace equality under an explicit allow-all SELECT policy (the baseline)",
  ),
  oneShot(
    "band_book_workspace_pages",
    "BandBook · workspace pages",
    "workspace equality under the own-pages OR admitted-workspace policy",
  ),
  oneShot(
    "band_book_my_pages",
    "BandBook · my pages",
    "owner equality under the owner-only policy",
  ),
  {
    name: "band_book_workspace_pages_live[100000]",
    title: "BandBook · open the workspace page list live",
    description:
      "Subscribe to the permissioned workspace page list (own pages OR admitted workspace) and receive its first published 50 pages. Local tier with immediate local updates.",
    fixture,
    storage,
    includes: ["subscribe_for_identity opening, runtime progress and first published result"],
    excludes: [
      "Seed, database reopen, public query preparation, teardown, counter extraction",
      "Network, subsequent updates and subscription finalization",
    ],
    work: {
      count: 1,
      unit: "subscriptions/s",
      explanation: "One subscription opened through its first published page; not update throughput.",
    },
    source,
  },
];
