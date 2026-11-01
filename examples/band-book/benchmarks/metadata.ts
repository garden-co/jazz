import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/band-book/benchmarks/benches/walltime.rs";
const fixture =
  "100,000 pages, 100 members, 25 workspaces of four members (1,000 pages per member); the reader is admitted to workspace 0, whose newest pages belong to another member. Ordered (owner, updated) and (workspace, updated) indexes.";
const storage =
  "SQLite file, WAL with synchronous=NORMAL; fresh runtime over seeded store, OS cache not flushed";
const excludes = [
  "Seed, database reopen, public query preparation, teardown, counter extraction",
  "Network and subscription delivery",
];

const oneShot = (name: string, title: string, predicate: string): BenchmarkMetadata => ({
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
      explanation:
        "One subscription opened through its first published page; not update throughput.",
    },
    source,
  },
];

// Contention: another band opening its big workspace on the same server.
const contention = "examples/band-book/benchmarks/benches/contention.rs";
for (const [request, title, unit, timed] of [
  [
    "open_my_pages",
    "open my pages",
    "page lists/s",
    "One remote read of your newest 50 pages, from request to verified rows",
  ],
  [
    "check_off_page",
    "check off a page",
    "check-offs/s",
    "One page update, from the call until the server acknowledges it (GlobalServer)",
  ],
] as const) {
  for (const [arm, during] of [
    ["idle", "with the server otherwise idle"],
    ["during_workspace_open", "while another band's member opens their big workspace"],
  ] as const) {
    bandBookBenchmarks.push({
      name: `band_book_${request}_${arm}`,
      title: `BandBook contention · ${title}, ${arm === "idle" ? "idle" : "during a workspace open"}`,
      description: `A member of one band requests over a WebSocket to one durable server ${during}. Opening the workspace loads its sidebar: every page, oldest first, unbounded, under the owner OR workspace-member policy; two devices reopen it back to back. Compare with the other arm: the difference is how long one large read holds up another tenant's cheap request.`,
      fixture:
        "One durable SQLite server: band A's workspace has 5,000 pages, band B's 200, interleaved in one table.",
      storage: "Durable SQLite server over WebSockets; each client has its own thread",
      includes: [timed],
      excludes: [
        "Seeding, server start, client sign-in and warm-up",
        "The browsing clients' work, except as it delays the server",
      ],
      work: {
        count: 1,
        unit,
        explanation: "One request per iteration; workspace opens are not counted.",
      },
      source: contention,
    });
  }
}
