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

const textSource = "examples/band-book/benchmarks/benches/walltime.rs";
const textNightlySource = "examples/band-book/benchmarks/benches/text_merge.rs";
const textFixture =
  'An authority and two devices in one process, connected over in-memory transports, all subscribed to a `blocks` table whose `text` column is `s.string().merge("markdown")`. A song paragraph is a 270-character lyric with Markdown emphasis; a long note is generated prose with Markdown runs and a line break every 40 words.';
const textStorage = "In-memory databases on every replica (no file I/O)";
const textIncludes = [
  "The device reconnecting, uploading its pending writes and each one settling at the authority (Global tier), merged where it missed a version",
  "Delivery of the result to both devices",
];
const textExcludes = [
  "Seeding the block, taking the devices offline and making the edits",
  "The other device's write, accepted before the timed reconnect",
  "Network: transports are in-memory queues",
];
const oneMerge = (explanation: string) => ({
  count: 1,
  unit: "merges/s",
  explanation,
});

const textMergeBenchmarks: BenchmarkMetadata[] = [
  {
    name: "band_book_text_fast_path",
    title: "BandBook · reconnect with an edit nobody else made",
    description:
      "A device edited a block while offline and nobody else changed it. It reconnects; the authority finds no version it missed and applies the write as is (the text merge fast path).",
    fixture: textFixture,
    storage: textStorage,
    includes: textIncludes,
    excludes: textExcludes,
    work: oneMerge("One write accepted without merging; the baseline for the merge cases."),
    source: textSource,
  },
  {
    name: "band_book_text_concurrent_paragraph",
    title: "BandBook · merge two offline edits to one paragraph",
    description:
      "Two devices edited the same song paragraph while offline, one at the start and one at the end. The first has reconnected and its write was accepted; the second reconnects and its write is merged with it, keeping both edits.",
    fixture: textFixture,
    storage: textStorage,
    includes: textIncludes,
    excludes: textExcludes,
    work: oneMerge("One three-way merge of a paragraph, plus accepting and delivering it."),
    source: textSource,
  },
  {
    name: "band_book_text_offline_chain[20]",
    title: "BandBook · reconnect with 20 offline writes to one paragraph",
    description:
      "A device typed into a paragraph offline, one write per pause (20 writes), while the other device edited the start of the same paragraph five times online. It reconnects; each of its 20 writes is merged over what it had seen.",
    fixture: textFixture,
    storage: textStorage,
    includes: textIncludes,
    excludes: textExcludes,
    work: {
      count: 20,
      unit: "merged writes/s",
      explanation: "Twenty chained writes, each merged on arrival.",
    },
    source: textNightlySource,
  },
  ...[100_000, 1_000_000].map((bytes) => ({
    name: `band_book_text_long_note[${bytes}]`,
    title: `BandBook · merge two offline edits to a ${bytes / 1_000} KB note`,
    description: `Two devices made 200 scattered word insertions each to a ${bytes.toLocaleString("en-US")}-byte tour note while offline, interleaved through the note. The first has reconnected; the second reconnects and its write is merged, keeping all 400 insertions.`,
    fixture: textFixture,
    storage: textStorage,
    includes: textIncludes,
    excludes: textExcludes,
    work: oneMerge("One three-way merge of a long value with 200 edits per side."),
    source: textNightlySource,
  })),
  {
    name: "band_book_text_over_budget[1000000]",
    title: "BandBook · a 1 MB merge past the work budget",
    description:
      "One device replaced a 1,000,000-byte note with different text while the other made 200 scattered insertions offline. Merging them would exceed the merge's work budget (100 million diff steps), so the authority stops and the later write's whole value wins: the worst case a merge can cost.",
    fixture: textFixture,
    storage: textStorage,
    includes: textIncludes,
    excludes: textExcludes,
    work: oneMerge("One merge attempt that runs out of budget, then the later write wins."),
    source: textNightlySource,
  },
];

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
  ...textMergeBenchmarks,
];
