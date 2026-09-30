import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/wequencer/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database, local durability";
const fixture = "One session with 16 tracks of 64 steps (1,024 pads).";

export const wequencerBenchmarks: BenchmarkMetadata[] = [
  {
    name: "wequencer_open_pattern",
    title: "Wequencer · open a 16-track pattern",
    description:
      "Subscribe to the session's ordered track list and to each track's ordered steps (17 subscriptions), and receive every first result.",
    fixture,
    storage,
    includes: [
      "Opening 17 live subscriptions",
      "Materializing 16 tracks and 1,024 steps",
      "Dropping the subscriptions",
    ],
    excludes: ["Schema compilation, database opening and seeding", "Audio scheduling"],
    work: { count: 1, unit: "patterns opened/s", explanation: "One full grid per iteration." },
    source,
  },
  {
    name: "wequencer_toggle_pad",
    title: "Wequencer · toggle a pad on a live grid",
    description:
      "Flip one step while all 16 track subscriptions are live, and wait until that track's subscription delivers the change. Successive iterations walk across tracks and steps.",
    fixture,
    storage,
    includes: [
      "Step update until local durability",
      "Incremental update of the owning track subscription until its delta arrives",
    ],
    excludes: ["Opening the live grid", "Sync to bandmates", "Audio scheduling"],
    work: { count: 1, unit: "pad toggles/s", explanation: "One toggled pad per iteration." },
    source,
  },
  {
    name: "wequencer_open_pattern_views[100]",
    title: "Wequencer · 100 bandmates open their pattern views",
    description:
      "Prepare and attach 100 distinct bindings of one pattern-view query shape (a pattern's 100 most recent pad edits), consuming each initial result.",
    fixture:
      "1,001 patterns and 2,000 pad edits; the busy pattern holds 1,000 edits, every other pattern one. Views show the newest 100.",
    storage: "In-memory Jazz database",
    includes: [
      "Per-view query preparation, binding, subscription and initial hydration",
      "Initial-result validation and runtime/retained-state receipt collection",
    ],
    excludes: ["Fixture seeding"],
    work: {
      count: 100,
      unit: "views opened/s",
      explanation: "100 pattern views attached per timed iteration.",
    },
    source,
  },
  ...[1000, 10000].map(
    (depth): BenchmarkMetadata => ({
      name: `wequencer_pad_edit_history[${depth}]`,
      title: "Wequencer · read a pad after a long offline edit chain",
      description:
        "Read the current value of one pad that was toggled over and over while offline: a chain of locally settled edits, each built on the last, all still retained.",
      fixture: `One pad edited ${depth.toLocaleString("en-US")} times; every candidate is retained.`,
      storage: "RocksDB; WAL without fsync",
      includes: ["One current-row read that scans the retained edit history"],
      excludes: [
        "Writing and settling the edit history",
        "The untimed receipt that checks the winner and read counts",
      ],
      work: {
        count: 1,
        unit: "reads/s",
        explanation: "One current-row read per iteration; the edit depth is load context.",
      },
      source,
    }),
  ),
];
