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
];
