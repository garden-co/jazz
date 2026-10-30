import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/record-player/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database, local durability";
const fixture =
  "4,096 tracks on 512 albums (8 tracks each; the first album's tracks carry 64 KiB audio bytes) and one playlist holding every track.";

export const recordPlayerBenchmarks: BenchmarkMetadata[] = [
  {
    name: "record_player_open_coverflow[4096]",
    title: "RecordPlayer · open the CoverFlow library",
    description:
      "Subscribe to the title-ordered 20-album CoverFlow shelf and the focused album's metadata-only track list, and receive both first results. Audio bytes are not materialized.",
    fixture,
    storage,
    includes: [
      "Opening two live subscriptions",
      "Materializing 20 albums and 8 tracks",
      "Dropping the subscriptions",
    ],
    excludes: ["Schema compilation, database opening and seeding", "Cover art and audio"],
    work: {
      count: 1,
      unit: "library opens/s",
      explanation: "One CoverFlow shelf plus track list per iteration.",
    },
    source,
  },
  {
    name: "record_player_open_playlist[4096]",
    title: "RecordPlayer · open a 4,096-track playlist",
    description:
      "Subscribe to the visible 16-entry window (offset 8) of a playlist ordered by position, and receive its first result.",
    fixture,
    storage,
    includes: [
      "Opening one live windowed subscription",
      "Materializing 16 entries",
      "Dropping the subscription",
    ],
    excludes: ["Schema compilation, database opening and seeding", "Audio"],
    work: { count: 1, unit: "playlist opens/s", explanation: "One playlist window per iteration." },
    source,
  },
  {
    name: "record_player_add_to_playlist[4096]",
    title: "RecordPlayer · add a track to a live playlist",
    description:
      "Insert a playlist entry at a fractional position inside the visible window of a subscribed 4,096-entry playlist, and wait until the window delivers it.",
    fixture: `${fixture} Each iteration adds one more entry.`,
    storage,
    includes: [
      "Entry insert until local durability",
      "Incremental update of the live window until its delta arrives",
    ],
    excludes: ["Opening the live subscription", "Sync to other listeners"],
    work: { count: 1, unit: "tracks added/s", explanation: "One added entry per iteration." },
    source,
  },
  {
    name: "record_player_scrub_track_64mb",
    title: "RecordPlayer · scrub a track",
    description:
      "Read a 64 KiB window from the middle of a track's 64 MiB audio, as the player does when the listener drags the playhead. Today the cost grows with the whole value's size rather than the window's (#3471), so this number should fall once that is fixed.",
    fixture:
      "One album holding one track whose 64 MiB audio was streamed in from a deterministic 32 KiB-per-read source.",
    storage: "In-memory Jazz database (test storage)",
    includes: [
      "Resolving the audio's large-value reference",
      "Reading the requested 64 KiB window",
    ],
    excludes: ["Uploading the audio", "Decoding the media"],
    work: { count: 1, unit: "seeks/s", explanation: "One 64 KiB range read per scrub." },
    source,
  },
];
