import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/music-agent/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database (test storage)";
const conversation =
  "One 200-turn conversation: 198 short alternating user and assistant turns, then a 128 KiB assistant reply stored as a large value, with a 256 KiB audio attachment.";

export const musicAgentBenchmarks: BenchmarkMetadata[] = [
  {
    name: "music_agent_stream_reply_1000_chunks",
    title: "MusicAgent streamed reply · 1,000 chunks",
    description:
      "Stream an assistant reply as 1,000 appends of 24 bytes each, one per arriving chunk, onto a turn that is already a large value, then wait until the last is locally durable.",
    fixture:
      "A prompt and a 128 KiB streamed assistant turn; a fresh conversation per measured run.",
    storage,
    includes: [
      "1,000 append updates to the reply",
      "Waiting for the last append's local durability",
    ],
    excludes: [
      "Schema compilation and database opening",
      "Seeding the conversation",
      "Model inference",
    ],
    work: { count: 1000, unit: "chunks/s", explanation: "1,000 appended chunks per reply." },
    source,
  },
  {
    name: "music_agent_open_transcript_200_turns",
    title: "MusicAgent open conversation · 200 turns",
    description:
      "Read a conversation's turns in order and materialize every body, including the long streamed reply.",
    fixture: conversation,
    storage,
    includes: ["Prepared indexed transcript query", "Materializing 200 turn bodies"],
    excludes: ["Seeding", "Query preparation", "Rendering"],
    work: { count: 1, unit: "conversations/s", explanation: "One full 200-turn transcript." },
    source,
  },
  {
    name: "music_agent_reopen_transcript_200_turns",
    title: "MusicAgent reopen after restart · 200 turns",
    description:
      "Reopen the database over its existing storage, as after an app restart, and read the same 200-turn conversation.",
    fixture: conversation,
    storage,
    includes: [
      "Schema compilation and database opening",
      "Transcript query preparation",
      "Materializing 200 turn bodies",
    ],
    excludes: ["Seeding", "Process start", "Rendering"],
    work: { count: 1, unit: "reopens/s", explanation: "One reopen plus one full transcript read." },
    source,
  },
  {
    name: "music_agent_attachment_seek_8mb",
    title: "MusicAgent attachment seek · 8 MiB audio",
    description:
      "Read a 64 KiB window from the middle of an 8 MiB audio attachment, as a player seek does.",
    fixture: "A prompt, a streamed assistant reply and an 8 MiB audio attachment on that reply.",
    storage,
    includes: [
      "Resolving the attachment's large-value reference",
      "Reading only the intersecting chunks",
    ],
    excludes: ["Uploading the attachment", "Decoding audio"],
    work: { count: 1, unit: "seeks/s", explanation: "One 64 KiB range read per seek." },
    source,
  },
];
