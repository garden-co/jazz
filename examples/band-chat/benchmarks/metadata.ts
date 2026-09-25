import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/band-chat/benchmarks/benches/walltime.rs";
const fixture = (messages: number) =>
  `${messages.toLocaleString("en-US")} messages from 32 members across ${(messages / 16).toLocaleString("en-US")} rooms (one membership each); one busy room holds 100 of them.`;
const reads = {
  storage: "In-memory Jazz database",
  excludes: [
    "Schema compilation, database opening and seeding",
    "Query preparation",
    "Sync, network latency and UI rendering",
  ],
  source,
};

export const bandChatBenchmarks: BenchmarkMetadata[] = [];
for (const messages of [1024, 4096]) {
  bandChatBenchmarks.push(
    {
      ...reads,
      name: `band_chat_timeline_second_page[${messages}]`,
      title: "BandChat · scroll back in a room",
      description:
        "Load the second page of a busy room's timeline: 25 messages, newest first, after skipping the latest 25.",
      fixture: fixture(messages),
      includes: ["One prepared read: room filter, sent-at order, offset 25, limit 25"],
      work: {
        count: 1,
        unit: "pages/s",
        explanation: "One 25-message page per iteration; the room size is load context.",
      },
    },
    {
      ...reads,
      name: `band_chat_unread_recent_rooms[${messages}]`,
      title: "BandChat · unread rooms",
      description: "List one member's rooms that have unread messages, most recently active first.",
      fixture: fixture(messages),
      includes: ["One prepared read: member and unread filters, last-activity order"],
      work: {
        count: 1,
        unit: "room lists/s",
        explanation: "One unread-room list per iteration, however many rooms it returns.",
      },
    },
    {
      ...reads,
      name: `band_chat_author_history[${messages}]`,
      title: "BandChat · a member's messages",
      description: "Show every message one member has written, newest first.",
      fixture: fixture(messages),
      includes: [`One prepared read returning ${messages / 32} messages, sent-at order`],
      work: {
        count: 1,
        unit: "histories/s",
        explanation: `One full author history (${messages / 32} messages) per iteration.`,
      },
    },
  );
}
for (const messages of [100, 1000, 10000]) {
  bandChatBenchmarks.push({
    name: `band_chat_caught_up_fast_resume[${messages}]`,
    title: "BandChat · reconnect when already up to date",
    description:
      "A peer that has already seen every message reconnects to the message history. The server confirms it is current without resending any message bodies.",
    fixture: `${messages.toLocaleString("en-US")} settled messages on the server; the peer's resume cursor comes from a real settled publication.`,
    storage: "In-memory Jazz node (the server side)",
    includes: [
      "Attaching a relay peer with a fast known-state declaration",
      "Query rehydration and the input-manifest reset it answers with",
    ],
    excludes: [
      "Writing, ingesting and settling the messages",
      "The first (warm) peer's attach",
      "Serialization and network latency",
    ],
    work: {
      count: 1,
      unit: "resumes/s",
      explanation: "One caught-up reconnect per iteration; the history length is load context.",
    },
    source,
  });
}
