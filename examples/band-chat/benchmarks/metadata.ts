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
for (const messages of [4096]) {
  bandChatBenchmarks.push(
    {
      ...reads,
      name: `band_chat_timeline_second_page[${messages}]`,
      title: "BandChat · scroll back in a room",
      description:
        "Load the second page of a busy room's timeline: 25 messages, newest first, after skipping the latest 25. Today the cost grows with the whole table, not just the page (#1962).",
      fixture: fixture(messages),
      includes: ["One prepared read: room filter, sent-at order, offset 25, limit 25"],
      work: {
        count: 1,
        unit: "pages/s",
        explanation:
          "One 25-message page per iteration. The room holds 100 messages at every scale, but the cost currently grows with the total message count (#1962).",
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
  );
}
for (const messages of [100, 10000]) {
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

const room = (messages: number) =>
  `32 members, 64 rooms (odd rooms public), 4 members per room, ${messages.toLocaleString("en-US")} messages of which a quarter are in the opened private room; settled before timing.`;
const roomStorage = "In-memory Jazz database; in-process authority, no network";
bandChatBenchmarks.push(
  {
    name: "band_chat_open_room[10000]",
    title: "BandChat · open a private room",
    description:
      "A member opens a private room: the newest 21 messages with their senders, newest first, through the membership read policy (the room is public or the reader is a member). A policy-protected ordered page currently loads the room's full visible history before trimming to 21 (#1733), so this grows with the room's size rather than the page's.",
    fixture: room(10000),
    storage: roomStorage,
    includes: ["subscribe_for_identity opening, runtime ticks and the first published page"],
    excludes: [
      "Schema compilation, seeding and query preparation",
      "Attachments, network and subscription teardown",
    ],
    work: {
      count: 1,
      unit: "rooms opened/s",
      explanation: "One room opening per iteration, returning a 21-message page.",
    },
    source,
  },
  {
    name: "band_chat_send_100[10000]",
    title: "BandChat · send messages",
    description:
      "A member sends 100 messages into the room they have open. Each is a standalone write that the in-process authority checks against the insert policy (member of the room, sending as their own profile) and accepts, and that then appears at the top of the open page.",
    fixture: `${room(10000)} The room is already open before timing.`,
    storage: roomStorage,
    includes: [
      "Message insert, authority insert-policy check and acceptance",
      "Runtime ticks until the open page shows each message",
    ],
    excludes: ["Room opening, seeding and network"],
    work: {
      count: 100,
      unit: "messages sent/s",
      explanation: "100 messages, each sent, accepted and shown before the next.",
    },
    source,
  },
  {
    name: "band_chat_new_message_rooms_open[100]",
    title: "BandChat · a new message while 100 rooms are open",
    description:
      "100 members each keep a different room open: one prepared query shape with 100 bindings, all hydrated. One new message lands in the busy room; its view gains the message and drops its oldest shown one, every other view stays quiet.",
    fixture:
      "1,001 rooms; the busy room holds 1,000 messages, every other room one. Views show the newest 100 messages of their room.",
    storage: "In-memory Jazz database",
    includes: [
      "One matching message write and the resulting maintained work",
      "Draining every open view and asserting the delta",
    ],
    excludes: ["Seeding, opening and hydrating the 100 views, and teardown"],
    work: {
      count: 1,
      unit: "messages delivered/s",
      explanation:
        "One write per iteration. The 100 open views are load context, NOT 100 writes or 100 delivered deltas.",
    },
    source,
  },
);
