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
  {
    name: "band_chat_post_announcements[10000]",
    title: "BandChat · post announcements to a full-history room",
    description:
      "The band's admin posts 10 announcements into the announcements room they have open. Reading the room needs a `member` or `admin` role claim and posting needs `admin`: session-claim-gated read and insert policies. The open view is the room's whole history in send order, with no LIMIT, and each post's update of that view currently scales with the history (https://github.com/garden-co/jazz/issues/2086), so the per-post cost at 10,000 messages is mostly that update.",
    fixture:
      "10,000 announcements and 10,000 general-room messages from 16 authors, settled; the admin's announcements view is open and hydrated before timing.",
    storage: roomStorage,
    includes: [
      "Announcement insert, authority admin-only insert-policy check and acceptance",
      "Runtime ticks until the open full-history view shows each announcement",
    ],
    excludes: ["Seeding, claim admission, opening the room, network and teardown"],
    work: {
      count: 10,
      unit: "announcements posted/s",
      explanation: "10 announcements, each posted, accepted and shown before the next.",
    },
    source,
  },
);

const nightlySource = "examples/band-chat/benchmarks/benches/nightly.rs";
const band =
  "200 members, 1,000 rooms: one room with 100,000 messages and 50 members, ten rooms of 10,000 and 989 rooms of 10–200 (303,695 messages). Every tenth message in the busy rooms has 1–3 reactions; every member has a read marker per room, and the deep and busy rooms carry a read journal (one entry per ~500 messages read). The reader is a member of 100 rooms. Membership read policies throughout; settled before timing.";
const bandStorage = "In-memory Jazz database; in-process authority, no network";
const bandExcludes = [
  "Schema compilation, seeding and query preparation",
  "Network, rendering and subscription teardown",
];
const deepRoomCase = (
  name: string,
  title: string,
  description: string,
  includes: string,
  unit: string,
  explanation: string,
  count = 1,
): BenchmarkMetadata => ({
  name,
  title,
  description,
  fixture: band,
  storage: bandStorage,
  includes: [includes],
  excludes: bandExcludes,
  work: { count, unit, explanation },
  source: nightlySource,
});
bandChatBenchmarks.push(
  deepRoomCase(
    "band_chat_open_deep_room[100000]",
    "BandChat · open a room with a long history",
    "A member opens the 100,000-message room: the newest 50 messages, each with its sender and its reactions, through the membership read policy, until the first published page. The page is 50 messages at any depth; the cost should not follow the room's size.",
    "subscribe_for_identity opening, runtime ticks and the first published page",
    "rooms opened/s",
    "One room opening per iteration, returning a 50-message page.",
  ),
  deepRoomCase(
    "band_chat_scroll_back_deep[100000]",
    "BandChat · scroll back deep into a room",
    "The 50 messages before a cursor halfway down the 100,000-message room (`$createdAt < cursor`, newest first), with senders and reactions. Cursor paging, not offset: the page should cost the same at any depth.",
    "Opening the page subscription until its first published result",
    "pages/s",
    "One 50-message page per iteration, 50,000 messages deep.",
  ),
  deepRoomCase(
    "band_chat_jump_to_message[100000]",
    "BandChat · jump to a message",
    "Jumping to a reply or search hit a quarter of the way into the 100,000-message room: the 25 messages up to it and the 25 after it, as two bounded windows with senders and reactions.",
    "Opening both windows until each publishes",
    "jumps/s",
    "One jump (two 25-message windows) per iteration.",
  ),
  deepRoomCase(
    "band_chat_search_room[100000]",
    "BandChat · search a room",
    "A one-shot read of the newest 50 messages in the 100,000-message room that contain a word, with senders. One message in 2,000 matches.",
    "One all_for_identity read",
    "searches/s",
    "One search per iteration, returning 50 hits.",
  ),
  deepRoomCase(
    "band_chat_live_window_new_message[100000]",
    "BandChat · a new message in an open room with a long history",
    "The reader has the 100,000-message room open (newest 50, senders and reactions). A bandmate's message passes the insert policy, is accepted and reaches the open page.",
    "Message insert, authority acceptance, runtime ticks until the open page shows it",
    "messages delivered/s",
    "One message per iteration.",
  ),
  deepRoomCase(
    "band_chat_inbox_open[100]",
    "BandChat · open the room list",
    "The reader's room list: every room they can read (100), each with its newest message (newest first, limit 1) and the reader's own marker. Unread state, order and preview all come from this one subscription; nobody writes to the room when they post.",
    "Opening the room-list subscription until its first published result",
    "room lists/s",
    "One room list of 100 rooms per iteration.",
  ),
  deepRoomCase(
    "band_chat_inbox_new_message[100]",
    "BandChat · a new message reaches the room list",
    "The room list is open. A bandmate's message lands in one of the reader's rooms and becomes that room's newest message in the list.",
    "Message insert, authority acceptance, runtime ticks until the room list changes",
    "messages delivered/s",
    "One message per iteration; the 100-room list is load context.",
  ),
  deepRoomCase(
    "band_chat_unread_count_deep[100000]",
    "BandChat · unread count in a room with a long history",
    "The reader's unread count for the 100,000-message room: messages from others after their marker (`$createdAt > marker`), capped at 100. Five are unread, so the count should cost five rows, not the room.",
    "Opening the count subscription until its first published result",
    "counts/s",
    "One count per iteration, returning 5.",
  ),
  deepRoomCase(
    "band_chat_unread_count_new_message[100000]",
    "BandChat · unread count goes up",
    "The reader's unread count for the 100,000-message room is live; a bandmate's message arrives and the count goes up by one.",
    "Message insert, authority acceptance, runtime ticks until the count changes",
    "messages delivered/s",
    "One message per iteration.",
  ),
  deepRoomCase(
    "band_chat_marker_move_fanout[50]",
    "BandChat · read receipts reach everyone in the room",
    "All 50 members have the room open with everyone's read markers (check marks). One member reads to the newest message: their marker moves and a read-journal row is appended in one transaction, and all 50 open views show the moved marker.",
    "Marker update and journal insert, authority acceptance, runtime ticks until every view shows it",
    "marker moves/s",
    "One marker move per iteration; the 50 open views are load context.",
  ),
  deepRoomCase(
    "band_chat_read_by_sheet[50]",
    "BandChat · who read this message",
    '"Read by" on a recent message in the 50-member room: each membership with the first read-journal entry that reaches the message (`upToAt >= message`, oldest first, limit 1), keeping members who read it. One read.',
    "One all_for_identity read",
    "sheets/s",
    "One sheet per iteration; 20 of 50 members have read the message.",
  ),
);
