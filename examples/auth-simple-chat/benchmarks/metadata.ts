import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/auth-simple-chat/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database; in-process authority, no network";
const fixture = (roomMessages: number) =>
  `${roomMessages.toLocaleString("en-US")} messages in the general room and ${roomMessages.toLocaleString("en-US")} in announcements, from 16 authors; settled before timing.`;

// Mirrors `sends_for` in benches/walltime.rs.
const sendsFor = (roomMessages: number) => (roomMessages >= 10000 ? 10 : 100);

export const authChatBenchmarks: BenchmarkMetadata[] = [1000, 10000].flatMap((roomMessages) => [
  {
    name: `auth_chat_open_room[${roomMessages}]`,
    title: "Auth chat · open the room",
    description:
      "A signed-in member (role claim `member`) opens the general room: the room's whole history in send order, through the claim-gated read policy of the auth-simple-chat and auth-workos-chat examples.",
    fixture: fixture(roomMessages),
    storage,
    includes: ["subscribe_for_identity opening, runtime ticks and the first published result"],
    excludes: [
      "Schema compilation, seeding, query preparation and claim admission",
      "Token verification, network and subscription teardown",
    ],
    work: {
      count: 1,
      unit: "rooms opened/s",
      explanation: `One room opening per iteration, returning ${roomMessages.toLocaleString("en-US")} messages; not row throughput.`,
    },
    source,
  },
  {
    name: `auth_chat_send[${roomMessages}]`,
    title: "Auth chat · send messages",
    description: `A signed-in member sends ${sendsFor(roomMessages)} messages into the room they have open. Each is a standalone write that the in-process authority checks against the claim-gated insert policy and accepts, and that then appears in the open room. The open room retains its whole history, and each send's room update currently scales with that history (https://github.com/garden-co/jazz/issues/2086), so the per-message cost at ${roomMessages.toLocaleString("en-US")} messages is mostly that update, not the cost of sending one message.`,
    fixture: `${fixture(roomMessages)} The room is already open before timing.`,
    storage,
    includes: [
      "Message insert, authority insert-policy check and acceptance",
      "Runtime ticks until the open room shows each message",
    ],
    excludes: ["Room opening, seeding, token verification and network"],
    work: {
      count: sendsFor(roomMessages),
      unit: "messages sent/s",
      explanation: `${sendsFor(roomMessages)} messages, each sent, accepted and shown before the next.`,
    },
    source,
  },
]);
