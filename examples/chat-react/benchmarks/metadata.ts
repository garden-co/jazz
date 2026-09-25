import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/chat-react/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database; in-process authority, no network";
const fixture = (messages: number) =>
  `32 users, 64 chats (odd chats public), 4 members per chat, ${messages.toLocaleString("en-US")} messages of which a quarter are in the opened private chat; settled before timing.`;

export const chatBenchmarks: BenchmarkMetadata[] = [1000, 10000].flatMap((messages) => [
  {
    name: `chat_open_chat[${messages}]`,
    title: "Chat · open a chat",
    description:
      "A member opens a private chat: the newest 21 messages with their senders, newest first, through the membership read policy (the chat is public or the reader is a member).",
    fixture: fixture(messages),
    storage,
    includes: ["subscribe_for_identity opening, runtime ticks and the first published page"],
    excludes: [
      "Schema compilation, seeding and query preparation",
      "Reactions, canvases, network and subscription teardown",
    ],
    work: {
      count: 1,
      unit: "chats opened/s",
      explanation: "One chat opening per iteration, returning a 21-message page.",
    },
    source,
  },
  {
    name: `chat_send_100[${messages}]`,
    title: "Chat · send messages",
    description:
      "A member sends 100 messages into the chat they have open. Each is a standalone write that the in-process authority checks against the insert policy (member of the chat, sending as their own profile) and accepts, and that then appears at the top of the open page.",
    fixture: `${fixture(messages)} The chat is already open before timing.`,
    storage,
    includes: [
      "Message insert, authority insert-policy check and acceptance",
      "Runtime ticks until the open page shows each message",
    ],
    excludes: ["Chat opening, seeding and network"],
    work: {
      count: 100,
      unit: "messages sent/s",
      explanation: "100 messages, each sent, accepted and shown before the next.",
    },
    source,
  },
]);
