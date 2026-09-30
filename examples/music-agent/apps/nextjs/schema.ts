import { schema as s } from "jazz-tools";
import { schema as betterAuthSchema } from "./schema-better-auth/schema";

const schema = {
  ...betterAuthSchema,

  // Written once by the bootstrap route. Links the Better Auth user (known from
  // the session cookie) to the Jazz account that owns this workspace, so routes
  // that only see cookies (the audio element's range requests) can authorize.
  profiles: s
    .table({ accountId: s.uuid(), authUserId: s.string(), displayName: s.string() }, {})
    .indexOnly(["accountId", "authUserId"]),

  // Booking data the agent's tools read. Seeded per workspace by bootstrap.
  artists: s.table(
    { ownerAccount: s.uuid(), name: s.string(), homeCity: s.string(), genre: s.string() },
    {
      songsViaArtist: s.reverse("songs", "artist"),
      calendarEventsViaArtist: s.reverse("calendarEvents", "artist"),
      conversationsViaArtist: s.reverse("conversations", "artist"),
    },
  ),
  songs: s
    .table(
      {
        ownerAccount: s.uuid(),
        artistId: s.uuid(),
        title: s.string(),
        durationSeconds: s.int(),
        energy: s.enum("low", "medium", "high"),
      },
      { artist: s.rel("artists", "artistId") },
    )
    .indexOnly(["ownerAccount"]),
  venues: s
    .table(
      {
        ownerAccount: s.uuid(),
        name: s.string(),
        city: s.string(),
        capacity: s.int(),
        style: s.string(),
        bookingContact: s.string(),
      },
      {},
    )
    .indexOnly(["ownerAccount", "city"]),
  calendarEvents: s
    .table(
      {
        ownerAccount: s.uuid(),
        artistId: s.uuid(),
        date: s.string(), // ISO calendar date, e.g. 2026-11-14
        kind: s.enum("show", "hold", "travel", "studio"),
        title: s.string(),
        city: s.string(),
      },
      { artist: s.rel("artists", "artistId") },
    )
    .indexOnly(["ownerAccount", "date"]),

  conversations: s
    .table(
      {
        ownerAccount: s.uuid(),
        artistId: s.uuid(),
        title: s.string(),
        // The leaf of the branch the conversation currently shows.
        headTurnId: s.uuid().optional(),
      },
      {
        artist: s.rel("artists", "artistId"),
        turnsViaConversation: s.reverse("turns", "conversation"),
        toolCallsViaConversation: s.reverse("toolCalls", "conversation"),
        attachmentsViaConversation: s.reverse("attachments", "conversation"),
      },
    )
    .indexOnly(["ownerAccount"]),

  // Turns form a tree: regenerating a reply adds a sibling under the same
  // parent, so every earlier answer stays reachable as its own branch.
  turns: s
    .table(
      {
        conversationId: s.uuid(),
        parentId: s.uuid().optional(),
        role: s.enum("user", "assistant"),
        // Assistant prose grows by page-relative appends while it streams.
        body: s.string(),
        status: s.enum("queued", "streaming", "complete", "interrupted", "failed"),
        provider: s.string().optional(),
        error: s.string().optional(),
        // Durable execution: the server process generating this turn and its
        // last sign of life. A stale heartbeat means the process went away.
        runnerId: s.string().optional(),
        heartbeatAt: s.timestamp().optional(),
      },
      {
        conversation: s.rel("conversations", "conversationId"),
        parent: s.rel("turns", "parentId"),
        childrenViaParent: s.reverse("turns", "parent"),
        attachmentsViaTurn: s.reverse("attachments", "turn"),
      },
    )
    .indexOnly(["conversationId", "status"]),
  toolCalls: s
    .table(
      {
        conversationId: s.uuid(),
        turnId: s.uuid(),
        ordinal: s.int(),
        name: s.string(),
        argumentsJson: s.string(),
        resultJson: s.string().optional(),
        status: s.enum("running", "complete", "error"),
        durationMs: s.int().optional(),
      },
      { conversation: s.rel("conversations", "conversationId") },
    )
    .indexOnly(["conversationId"]),
  attachments: s
    .table(
      {
        conversationId: s.uuid(),
        turnId: s.uuid(),
        filename: s.string(),
        mediaType: s.string(),
        byteLength: s.int(),
        // Audio bytes. The player reads them page by page through HTTP ranges.
        payload: s.bytes(),
      },
      { conversation: s.rel("conversations", "conversationId"), turn: s.rel("turns", "turnId") },
    )
    .indexOnly(["conversationId"]),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Conversation = s.RowOf<typeof app.conversations>;
export type Turn = s.RowOf<typeof app.turns>;
/** Turns as the UI reads them: with Jazz's own creation time (`$createdAt`). */
export type TimedTurn = Turn & { $createdAt: Date };
export type ToolCall = s.RowOf<typeof app.toolCalls>;
export type Attachment = s.RowOf<typeof app.attachments>;
