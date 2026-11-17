import { schema as s } from "jazz-tools";
import { tables as betterAuthSchema } from "./schema-better-auth/schema";

export const schema = s.defineSchema({
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
});

const { definePermissions, allowedTo, session, anyOf, allOf } = schema;

// A workspace belongs to one Jazz account: rows carry their owner's account.
const mine = { ownerAccount: session.user };

/**
 * The browser may start conversations, write the user's own turns and attach
 * audio to them. Only the server (backend authority) writes assistant turns,
 * tool calls and the seeded booking data, so a client can never forge an
 * agent reply.
 */
export const permissions = definePermissions({
  artists: {
    read: mine,
  },
  attachments: {
    read: (attachment) => allowedTo.read(attachment.conversation),
    insert: (attachment) =>
      allOf(
        attachment.conversation.where(mine),
        attachment.turn.where({ conversationId: attachment.conversationId, role: "user" }),
      ),
  },
  // Better Auth's rows are written only by the backend.
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  calendarEvents: {
    read: mine,
  },
  // A conversation may only name an artist from the same workspace: the agent
  // reads that artist's data with backend authority.
  conversations: {
    read: mine,
    insert: (conversation) => allOf(mine, conversation.artist.where(mine)),
    updateIf: mine,
    updateTo: (conversation) => allOf(mine, conversation.artist.where(mine)),
    delete: mine,
  },
  profiles: {
    read: { accountId: session.user },
  },
  songs: {
    read: mine,
  },
  toolCalls: {
    read: (toolCall) => allowedTo.read(toolCall.conversation),
  },
  turns: {
    read: (turn) => allowedTo.read(turn.conversation),
    insert: (turn) =>
      allOf(
        { role: "user", status: "complete" },
        turn.conversation.where(mine),
        // A turn continues its conversation: its parent, if any, is a turn of
        // the same conversation.
        anyOf({ parentId: null }, turn.parent.where({ conversationId: turn.conversationId })),
      ),
  },
  venues: {
    read: mine,
  },
});
