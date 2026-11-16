// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type CalendarEventKind = "show" | "hold" | "travel" | "studio";
export type SongEnergy = "low" | "medium" | "high";
export type ToolCallStatus = "running" | "complete" | "error";
export type TurnRole = "user" | "assistant";
export type TurnStatus = "queued" | "streaming" | "complete" | "interrupted" | "failed";

export type Artist = {
  readonly id: string;
  ownerAccount: string;
  name: string;
  homeCity: string;
  genre: string;
};

export type ArtistInsert = {
  ownerAccount: string;
  name: string;
  homeCity: string;
  genre: string;
};

export type Attachment = {
  readonly id: string;
  conversationId: string;
  turnId: string;
  filename: string;
  mediaType: string;
  byteLength: number;
  payload: Uint8Array;
};

export type AttachmentInsert = {
  conversationId: string;
  turnId: string;
  filename: string;
  mediaType: string;
  byteLength: number;
  payload: Uint8Array;
};

export type BetterAuthAccount = {
  readonly id: string;
  accountId: string;
  providerId: string;
  userId: string;
  password: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthAccountInsert = {
  accountId: string;
  providerId: string;
  userId: string;
  password?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthJwk = {
  readonly id: string;
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt: Date | null;
  alg: string | null;
  crv: string | null;
};

export type BetterAuthJwkInsert = {
  publicKey: string;
  privateKey: string;
  createdAt: Date;
  expiresAt?: Date | null;
  alg?: string | null;
  crv?: string | null;
};

export type BetterAuthSession = {
  readonly id: string;
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress: string | null;
  userAgent: string | null;
  userId: string;
};

export type BetterAuthSessionInsert = {
  expiresAt: Date;
  token: string;
  createdAt: Date;
  updatedAt: Date;
  ipAddress?: string | null;
  userAgent?: string | null;
  userId: string;
};

export type BetterAuthUser = {
  readonly id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthUserInsert = {
  name: string;
  email: string;
  emailVerified: boolean;
  image?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerification = {
  readonly id: string;
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type BetterAuthVerificationInsert = {
  identifier: string;
  value: string;
  expiresAt: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type CalendarEvent = {
  readonly id: string;
  ownerAccount: string;
  artistId: string;
  date: string;
  kind: CalendarEventKind;
  title: string;
  city: string;
};

export type CalendarEventInsert = {
  ownerAccount: string;
  artistId: string;
  date: string;
  kind: CalendarEventKind;
  title: string;
  city: string;
};

export type Conversation = {
  readonly id: string;
  ownerAccount: string;
  artistId: string;
  title: string;
  headTurnId: string | null;
};

export type ConversationInsert = {
  ownerAccount: string;
  artistId: string;
  title: string;
  headTurnId?: string | null;
};

export type Profile = {
  readonly id: string;
  accountId: string;
  authUserId: string;
  displayName: string;
};

export type ProfileInsert = {
  accountId: string;
  authUserId: string;
  displayName: string;
};

export type Song = {
  readonly id: string;
  ownerAccount: string;
  artistId: string;
  title: string;
  durationSeconds: number;
  energy: SongEnergy;
};

export type SongInsert = {
  ownerAccount: string;
  artistId: string;
  title: string;
  durationSeconds: number;
  energy: SongEnergy;
};

export type ToolCall = {
  readonly id: string;
  conversationId: string;
  turnId: string;
  ordinal: number;
  name: string;
  argumentsJson: string;
  resultJson: string | null;
  status: ToolCallStatus;
  durationMs: number | null;
};

export type ToolCallInsert = {
  conversationId: string;
  turnId: string;
  ordinal: number;
  name: string;
  argumentsJson: string;
  resultJson?: string | null;
  status: ToolCallStatus;
  durationMs?: number | null;
};

export type Turn = {
  readonly id: string;
  conversationId: string;
  parentId: string | null;
  role: TurnRole;
  body: string;
  status: TurnStatus;
  provider: string | null;
  error: string | null;
  runnerId: string | null;
  heartbeatAt: Date | null;
};

export type TurnInsert = {
  conversationId: string;
  parentId?: string | null;
  role: TurnRole;
  body: string;
  status: TurnStatus;
  provider?: string | null;
  error?: string | null;
  runnerId?: string | null;
  heartbeatAt?: Date | null;
};

export type Venue = {
  readonly id: string;
  ownerAccount: string;
  name: string;
  city: string;
  capacity: number;
  style: string;
  bookingContact: string;
};

export type VenueInsert = {
  ownerAccount: string;
  name: string;
  city: string;
  capacity: number;
  style: string;
  bookingContact: string;
};

interface Types extends AppTypes {
  artists: {
    row: Artist;
    insert: ArtistInsert;
    columns: {
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      homeCity: { kind: "string"; nullable: false; value: string };
      genre: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      calendarEventsViaArtist: { many: "calendarEvents" };
      conversationsViaArtist: { many: "conversations" };
      songsViaArtist: { many: "songs" };
    };
  };
  attachments: {
    row: Attachment;
    insert: AttachmentInsert;
    columns: {
      conversationId: { kind: "uuid"; nullable: false; value: string };
      turnId: { kind: "uuid"; nullable: false; value: string };
      filename: { kind: "string"; nullable: false; value: string };
      mediaType: { kind: "string"; nullable: false; value: string };
      byteLength: { kind: "int"; nullable: false; value: number };
      payload: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {
      conversation: { one: "conversations"; nullable: false };
      turn: { one: "turns"; nullable: false };
    };
  };
  better_auth_account: {
    row: BetterAuthAccount;
    insert: BetterAuthAccountInsert;
    columns: {
      accountId: { kind: "string"; nullable: false; value: string };
      providerId: { kind: "string"; nullable: false; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
      password: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_jwks: {
    row: BetterAuthJwk;
    insert: BetterAuthJwkInsert;
    columns: {
      publicKey: { kind: "string"; nullable: false; value: string };
      privateKey: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      expiresAt: { kind: "timestamp"; nullable: true; value: Date };
      alg: { kind: "string"; nullable: true; value: string };
      crv: { kind: "string"; nullable: true; value: string };
    };
    relations: {};
  };
  better_auth_session: {
    row: BetterAuthSession;
    insert: BetterAuthSessionInsert;
    columns: {
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      token: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
      ipAddress: { kind: "string"; nullable: true; value: string };
      userAgent: { kind: "string"; nullable: true; value: string };
      userId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      user: { one: "better_auth_user"; nullable: false };
    };
  };
  better_auth_user: {
    row: BetterAuthUser;
    insert: BetterAuthUserInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
      emailVerified: { kind: "boolean"; nullable: false; value: boolean };
      image: { kind: "string"; nullable: true; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      better_auth_accountViaUser: { many: "better_auth_account" };
      better_auth_sessionViaUser: { many: "better_auth_session" };
    };
  };
  better_auth_verification: {
    row: BetterAuthVerification;
    insert: BetterAuthVerificationInsert;
    columns: {
      identifier: { kind: "string"; nullable: false; value: string };
      value: { kind: "string"; nullable: false; value: string };
      expiresAt: { kind: "timestamp"; nullable: false; value: Date };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
  calendarEvents: {
    row: CalendarEvent;
    insert: CalendarEventInsert;
    columns: {
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
      artistId: { kind: "uuid"; nullable: false; value: string };
      date: { kind: "string"; nullable: false; value: string };
      kind: { kind: "enum"; nullable: false; value: CalendarEventKind };
      title: { kind: "string"; nullable: false; value: string };
      city: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      artist: { one: "artists"; nullable: false };
    };
  };
  conversations: {
    row: Conversation;
    insert: ConversationInsert;
    columns: {
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
      artistId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      headTurnId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      artist: { one: "artists"; nullable: false };
      attachmentsViaConversation: { many: "attachments" };
      toolCallsViaConversation: { many: "toolCalls" };
      turnsViaConversation: { many: "turns" };
    };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      accountId: { kind: "uuid"; nullable: false; value: string };
      authUserId: { kind: "string"; nullable: false; value: string };
      displayName: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  songs: {
    row: Song;
    insert: SongInsert;
    columns: {
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
      artistId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
      durationSeconds: { kind: "int"; nullable: false; value: number };
      energy: { kind: "enum"; nullable: false; value: SongEnergy };
    };
    relations: {
      artist: { one: "artists"; nullable: false };
    };
  };
  toolCalls: {
    row: ToolCall;
    insert: ToolCallInsert;
    columns: {
      conversationId: { kind: "uuid"; nullable: false; value: string };
      turnId: { kind: "uuid"; nullable: false; value: string };
      ordinal: { kind: "int"; nullable: false; value: number };
      name: { kind: "string"; nullable: false; value: string };
      argumentsJson: { kind: "string"; nullable: false; value: string };
      resultJson: { kind: "string"; nullable: true; value: string };
      status: { kind: "enum"; nullable: false; value: ToolCallStatus };
      durationMs: { kind: "int"; nullable: true; value: number };
    };
    relations: {
      conversation: { one: "conversations"; nullable: false };
    };
  };
  turns: {
    row: Turn;
    insert: TurnInsert;
    columns: {
      conversationId: { kind: "uuid"; nullable: false; value: string };
      parentId: { kind: "uuid"; nullable: true; value: string };
      role: { kind: "enum"; nullable: false; value: TurnRole };
      body: { kind: "string"; nullable: false; value: string };
      status: { kind: "enum"; nullable: false; value: TurnStatus };
      provider: { kind: "string"; nullable: true; value: string };
      error: { kind: "string"; nullable: true; value: string };
      runnerId: { kind: "string"; nullable: true; value: string };
      heartbeatAt: { kind: "timestamp"; nullable: true; value: Date };
    };
    relations: {
      attachmentsViaTurn: { many: "attachments" };
      childrenViaParent: { many: "turns" };
      conversation: { one: "conversations"; nullable: false };
      parent: { one: "turns"; nullable: true };
    };
  };
  venues: {
    row: Venue;
    insert: VenueInsert;
    columns: {
      ownerAccount: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      city: { kind: "string"; nullable: false; value: string };
      capacity: { kind: "int"; nullable: false; value: number };
      style: { kind: "string"; nullable: false; value: string };
      bookingContact: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:20649b4171bf34fd96832203c7ab0e646a82da077d6e7f157d3576f349da42f9",
  "schema": {
    "current": "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
    "schemas": [
      {
        "hash": "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
        "schema": {
          "tables": {
            "artists": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "homeCity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "genre", "nullable": false },
              ],
            },
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "turnId",
                  "nullable": false,
                  "references": "turns",
                },
                { "column_type": { "type": "Text" }, "name": "filename", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mediaType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": false },
              ],
              "indexed_columns": ["conversationId"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "issuer", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "calendarEvents": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "date", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["show", "hold", "travel", "studio"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "city", "nullable": false },
              ],
              "indexed_columns": ["ownerAccount", "date"],
            },
            "conversations": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "headTurnId", "nullable": true },
              ],
              "indexed_columns": ["ownerAccount"],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "authUserId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
              ],
              "indexed_columns": ["accountId", "authUserId"],
            },
            "songs": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "durationSeconds",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Enum", "variants": ["low", "medium", "high"] },
                  "name": "energy",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["ownerAccount"],
            },
            "toolCalls": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                { "column_type": { "type": "Uuid" }, "name": "turnId", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "ordinal", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "argumentsJson", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "resultJson", "nullable": true },
                {
                  "column_type": { "type": "Enum", "variants": ["running", "complete", "error"] },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "durationMs", "nullable": true },
              ],
              "indexed_columns": ["conversationId"],
            },
            "turns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": true,
                  "references": "turns",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["user", "assistant"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["queued", "streaming", "complete", "interrupted", "failed"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "provider", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "error", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "runnerId", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "heartbeatAt", "nullable": true },
              ],
              "indexed_columns": ["conversationId", "status"],
            },
            "venues": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "city", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "capacity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "style", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "bookingContact", "nullable": false },
              ],
              "indexed_columns": ["ownerAccount", "city"],
            },
          },
        },
      },
      {
        "hash": "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
        "schema": {
          "tables": {
            "artists": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "homeCity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "genre", "nullable": false },
              ],
            },
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "turnId",
                  "nullable": false,
                  "references": "turns",
                },
                { "column_type": { "type": "Text" }, "name": "filename", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "mediaType", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byteLength", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": false },
              ],
              "indexed_columns": ["conversationId"],
            },
            "better_auth_account": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "providerId", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
                { "column_type": { "type": "Text" }, "name": "password", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_jwks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "publicKey", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "privateKey", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "alg", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "crv", "nullable": true },
              ],
            },
            "better_auth_session": {
              "columns": [
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "token", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "ipAddress", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "userAgent", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "userId",
                  "nullable": false,
                  "references": "better_auth_user",
                },
              ],
            },
            "better_auth_user": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "emailVerified", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "image", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "better_auth_verification": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "identifier", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "value", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "expiresAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
            },
            "calendarEvents": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "date", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["show", "hold", "travel", "studio"],
                  },
                  "name": "kind",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "city", "nullable": false },
              ],
              "indexed_columns": ["ownerAccount", "date"],
            },
            "conversations": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "headTurnId", "nullable": true },
              ],
              "indexed_columns": ["ownerAccount"],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "accountId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "authUserId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
              ],
              "indexed_columns": ["accountId", "authUserId"],
            },
            "songs": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "artistId",
                  "nullable": false,
                  "references": "artists",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "name": "durationSeconds",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Enum", "variants": ["low", "medium", "high"] },
                  "name": "energy",
                  "nullable": false,
                },
              ],
              "indexed_columns": ["ownerAccount"],
            },
            "toolCalls": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                { "column_type": { "type": "Uuid" }, "name": "turnId", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "ordinal", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "argumentsJson", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "resultJson", "nullable": true },
                {
                  "column_type": { "type": "Enum", "variants": ["running", "complete", "error"] },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "durationMs", "nullable": true },
              ],
              "indexed_columns": ["conversationId"],
            },
            "turns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversationId",
                  "nullable": false,
                  "references": "conversations",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "parentId",
                  "nullable": true,
                  "references": "turns",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["user", "assistant"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": {
                    "type": "Enum",
                    "variants": ["queued", "streaming", "complete", "interrupted", "failed"],
                  },
                  "name": "status",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "provider", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "error", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "runnerId", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "heartbeatAt", "nullable": true },
              ],
              "indexed_columns": ["conversationId", "status"],
            },
            "venues": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerAccount", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "city", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "capacity", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "style", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "bookingContact", "nullable": false },
              ],
              "indexed_columns": ["ownerAccount", "city"],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "issuer",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "better_auth_account",
          },
        ],
        "fromHash": "5c11d90336bf4b28b4d6f5d0db9bc18f3c744527135959aa7bbda98dd4ee8c99",
        "toHash": "ab70708b1e97287537c8675ff0dba1cabde8b4d584cc896aec8bcec588cb632d",
      },
    ],
  },
  "relations": {
    "artists": {
      "calendarEventsViaArtist": { "many": "calendarEvents", "relation": "artist" },
      "conversationsViaArtist": { "many": "conversations", "relation": "artist" },
      "songsViaArtist": { "many": "songs", "relation": "artist" },
    },
    "attachments": {
      "conversation": { "one": "conversations", "column": "conversationId" },
      "turn": { "one": "turns", "column": "turnId" },
    },
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "calendarEvents": { "artist": { "one": "artists", "column": "artistId" } },
    "conversations": {
      "artist": { "one": "artists", "column": "artistId" },
      "attachmentsViaConversation": { "many": "attachments", "relation": "conversation" },
      "toolCallsViaConversation": { "many": "toolCalls", "relation": "conversation" },
      "turnsViaConversation": { "many": "turns", "relation": "conversation" },
    },
    "profiles": {},
    "songs": { "artist": { "one": "artists", "column": "artistId" } },
    "toolCalls": { "conversation": { "one": "conversations", "column": "conversationId" } },
    "turns": {
      "attachmentsViaTurn": { "many": "attachments", "relation": "turn" },
      "childrenViaParent": { "many": "turns", "relation": "parent" },
      "conversation": { "one": "conversations", "column": "conversationId" },
      "parent": { "one": "turns", "column": "parentId" },
    },
    "venues": {},
  },
});
