// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 534f701c28bf5d87. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

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

export type Canvas = {
  readonly id: string;
  roomId: string;
  title: string;
};

export type CanvasInsert = {
  roomId: string;
  title: string;
};

export type JoinRequest = {
  readonly id: string;
  roomId: string;
  requester: string;
  profileId: string;
};

export type JoinRequestInsert = {
  roomId: string;
  requester: string;
  profileId: string;
};

export type Message = {
  readonly id: string;
  roomId: string;
  senderId: string;
  text: string;
  attachment: Uint8Array | null;
  attachmentName: string | null;
  attachmentType: string | null;
  attachmentSize: number | null;
  canvasId: string | null;
};

export type MessageInsert = {
  roomId: string;
  senderId: string;
  text: string;
  attachment?: Uint8Array | null;
  attachmentName?: string | null;
  attachmentType?: string | null;
  attachmentSize?: number | null;
  canvasId?: string | null;
};

export type Profile = {
  readonly id: string;
  author: string;
  displayName: string;
  avatar: Uint8Array | null;
  avatarType: string | null;
};

export type ProfileInsert = {
  author: string;
  displayName: string;
  avatar?: Uint8Array | null;
  avatarType?: string | null;
};

export type Reaction = {
  readonly id: string;
  roomId: string;
  messageId: string;
  author: string;
  emoji: string;
};

export type ReactionInsert = {
  roomId: string;
  messageId: string;
  author: string;
  emoji: string;
};

export type ReadMarker = {
  readonly id: string;
  roomId: string;
  reader: string;
  lastReadAt: Date;
};

export type ReadMarkerInsert = {
  roomId: string;
  reader: string;
  lastReadAt: Date;
};

export type RoomMember = {
  readonly id: string;
  roomId: string;
  memberAuthor: string;
  memberProfileId: string | null;
};

export type RoomMemberInsert = {
  roomId: string;
  memberAuthor: string;
  memberProfileId?: string | null;
};

export type Room = {
  readonly id: string;
  name: string;
};

export type RoomInsert = {
  name: string;
};

export type Stroke = {
  readonly id: string;
  canvasId: string;
  roomId: string;
  author: string;
  color: string;
  width: number;
  points: number[];
};

export type StrokeInsert = {
  canvasId: string;
  roomId: string;
  author: string;
  color: string;
  width: number;
  points: number[];
};

interface Types extends AppTypes {
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
  canvases: {
    row: Canvas;
    insert: CanvasInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      messagesViaCanvas: { many: "messages" };
      room: { one: "rooms"; nullable: false };
      strokesViaCanvas: { many: "strokes" };
    };
  };
  joinRequests: {
    row: JoinRequest;
    insert: JoinRequestInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      requester: { kind: "uuid"; nullable: false; value: string };
      profileId: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      profile: { one: "profiles"; nullable: false };
      room: { one: "rooms"; nullable: false };
    };
  };
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      senderId: { kind: "uuid"; nullable: false; value: string };
      text: { kind: "string"; nullable: false; value: string };
      attachment: { kind: "bytes"; nullable: true; value: Uint8Array };
      attachmentName: { kind: "string"; nullable: true; value: string };
      attachmentType: { kind: "string"; nullable: true; value: string };
      attachmentSize: { kind: "int"; nullable: true; value: number };
      canvasId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      canvas: { one: "canvases"; nullable: true };
      reactionsViaMessage: { many: "reactions" };
      room: { one: "rooms"; nullable: false };
      sender: { one: "profiles"; nullable: false };
    };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      author: { kind: "uuid"; nullable: false; value: string };
      displayName: { kind: "string"; nullable: false; value: string };
      avatar: { kind: "bytes"; nullable: true; value: Uint8Array };
      avatarType: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      joinRequestsViaProfile: { many: "joinRequests" };
      membershipsViaProfile: { many: "roomMembers" };
      messagesViaSender: { many: "messages" };
    };
  };
  reactions: {
    row: Reaction;
    insert: ReactionInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      messageId: { kind: "uuid"; nullable: false; value: string };
      author: { kind: "uuid"; nullable: false; value: string };
      emoji: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      message: { one: "messages"; nullable: false };
      room: { one: "rooms"; nullable: false };
    };
  };
  readMarkers: {
    row: ReadMarker;
    insert: ReadMarkerInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      reader: { kind: "uuid"; nullable: false; value: string };
      lastReadAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      room: { one: "rooms"; nullable: false };
    };
  };
  roomMembers: {
    row: RoomMember;
    insert: RoomMemberInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      memberAuthor: { kind: "uuid"; nullable: false; value: string };
      memberProfileId: { kind: "uuid"; nullable: true; value: string };
    };
    relations: {
      memberProfile: { one: "profiles"; nullable: true };
      room: { one: "rooms"; nullable: false };
    };
  };
  rooms: {
    row: Room;
    insert: RoomInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      canvasesViaRoom: { many: "canvases" };
      joinRequestsViaRoom: { many: "joinRequests" };
      messagesViaRoom: { many: "messages" };
      reactionsViaRoom: { many: "reactions" };
      readMarkersViaRoom: { many: "readMarkers" };
      roomMembersViaRoom: { many: "roomMembers" };
    };
  };
  strokes: {
    row: Stroke;
    insert: StrokeInsert;
    columns: {
      canvasId: { kind: "uuid"; nullable: false; value: string };
      roomId: { kind: "uuid"; nullable: false; value: string };
      author: { kind: "uuid"; nullable: false; value: string };
      color: { kind: "string"; nullable: false; value: string };
      width: { kind: "int"; nullable: false; value: number };
      points: { kind: "array"; nullable: false; value: number[] };
    };
    relations: {
      canvas: { one: "canvases"; nullable: false };
      room: { one: "rooms"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b2ea80621d12aafa97d9536e5f16ad75f121746724cfd32004a8b64613a36efc",
  "schema": {
    "current": "2322e143bd2759b30f51c27f300fb37206653faf1142a5cc7b16fc9f821ff7a7",
    "schemas": [
      {
        "hash": "0fe493411de2f896802f229b2d445a17fcb95794a8c492906c2ba22c1cf7a8d0",
        "schema": {
          "tables": {
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
            "canvases": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
            "joinRequests": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "requester", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "profileId",
                  "nullable": false,
                  "references": "profiles",
                },
              ],
            },
            "messages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "senderId",
                  "nullable": false,
                  "references": "profiles",
                },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "attachment", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "attachmentName", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "attachmentType", "nullable": true },
                { "column_type": { "type": "Integer" }, "name": "attachmentSize", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": true,
                  "references": "canvases",
                },
              ],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "avatar", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "avatarType", "nullable": true },
              ],
            },
            "reactions": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "messageId",
                  "nullable": false,
                  "references": "messages",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "emoji", "nullable": false },
              ],
            },
            "readMarkers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "reader", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "lastReadAt", "nullable": false },
              ],
            },
            "roomMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "memberAuthor", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "memberProfileId",
                  "nullable": true,
                  "references": "profiles",
                },
              ],
            },
            "rooms": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "strokes": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                {
                  "column_type": { "element": { "type": "Integer" }, "type": "Array" },
                  "name": "points",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
      {
        "hash": "2322e143bd2759b30f51c27f300fb37206653faf1142a5cc7b16fc9f821ff7a7",
        "schema": {
          "tables": {
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
            "canvases": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
              ],
            },
            "joinRequests": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "requester", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "profileId",
                  "nullable": false,
                  "references": "profiles",
                },
              ],
            },
            "messages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "senderId",
                  "nullable": false,
                  "references": "profiles",
                },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "attachment", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "attachmentName", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "attachmentType", "nullable": true },
                { "column_type": { "type": "Integer" }, "name": "attachmentSize", "nullable": true },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": true,
                  "references": "canvases",
                },
              ],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "displayName", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "avatar", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "avatarType", "nullable": true },
              ],
            },
            "reactions": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "messageId",
                  "nullable": false,
                  "references": "messages",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "emoji", "nullable": false },
              ],
            },
            "readMarkers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "reader", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "lastReadAt", "nullable": false },
              ],
            },
            "roomMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "memberAuthor", "nullable": false },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "memberProfileId",
                  "nullable": true,
                  "references": "profiles",
                },
              ],
            },
            "rooms": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "name", "nullable": false }],
            },
            "strokes": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "canvasId",
                  "nullable": false,
                  "references": "canvases",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "roomId",
                  "nullable": false,
                  "references": "rooms",
                },
                { "column_type": { "type": "Uuid" }, "name": "author", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "color", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "width", "nullable": false },
                {
                  "column_type": { "element": { "type": "Integer" }, "type": "Array" },
                  "name": "points",
                  "nullable": false,
                },
              ],
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
        "fromHash": "0fe493411de2f896802f229b2d445a17fcb95794a8c492906c2ba22c1cf7a8d0",
        "toHash": "2322e143bd2759b30f51c27f300fb37206653faf1142a5cc7b16fc9f821ff7a7",
      },
    ],
  },
  "relations": {
    "better_auth_account": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_jwks": {},
    "better_auth_session": { "user": { "one": "better_auth_user", "column": "userId" } },
    "better_auth_user": {
      "better_auth_accountViaUser": { "many": "better_auth_account", "relation": "user" },
      "better_auth_sessionViaUser": { "many": "better_auth_session", "relation": "user" },
    },
    "better_auth_verification": {},
    "canvases": {
      "messagesViaCanvas": { "many": "messages", "relation": "canvas" },
      "room": { "one": "rooms", "column": "roomId" },
      "strokesViaCanvas": { "many": "strokes", "relation": "canvas" },
    },
    "joinRequests": {
      "profile": { "one": "profiles", "column": "profileId" },
      "room": { "one": "rooms", "column": "roomId" },
    },
    "messages": {
      "canvas": { "one": "canvases", "column": "canvasId" },
      "reactionsViaMessage": { "many": "reactions", "relation": "message" },
      "room": { "one": "rooms", "column": "roomId" },
      "sender": { "one": "profiles", "column": "senderId" },
    },
    "profiles": {
      "joinRequestsViaProfile": { "many": "joinRequests", "relation": "profile" },
      "membershipsViaProfile": { "many": "roomMembers", "relation": "memberProfile" },
      "messagesViaSender": { "many": "messages", "relation": "sender" },
    },
    "reactions": {
      "message": { "one": "messages", "column": "messageId" },
      "room": { "one": "rooms", "column": "roomId" },
    },
    "readMarkers": { "room": { "one": "rooms", "column": "roomId" } },
    "roomMembers": {
      "memberProfile": { "one": "profiles", "column": "memberProfileId" },
      "room": { "one": "rooms", "column": "roomId" },
    },
    "rooms": {
      "canvasesViaRoom": { "many": "canvases", "relation": "room" },
      "joinRequestsViaRoom": { "many": "joinRequests", "relation": "room" },
      "messagesViaRoom": { "many": "messages", "relation": "room" },
      "reactionsViaRoom": { "many": "reactions", "relation": "room" },
      "readMarkersViaRoom": { "many": "readMarkers", "relation": "room" },
      "roomMembersViaRoom": { "many": "roomMembers", "relation": "room" },
    },
    "strokes": {
      "canvas": { "one": "canvases", "column": "canvasId" },
      "room": { "one": "rooms", "column": "roomId" },
    },
  },
});
