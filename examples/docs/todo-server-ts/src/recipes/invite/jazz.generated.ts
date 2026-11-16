// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 9e76714575c1a1aa. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ChatInvite = {
  readonly id: string;
  chatId: string;
  code: string;
  singleUse: boolean;
};

export type ChatInviteInsert = {
  chatId: string;
  code: string;
  singleUse: boolean;
};

export type ChatMember = {
  readonly id: string;
  chatId: string;
  user_id: string;
  inviteId: string | null;
};

export type ChatMemberInsert = {
  chatId: string;
  user_id: string;
  inviteId?: string | null;
};

export type Chat = {
  readonly id: string;
};

export type ChatInsert = {};

interface Types extends AppTypes {
  chatInvites: {
    row: ChatInvite;
    insert: ChatInviteInsert;
    columns: {
      chatId: { kind: "uuid"; nullable: false; value: string };
      code: { kind: "string"; nullable: false; value: string };
      singleUse: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
    };
  };
  chatMembers: {
    row: ChatMember;
    insert: ChatMemberInsert;
    columns: {
      chatId: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
      inviteId: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
    };
  };
  chats: {
    row: Chat;
    insert: ChatInsert;
    columns: {};
    relations: {
      invites: { many: "chatInvites" };
      members: { many: "chatMembers" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:54e2e5dd096bb6229e84d8bbd558f9e1557b592631710592e305f38f472aec8d",
  "schema": {
    "current": "d276a8b864a5e91f27012e0ebbee3c2df84c4a24f1fc394271314b2d5edf8261",
    "schemas": [
      {
        "hash": "d276a8b864a5e91f27012e0ebbee3c2df84c4a24f1fc394271314b2d5edf8261",
        "schema": {
          "tables": {
            "chatInvites": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chatId",
                  "nullable": false,
                  "references": "chats",
                },
                { "column_type": { "type": "Text" }, "name": "code", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "singleUse", "nullable": false },
              ],
            },
            "chatMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chatId",
                  "nullable": false,
                  "references": "chats",
                },
                { "column_type": { "type": "Uuid" }, "name": "user_id", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "inviteId", "nullable": true },
              ],
            },
            "chats": { "columns": [] },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "chatInvites": { "chat": { "one": "chats", "column": "chatId" } },
    "chatMembers": { "chat": { "one": "chats", "column": "chatId" } },
    "chats": {
      "invites": { "many": "chatInvites", "relation": "chat" },
      "members": { "many": "chatMembers", "relation": "chat" },
    },
  },
});
