// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Announcement = {
  readonly id: string;
  title: string;
};

export type AnnouncementInsert = {
  title: string;
};

export type ChatMember = {
  readonly id: string;
  chat_id: string;
  user_id: string;
};

export type ChatMemberInsert = {
  chat_id: string;
  user_id: string;
};

export type Chat = {
  readonly id: string;
  title: string;
  visibility: string;
  owner_id: string;
};

export type ChatInsert = {
  title: string;
  visibility: string;
  owner_id: string;
};

export type Message = {
  readonly id: string;
  chat_id: string;
  body: string;
  author_id: string;
  owner_id: string;
};

export type MessageInsert = {
  chat_id: string;
  body: string;
  author_id: string;
  owner_id: string;
};

interface Types extends AppTypes {
  announcements: {
    row: Announcement;
    insert: AnnouncementInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  chat_members: {
    row: ChatMember;
    insert: ChatMemberInsert;
    columns: {
      chat_id: { kind: "uuid"; nullable: false; value: string };
      user_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
    };
  };
  chats: {
    row: Chat;
    insert: ChatInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      visibility: { kind: "string"; nullable: false; value: string };
      owner_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      chat_membersViaChat: { many: "chat_members" };
      messagesViaChat: { many: "messages" };
    };
  };
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      chat_id: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
      author_id: { kind: "uuid"; nullable: false; value: string };
      owner_id: { kind: "uuid"; nullable: false; value: string };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:f4beee6fa5a48394439d718133245b46a3d5d599c23d8576ff6697a97cd7ecc8",
  "schema": {
    "current": "e0cef99dc9ae9b72926053048d318821c23b8dd35e59ba4d054060c0d3d361cc",
    "schemas": [
      {
        "hash": "e0cef99dc9ae9b72926053048d318821c23b8dd35e59ba4d054060c0d3d361cc",
        "schema": {
          "tables": {
            "announcements": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
            "chat_members": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chat_id",
                  "nullable": false,
                  "references": "chats",
                },
                { "column_type": { "type": "Uuid" }, "name": "user_id", "nullable": false },
              ],
            },
            "chats": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "visibility", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
              ],
            },
            "messages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chat_id",
                  "nullable": false,
                  "references": "chats",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "author_id", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "owner_id", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "announcements": {},
    "chat_members": { "chat": { "one": "chats", "column": "chat_id" } },
    "chats": {
      "chat_membersViaChat": { "many": "chat_members", "relation": "chat" },
      "messagesViaChat": { "many": "messages", "relation": "chat" },
    },
    "messages": { "chat": { "one": "chats", "column": "chat_id" } },
  },
});
