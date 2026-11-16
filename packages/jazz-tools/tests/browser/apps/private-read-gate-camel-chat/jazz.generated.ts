// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output ab66aaccb327b9ff. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type ChatMember = {
  readonly id: string;
  chatId: string;
  userId: string;
  joinCode: string | null;
};

export type ChatMemberInsert = {
  chatId: string;
  userId: string;
  joinCode?: string | null;
};

export type Chat = {
  readonly id: string;
  name: string | null;
  isPublic: boolean;
  createdBy: string;
  joinCode: string | null;
};

export type ChatInsert = {
  name?: string | null;
  isPublic: boolean;
  createdBy: string;
  joinCode?: string | null;
};

export type Message = {
  readonly id: string;
  chatId: string;
  senderId: string;
  text: string;
  createdAt: Date;
};

export type MessageInsert = {
  chatId: string;
  senderId: string;
  text: string;
  createdAt: Date;
};

export type Profile = {
  readonly id: string;
  userId: string;
  name: string;
  avatar: string | null;
};

export type ProfileInsert = {
  userId: string;
  name: string;
  avatar?: string | null;
};

export type Reaction = {
  readonly id: string;
  messageId: string;
  userId: string;
  emoji: string;
};

export type ReactionInsert = {
  messageId: string;
  userId: string;
  emoji: string;
};

interface Types extends AppTypes {
  chatMembers: {
    row: ChatMember;
    insert: ChatMemberInsert;
    columns: {
      chatId: { kind: "uuid"; nullable: false; value: string };
      userId: { kind: "string"; nullable: false; value: string };
      joinCode: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
    };
  };
  chats: {
    row: Chat;
    insert: ChatInsert;
    columns: {
      name: { kind: "string"; nullable: true; value: string };
      isPublic: { kind: "boolean"; nullable: false; value: boolean };
      createdBy: { kind: "string"; nullable: false; value: string };
      joinCode: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      chatMembersViaChat: { many: "chatMembers" };
      messagesViaChat: { many: "messages" };
    };
  };
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      chatId: { kind: "uuid"; nullable: false; value: string };
      senderId: { kind: "uuid"; nullable: false; value: string };
      text: { kind: "string"; nullable: false; value: string };
      createdAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      chat: { one: "chats"; nullable: false };
      reactionsViaMessage: { many: "reactions" };
      sender: { one: "profiles"; nullable: false };
    };
  };
  profiles: {
    row: Profile;
    insert: ProfileInsert;
    columns: {
      userId: { kind: "string"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      avatar: { kind: "string"; nullable: true; value: string };
    };
    relations: {
      messagesViaSender: { many: "messages" };
    };
  };
  reactions: {
    row: Reaction;
    insert: ReactionInsert;
    columns: {
      messageId: { kind: "uuid"; nullable: false; value: string };
      userId: { kind: "string"; nullable: false; value: string };
      emoji: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      message: { one: "messages"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:d275bce493dccf036ea79924669c615d03b93b72e740c63296e1b32c447d3cd2",
  "schema": {
    "current": "a9d567490e903de300ba4ce5b15633b35a86f78bbd7819381600088625abf795",
    "schemas": [
      {
        "hash": "a9d567490e903de300ba4ce5b15633b35a86f78bbd7819381600088625abf795",
        "schema": {
          "tables": {
            "chatMembers": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chatId",
                  "nullable": false,
                  "references": "chats",
                },
                { "column_type": { "type": "Text" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "joinCode", "nullable": true },
              ],
            },
            "chats": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": true },
                { "column_type": { "type": "Boolean" }, "name": "isPublic", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "createdBy", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "joinCode", "nullable": true },
              ],
            },
            "messages": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "chatId",
                  "nullable": false,
                  "references": "chats",
                },
                {
                  "column_type": { "type": "Uuid" },
                  "name": "senderId",
                  "nullable": false,
                  "references": "profiles",
                },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "createdAt", "nullable": false },
              ],
            },
            "profiles": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "avatar", "nullable": true },
              ],
            },
            "reactions": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "messageId",
                  "nullable": false,
                  "references": "messages",
                },
                { "column_type": { "type": "Text" }, "name": "userId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "emoji", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "chatMembers": { "chat": { "one": "chats", "column": "chatId" } },
    "chats": {
      "chatMembersViaChat": { "many": "chatMembers", "relation": "chat" },
      "messagesViaChat": { "many": "messages", "relation": "chat" },
    },
    "messages": {
      "chat": { "one": "chats", "column": "chatId" },
      "reactionsViaMessage": { "many": "reactions", "relation": "message" },
      "sender": { "one": "profiles", "column": "senderId" },
    },
    "profiles": { "messagesViaSender": { "many": "messages", "relation": "sender" } },
    "reactions": { "message": { "one": "messages", "column": "messageId" } },
  },
});
