// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type TurnRole = "user" | "assistant" | "tool";

export type Attachment = {
  readonly id: string;
  turn_id: string;
  filename: string;
  media_type: string;
  payload: Uint8Array;
  byte_length: number;
};

export type AttachmentInsert = {
  turn_id: string;
  filename: string;
  media_type: string;
  payload: Uint8Array;
  byte_length: number;
};

export type Conversation = {
  readonly id: string;
  title: string;
  created_at: Date;
};

export type ConversationInsert = {
  title: string;
  created_at: Date;
};

export type ToolCall = {
  readonly id: string;
  turn_id: string;
  name: string;
  arguments_json: string;
  result_json: string;
};

export type ToolCallInsert = {
  turn_id: string;
  name: string;
  arguments_json: string;
  result_json: string;
};

export type Turn = {
  readonly id: string;
  conversation_id: string;
  role: TurnRole;
  ordinal: number;
  body: string;
  created_at: Date;
};

export type TurnInsert = {
  conversation_id: string;
  role: TurnRole;
  ordinal: number;
  body: string;
  created_at: Date;
};

interface Types extends AppTypes {
  attachments: {
    row: Attachment;
    insert: AttachmentInsert;
    columns: {
      turn_id: { kind: "uuid"; nullable: false; value: string };
      filename: { kind: "string"; nullable: false; value: string };
      media_type: { kind: "string"; nullable: false; value: string };
      payload: { kind: "bytes"; nullable: false; value: Uint8Array };
      byte_length: { kind: "int"; nullable: false; value: number };
    };
    relations: {
      turn: { one: "turns"; nullable: false };
    };
  };
  conversations: {
    row: Conversation;
    insert: ConversationInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      created_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      turnsViaConversation: { many: "turns" };
    };
  };
  tool_calls: {
    row: ToolCall;
    insert: ToolCallInsert;
    columns: {
      turn_id: { kind: "uuid"; nullable: false; value: string };
      name: { kind: "string"; nullable: false; value: string };
      arguments_json: { kind: "string"; nullable: false; value: string };
      result_json: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      turn: { one: "turns"; nullable: false };
    };
  };
  turns: {
    row: Turn;
    insert: TurnInsert;
    columns: {
      conversation_id: { kind: "uuid"; nullable: false; value: string };
      role: { kind: "enum"; nullable: false; value: TurnRole };
      ordinal: { kind: "int"; nullable: false; value: number };
      body: { kind: "string"; nullable: false; value: string };
      created_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {
      attachmentsViaTurn: { many: "attachments" };
      conversation: { one: "conversations"; nullable: false };
      tool_callsViaTurn: { many: "tool_calls" };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:82d023aacd8af4116e0d124760604b6fc0962bfa00a1332faef80ee9af746d2c",
  "schema": {
    "current": "26ed4a1743c7b8329712ce43758fb2524472f51d2a6b6d01e9da58989a854fe4",
    "schemas": [
      {
        "hash": "26ed4a1743c7b8329712ce43758fb2524472f51d2a6b6d01e9da58989a854fe4",
        "schema": {
          "tables": {
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "turn_id",
                  "nullable": false,
                  "references": "turns",
                },
                { "column_type": { "type": "Text" }, "name": "filename", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "media_type", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "byte_length", "nullable": false },
              ],
            },
            "conversations": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "created_at", "nullable": false },
              ],
            },
            "tool_calls": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "turn_id",
                  "nullable": false,
                  "references": "turns",
                },
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "arguments_json", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "result_json", "nullable": false },
              ],
            },
            "turns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversation_id",
                  "nullable": false,
                  "references": "conversations",
                },
                {
                  "column_type": { "type": "Enum", "variants": ["user", "assistant", "tool"] },
                  "name": "role",
                  "nullable": false,
                },
                { "column_type": { "type": "Integer" }, "name": "ordinal", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "created_at", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "attachments": { "turn": { "one": "turns", "column": "turn_id" } },
    "conversations": { "turnsViaConversation": { "many": "turns", "relation": "conversation" } },
    "tool_calls": { "turn": { "one": "turns", "column": "turn_id" } },
    "turns": {
      "attachmentsViaTurn": { "many": "attachments", "relation": "turn" },
      "conversation": { "one": "conversations", "column": "conversation_id" },
      "tool_callsViaTurn": { "many": "tool_calls", "relation": "turn" },
    },
  },
});
