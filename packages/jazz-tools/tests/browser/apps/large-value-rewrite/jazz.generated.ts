// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Attachment = {
  readonly id: string;
  conversation_id: string;
  filename: string;
  payload: Uint8Array;
};

export type AttachmentInsert = {
  conversation_id: string;
  filename: string;
  payload: Uint8Array;
};

export type Conversation = {
  readonly id: string;
  title: string;
};

export type ConversationInsert = {
  title: string;
};

export type Turn = {
  readonly id: string;
  conversation_id: string;
  body: string;
};

export type TurnInsert = {
  conversation_id: string;
  body: string;
};

interface Types extends AppTypes {
  attachments: {
    row: Attachment;
    insert: AttachmentInsert;
    columns: {
      conversation_id: { kind: "uuid"; nullable: false; value: string };
      filename: { kind: "string"; nullable: false; value: string };
      payload: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {
      conversation: { one: "conversations"; nullable: false };
    };
  };
  conversations: {
    row: Conversation;
    insert: ConversationInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      attachmentsViaConversation: { many: "attachments" };
      turnsViaConversation: { many: "turns" };
    };
  };
  turns: {
    row: Turn;
    insert: TurnInsert;
    columns: {
      conversation_id: { kind: "uuid"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {
      conversation: { one: "conversations"; nullable: false };
    };
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:2389735f42de911eaadbd0f544325176725f7610052db5e209faf48a8456fddd",
  "schema": {
    "current": "ba6d84510231ada3f44ea6209f1c897c3cab8c8f8b91c62f1b3c796e169acd22",
    "schemas": [
      {
        "hash": "ba6d84510231ada3f44ea6209f1c897c3cab8c8f8b91c62f1b3c796e169acd22",
        "schema": {
          "tables": {
            "attachments": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversation_id",
                  "nullable": false,
                  "references": "conversations",
                },
                { "column_type": { "type": "Text" }, "name": "filename", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": false },
              ],
            },
            "conversations": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
            "turns": {
              "columns": [
                {
                  "column_type": { "type": "Uuid" },
                  "name": "conversation_id",
                  "nullable": false,
                  "references": "conversations",
                },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": {
    "attachments": { "conversation": { "one": "conversations", "column": "conversation_id" } },
    "conversations": {
      "attachmentsViaConversation": { "many": "attachments", "relation": "conversation" },
      "turnsViaConversation": { "many": "turns", "relation": "conversation" },
    },
    "turns": { "conversation": { "one": "conversations", "column": "conversation_id" } },
  },
});
