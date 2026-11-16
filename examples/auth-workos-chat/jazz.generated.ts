// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 44b8bf946f06b3dc. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Message = {
  readonly id: string;
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
};

export type MessageInsert = {
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
};

interface Types extends AppTypes {
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      author_name: { kind: "string"; nullable: false; value: string };
      chat_id: { kind: "string"; nullable: false; value: string };
      text: { kind: "string"; nullable: false; value: string };
      sent_at: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:9436510498d972bda4563a3330b45b58d83d5a5f4ec7ccf418265ac85afee96f",
  "schema": {
    "current": "e26d5055b2f2734daa2182431b71c58abc26f6ef632385d451a997cf5b0003b7",
    "schemas": [
      {
        "hash": "e26d5055b2f2734daa2182431b71c58abc26f6ef632385d451a997cf5b0003b7",
        "schema": {
          "tables": {
            "messages": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "author_name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "chat_id", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "sent_at", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "messages": {} },
});
