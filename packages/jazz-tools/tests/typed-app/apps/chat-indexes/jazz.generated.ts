// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 495c2f70e9aa0326. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Message = {
  readonly id: string;
  roomId: string;
  text: string;
};

export type MessageInsert = {
  roomId: string;
  text: string;
};

interface Types extends AppTypes {
  messages: {
    row: Message;
    insert: MessageInsert;
    columns: {
      roomId: { kind: "uuid"; nullable: false; value: string };
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:d94d08fe852bced3ea82a7685b7f433ed68d4a6b4f9fcd741627e3f77a2d5169",
  "schema": {
    "current": "25e661ee987d86522dd6a42d56adc437a59a42f49b5051e0e8248c7e0ab26115",
    "schemas": [
      {
        "hash": "25e661ee987d86522dd6a42d56adc437a59a42f49b5051e0e8248c7e0ab26115",
        "schema": {
          "tables": {
            "messages": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "roomId", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
              ],
              "composite_indexes": [["roomId", "$createdAt"], ["roomId", "$updatedAt"]],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "messages": {} },
});
