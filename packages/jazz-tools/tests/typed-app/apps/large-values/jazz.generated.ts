// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 52f8a81c35079f25. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type JsonValue } from "jazz-tools";

export type Document = {
  readonly id: string;
  title: string;
  payload: Uint8Array;
  metadata: JsonValue;
  done: boolean;
};

export type DocumentInsert = {
  title: string;
  payload: Uint8Array;
  metadata: JsonValue;
  done: boolean;
};

interface Types extends AppTypes {
  documents: {
    row: Document;
    insert: DocumentInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      payload: { kind: "bytes"; nullable: false; value: Uint8Array };
      metadata: { kind: "json"; nullable: false; value: JsonValue };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:316e223648ec72cc911c9cfaadf86029e098a929748b12e08a8a528517fe0594",
  "schema": {
    "current": "2939b5d586b4c5b9f827fe6572b84f4512491cbe4d7a4aed743fe09d5da020c8",
    "schemas": [
      {
        "hash": "2939b5d586b4c5b9f827fe6572b84f4512491cbe4d7a4aed743fe09d5da020c8",
        "schema": {
          "tables": {
            "documents": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Bytea" }, "name": "payload", "nullable": false },
                { "column_type": { "type": "Json" }, "name": "metadata", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "documents": {} },
});
