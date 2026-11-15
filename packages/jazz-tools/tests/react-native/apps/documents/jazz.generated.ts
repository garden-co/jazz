// jazz.generated.ts: generated from jazz.app.json by `jazz-tools validate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export interface Document {
  readonly id: string;
  title: string;
  done: boolean;
}

export interface DocumentInsert {
  title: string;
  done: boolean;
}

interface Types extends AppTypes {
  documents: {
    row: Document;
    insert: DocumentInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:0daf0370934d04f5d0577d78e992236f6ce5f194f46ca9ea25c15d7a239c0e4b",
  "schema": {
    "current": "4467013fb5b5b30d8040cf53933938a4f0ae68b4082e7670a8413f823c345c78",
    "schemas": [
      {
        "hash": "4467013fb5b5b30d8040cf53933938a4f0ae68b4082e7670a8413f823c345c78",
        "schema": {
          "tables": {
            "documents": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
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
