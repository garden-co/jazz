// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Note = {
  readonly id: string;
  text: string;
  seq: number;
};

export type NoteInsert = {
  text: string;
  seq: number;
};

interface Types extends AppTypes {
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
      seq: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:f6a58c9eb146472363cd4c02a5ec7701375f20dfc73ed39228c50cfac5d0b7be",
  "schema": {
    "current": "0cc209d9ee7ed7e5ba90d5fa3f23e5861f561fd7b9c78ef18793d89c7dfb32aa",
    "schemas": [
      {
        "hash": "0cc209d9ee7ed7e5ba90d5fa3f23e5861f561fd7b9c78ef18793d89c7dfb32aa",
        "schema": {
          "tables": {
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "seq", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "notes": {} },
});
