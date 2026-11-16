// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output f27c01696da1fd10. Formatters may reformat this file, except `app` below.
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
  "source": "sha256:b1872f1d358544ff701d0a215f1c2cd1d104a0a7780b60d5b1b90605144ed87f",
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
