// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Note = {
  readonly id: string;
  title: string;
  done: boolean;
};

export type NoteInsert = {
  title: string;
  done: boolean;
};

interface Types extends AppTypes {
  notes: {
    row: Note;
    insert: NoteInsert;
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
  "source": "sha256:02266cf45caf99025a83d8975078221322e7414a504130efe24d6b1468ef5a92",
  "schema": {
    "current": "13f261f41d699e1a7f60d5c6c77ddf8ca63431f4ec76b48967ef1c5eae77fb25",
    "schemas": [
      {
        "hash": "13f261f41d699e1a7f60d5c6c77ddf8ca63431f4ec76b48967ef1c5eae77fb25",
        "schema": {
          "tables": {
            "notes": {
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
  "relations": { "notes": {} },
});
