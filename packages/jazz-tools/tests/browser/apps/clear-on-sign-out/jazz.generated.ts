// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Note = {
  readonly id: string;
  text: string;
};

export type NoteInsert = {
  text: string;
};

interface Types extends AppTypes {
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:7b9955c9ccc252ef508c66d3fc6ba620106ebcf14f089b6ef98a5d69e6fda1bc",
  "schema": {
    "current": "e0ccddc46dd79c94beedaed98cf426b5e3ce0273dc92a2f03cc32ec2a706fb53",
    "schemas": [
      {
        "hash": "e0ccddc46dd79c94beedaed98cf426b5e3ce0273dc92a2f03cc32ec2a706fb53",
        "schema": {
          "tables": {
            "notes": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "text", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "notes": {} },
});
