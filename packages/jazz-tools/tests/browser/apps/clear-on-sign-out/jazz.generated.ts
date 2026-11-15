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
  "source": "sha256:c355c6307fdbd9dd3a011d2e07e67681bce994fe2f77ff772aa4644d6e60e937",
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
