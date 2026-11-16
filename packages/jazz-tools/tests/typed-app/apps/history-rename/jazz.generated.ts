// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Note = {
  readonly id: string;
  heading: string;
  category: string;
};

export type NoteInsert = {
  heading: string;
  category: string;
};

interface Types extends AppTypes {
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      heading: { kind: "string"; nullable: false; value: string };
      category: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:adad192cef7270ab68e20795b5861eacb3273b37ec1cf2e12281d17bc20a4aae",
  "schema": {
    "current": "f114d7fc25c3c85e60a2fc63db18a8f141e6717c4f39ca9dde9e6f62ca0bda20",
    "schemas": [
      {
        "hash": "c4ce836e6d15b5b641b9dbfc2dcd48f2e29187398b2e3213c3522c53c126f5cb",
        "schema": {
          "tables": {
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "legacy", "nullable": false },
              ],
            },
          },
        },
      },
      {
        "hash": "f114d7fc25c3c85e60a2fc63db18a8f141e6717c4f39ca9dde9e6f62ca0bda20",
        "schema": {
          "tables": {
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "heading", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "category", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              { "column": "title", "type": "rename", "value": "heading" },
              {
                "column": "category",
                "column_type": { "type": "Text" },
                "type": "introduce",
                "value": { "type": "Text", "value": "uncategorized" },
              },
              {
                "column": "legacy",
                "column_type": { "type": "Text" },
                "type": "drop",
                "value": { "type": "Text", "value": "" },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "notes",
          },
        ],
        "fromHash": "c4ce836e6d15b5b641b9dbfc2dcd48f2e29187398b2e3213c3522c53c126f5cb",
        "toHash": "f114d7fc25c3c85e60a2fc63db18a8f141e6717c4f39ca9dde9e6f62ca0bda20",
      },
    ],
  },
  "relations": { "notes": {} },
});
