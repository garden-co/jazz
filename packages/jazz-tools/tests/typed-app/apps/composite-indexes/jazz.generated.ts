// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Document = {
  readonly id: string;
  ownerId: string;
  updatedAt: Date;
};

export type DocumentInsert = {
  ownerId: string;
  updatedAt: Date;
};

interface Types extends AppTypes {
  documents: {
    row: Document;
    insert: DocumentInsert;
    columns: {
      ownerId: { kind: "uuid"; nullable: false; value: string };
      updatedAt: { kind: "timestamp"; nullable: false; value: Date };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:29383a6797b3f2b37c8b0329b38e28ea4cbeee7a58bf64d5075986bd65fc0f94",
  "schema": {
    "current": "4da08798e3ca504947a494ff6274d6a1955f8217e5a86c7ae6ca05dd33caae10",
    "schemas": [
      {
        "hash": "4da08798e3ca504947a494ff6274d6a1955f8217e5a86c7ae6ca05dd33caae10",
        "schema": {
          "tables": {
            "documents": {
              "columns": [
                { "column_type": { "type": "Uuid" }, "name": "ownerId", "nullable": false },
                { "column_type": { "type": "Timestamp" }, "name": "updatedAt", "nullable": false },
              ],
              "composite_indexes": [["ownerId", "updatedAt"]],
              "indexed_columns": ["ownerId"],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "documents": {} },
});
