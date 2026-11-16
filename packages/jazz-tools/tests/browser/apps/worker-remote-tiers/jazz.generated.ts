// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output f82d3aaa83da8e36. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Entry = {
  readonly id: string;
  title: string;
};

export type EntryInsert = {
  title: string;
};

interface Types extends AppTypes {
  entries: {
    row: Entry;
    insert: EntryInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:cb9ebe73de38d89b1179cbf795aef6236ae72497282c903bd0076aaf6facaed9",
  "schema": {
    "current": "cbb2a03497537465756764994d0be8d5603f7da19d7dc54f2406021013f6e073",
    "schemas": [
      {
        "hash": "cbb2a03497537465756764994d0be8d5603f7da19d7dc54f2406021013f6e073",
        "schema": {
          "tables": {
            "entries": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "entries": {} },
});
