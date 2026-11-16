// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 36e8cd9e031aa376. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Document = {
  readonly id: string;
  title: string;
  done: boolean;
};

export type DocumentInsert = {
  title: string;
  done: boolean;
};

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
  "source": "sha256:ca1086691b82b20ca9e82a3dff19a1ba5a471bf29aaeed5ce50ae25d6d95abb4",
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
