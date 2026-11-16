// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Doc = {
  readonly id: string;
  label: string;
  body: string;
};

export type DocInsert = {
  label: string;
  body: string;
};

interface Types extends AppTypes {
  docs: {
    row: Doc;
    insert: DocInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:61e32a55de4f18e692f247c02b985c48ffe15bc84ad985d53954908df08a0588",
  "schema": {
    "current": "0654ec1b2bee5aa6de53a44f7d92fd79a1eca059a50f7c5205ff6174c70c4377",
    "schemas": [
      {
        "hash": "0654ec1b2bee5aa6de53a44f7d92fd79a1eca059a50f7c5205ff6174c70c4377",
        "schema": {
          "tables": {
            "docs": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "docs": {} },
});
