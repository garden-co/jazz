// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 1b9c69be1e36f070. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Row = {
  readonly id: string;
  data: string;
};

export type RowInsert = {
  data: string;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      data: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:b4bf6861ea6c931d7cc2345981a54adf220dbe32cf512ae45fbdf5a0caaf0985",
  "schema": {
    "current": "97730bad8c4c1a6e1114aee523d76efc7bd3537c4f8d552a89aa5dc59a290c35",
    "schemas": [
      {
        "hash": "97730bad8c4c1a6e1114aee523d76efc7bd3537c4f8d552a89aa5dc59a290c35",
        "schema": {
          "tables": {
            "rows": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "data", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "rows": {} },
});
