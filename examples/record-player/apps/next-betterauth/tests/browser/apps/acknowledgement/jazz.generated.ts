// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Receipt = {
  readonly id: string;
  label: string;
};

export type ReceiptInsert = {
  label: string;
};

interface Types extends AppTypes {
  receipts: {
    row: Receipt;
    insert: ReceiptInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:1e86e83ea6942e430c349d8a4c9b40f3cd4292a22d7c1d9bf4cc7a0415f5ba15",
  "schema": {
    "current": "216777cb3a458533d1f502d3d852fe39a42969a43a151125c6f7a812d30b55e2",
    "schemas": [
      {
        "hash": "216777cb3a458533d1f502d3d852fe39a42969a43a151125c6f7a812d30b55e2",
        "schema": {
          "tables": {
            "receipts": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "label", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "receipts": {} },
});
