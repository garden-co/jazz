// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Value = {
  readonly id: string;
  name: string;
  text: string | null;
  bytes: Uint8Array | null;
  control: boolean;
};

export type ValueInsert = {
  name: string;
  text?: string | null;
  bytes?: Uint8Array | null;
  control: boolean;
};

interface Types extends AppTypes {
  values: {
    row: Value;
    insert: ValueInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      text: { kind: "string"; nullable: true; value: string };
      bytes: { kind: "bytes"; nullable: true; value: Uint8Array };
      control: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:da121a2493c9bd78cc7e74800921e141484d2ffa31b5e0ab19c0bbd43d918645",
  "schema": {
    "current": "a0f628776bb379f7e6310397d01d538e6ff113af69387469f10f40937c817349",
    "schemas": [
      {
        "hash": "a0f628776bb379f7e6310397d01d538e6ff113af69387469f10f40937c817349",
        "schema": {
          "tables": {
            "values": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "text", "nullable": true },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": true },
                { "column_type": { "type": "Boolean" }, "name": "control", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "values": {} },
});
