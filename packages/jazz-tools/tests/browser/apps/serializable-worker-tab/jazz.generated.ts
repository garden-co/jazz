// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Audit = {
  readonly id: string;
  note: string;
};

export type AuditInsert = {
  note: string;
};

export type Seat = {
  readonly id: string;
  show: string;
  holder: string;
};

export type SeatInsert = {
  show: string;
  holder: string;
};

interface Types extends AppTypes {
  audit: {
    row: Audit;
    insert: AuditInsert;
    columns: {
      note: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
  seats: {
    row: Seat;
    insert: SeatInsert;
    columns: {
      show: { kind: "string"; nullable: false; value: string };
      holder: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:bb5a88da3e2ba97f1d76a890c55c23cc3120ab6162dc135ce03fc549331cbfd4",
  "schema": {
    "current": "89a62ce10cd70e15ea0569c54f82528bef28cd8fbf7b53e177b8d1437ed48ffb",
    "schemas": [
      {
        "hash": "89a62ce10cd70e15ea0569c54f82528bef28cd8fbf7b53e177b8d1437ed48ffb",
        "schema": {
          "tables": {
            "audit": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "note", "nullable": false }],
            },
            "seats": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "show", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "holder", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "audit": {}, "seats": {} },
});
