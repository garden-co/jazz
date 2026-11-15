// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Header = {
  readonly id: string;
  version: number;
};

export type HeaderInsert = {
  version: number;
};

export type Item = {
  readonly id: string;
  version: number;
};

export type ItemInsert = {
  version: number;
};

interface Types extends AppTypes {
  headers: {
    row: Header;
    insert: HeaderInsert;
    columns: {
      version: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
  };
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      version: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:5e24942fa64448136b52f69132402b8cfed645eeee5de5dababc52d1e81c5e52",
  "schema": {
    "current": "ca44bdbe82a79e4246684807c7fbd2c2d0847b06031db0513135e08286828b7a",
    "schemas": [
      {
        "hash": "ca44bdbe82a79e4246684807c7fbd2c2d0847b06031db0513135e08286828b7a",
        "schema": {
          "tables": {
            "headers": {
              "columns": [
                { "column_type": { "type": "Integer" }, "name": "version", "nullable": false },
              ],
            },
            "items": {
              "columns": [
                { "column_type": { "type": "Integer" }, "name": "version", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "headers": {}, "items": {} },
});
