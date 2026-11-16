// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 2df54fad0ddae1fb. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl, type TransformImpl } from "jazz-tools";
import { computed, transforms } from "./jazz.computed.js";

export type RowJson = { extra: string; title: string };

export type Row = {
  readonly id: string;
  text: string;
  json: RowJson;
  bytes: Uint8Array;
  readonly summary: string;
};

export type RowInsert = {
  text: string;
  json: RowJson;
  bytes: Uint8Array;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      text: { kind: "string"; nullable: false; value: string };
      json: { kind: "json"; nullable: false; value: RowJson };
      bytes: { kind: "bytes"; nullable: false; value: Uint8Array };
    };
    relations: {};
    computed: {
      summary: { from: { text: string; json: RowJson; bytes: Uint8Array }; value: string };
    };
    transforms: {
      text: "upperCase";
    };
  };
}

interface Transforms {
  upperCase: { stored: string; view: string };
}

export type AppComputed = ComputedImpl<Types>;
export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:157a941464674baaf3c07c38b46e51a0efeeb9a6f4699aebad7926e03799aac9",
  "schema": {
    "current": "2043ae6b237e5acde54bba47bc090a66e866c94ba8ecb9ebd9879933c765aec8",
    "schemas": [
      {
        "hash": "2043ae6b237e5acde54bba47bc090a66e866c94ba8ecb9ebd9879933c765aec8",
        "schema": {
          "tables": {
            "rows": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "text", "nullable": false },
                {
                  "column_type": {
                    "schema": {
                      "additionalProperties": false,
                      "properties": { "extra": { "type": "string" }, "title": { "type": "string" } },
                      "required": ["title", "extra"],
                      "type": "object",
                    },
                    "type": "Json",
                  },
                  "name": "json",
                  "nullable": false,
                },
                { "column_type": { "type": "Bytea" }, "name": "bytes", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "rows": { "summary": ["text", "json", "bytes"] } },
    "transforms": { "rows": { "text": "upperCase" } },
  },
  "relations": { "rows": {} },
  computed,
  transforms,
});
