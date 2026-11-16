// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 891ad886203f959e. Formatters may reformat this file, except `app` below.
import {
  bindApp,
  type AppTypes,
  type ComputedImpl,
  type JsonValue,
  type TransformImpl,
} from "jazz-tools";
import { computed, transforms } from "./jazz.computed.js";

export type Row = {
  readonly id: string;
  data: JsonValue;
  readonly first: JsonValue;
  readonly second: JsonValue;
  readonly repeated: JsonValue;
};

export type RowInsert = {
  data: JsonValue;
};

interface Types extends AppTypes {
  rows: {
    row: Row;
    insert: RowInsert;
    columns: {
      data: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    computed: {
      first: { from: { data: JsonValue }; value: JsonValue };
      second: { from: { data: JsonValue }; value: JsonValue };
      repeated: { from: { data: JsonValue }; value: JsonValue };
    };
    transforms: {
      data: "jsonText";
    };
  };
}

interface Transforms {
  jsonText: { stored: string; view: JsonValue };
}

export type AppComputed = ComputedImpl<Types>;
export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:06e1976a80508c3e8f5dffdfd454edaa7d7d271051c0c4f88b4f1b09669ea14f",
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
    "computed": { "rows": { "first": ["data"], "second": ["data"], "repeated": ["data"] } },
    "transforms": { "rows": { "data": "jsonText" } },
  },
  "relations": { "rows": {} },
  computed,
  transforms,
});
