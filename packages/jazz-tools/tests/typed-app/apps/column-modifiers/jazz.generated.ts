// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output a3a20bcbd19dd2a4. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type TransformImpl } from "jazz-tools";
import { transforms } from "./jazz.computed.js";

export type Item = {
  readonly id: string;
  count: number;
  tags: string[];
  transformed: number;
  optionalCount: number | null;
  defaultedCount: number;
  optionalDefaulted: number | null;
  transformedOptional: number | null;
  defaultedMerge: number;
};

export type ItemInsert = {
  count: number;
  tags: string[];
  transformed: number;
  optionalCount?: number | null;
  defaultedCount?: number;
  optionalDefaulted?: number | null;
  transformedOptional?: number | null;
  defaultedMerge?: number;
};

interface Types extends AppTypes {
  items: {
    row: Item;
    insert: ItemInsert;
    columns: {
      count: { kind: "int"; nullable: false; value: number };
      tags: { kind: "array"; nullable: false; value: string[] };
      transformed: { kind: "string"; nullable: false; value: string };
      optionalCount: { kind: "int"; nullable: true; value: number };
      defaultedCount: { kind: "int"; nullable: false; value: number };
      optionalDefaulted: { kind: "int"; nullable: true; value: number };
      transformedOptional: { kind: "string"; nullable: true; value: string };
      defaultedMerge: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    transforms: {
      transformed: "length";
      transformedOptional: "length";
    };
  };
}

interface Transforms {
  length: { stored: string; view: number };
}

export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:88ada474688c4a07b61aca197baeb0ddd0e7384b4cbbc8d8e69427affd32dfd5",
  "schema": {
    "current": "a696d597a7a5965067e17d888f855b26007c9cb1fd659602ae6a6e1f1490f986",
    "schemas": [
      {
        "hash": "a696d597a7a5965067e17d888f855b26007c9cb1fd659602ae6a6e1f1490f986",
        "schema": {
          "tables": {
            "items": {
              "columns": [
                {
                  "column_type": { "type": "Integer" },
                  "merge_strategy": "Counter",
                  "name": "count",
                  "nullable": false,
                },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "merge_strategy": "GSet",
                  "name": "tags",
                  "nullable": false,
                },
                { "column_type": { "type": "Text" }, "name": "transformed", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "optionalCount", "nullable": true },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 0 },
                  "name": "defaultedCount",
                  "nullable": false,
                },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Null" },
                  "name": "optionalDefaulted",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Text" },
                  "name": "transformedOptional",
                  "nullable": true,
                },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 0 },
                  "merge_strategy": "Counter",
                  "name": "defaultedMerge",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "transforms": { "items": { "transformed": "length", "transformedOptional": "length" } },
  },
  "relations": { "items": {} },
  transforms,
});
