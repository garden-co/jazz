// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 59a3c92c3b8acdbc. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type TransformImpl } from "jazz-tools";
import { transforms } from "./jazz.computed.js";

export type PriorityScore = "low" | "medium" | "high";

export type Priority = {
  readonly id: string;
  label: string;
  score: PriorityScore;
};

export type PriorityInsert = {
  label: string;
  score: PriorityScore;
};

interface Types extends AppTypes {
  priorities: {
    row: Priority;
    insert: PriorityInsert;
    columns: {
      label: { kind: "string"; nullable: false; value: string };
      score: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    transforms: {
      score: "priority";
    };
  };
}

interface Transforms {
  priority: { stored: number; view: "low" | "medium" | "high" };
}

export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:1c5433a138a6e8ad03a2b41ed06cabe1ba7e7ad43952cdb37c23fb4945ab145f",
  "schema": {
    "current": "c318690dbe0443d7094889b5290e488b6ddc3ab9394247ecea44b4bbc7fb67f6",
    "schemas": [
      {
        "hash": "c318690dbe0443d7094889b5290e488b6ddc3ab9394247ecea44b4bbc7fb67f6",
        "schema": {
          "tables": {
            "priorities": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "label", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "score", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "transforms": { "priorities": { "score": "priority" } },
  },
  "relations": { "priorities": {} },
  transforms,
});
