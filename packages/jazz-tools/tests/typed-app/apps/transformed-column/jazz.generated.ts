// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 127fa46defeb2226. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type TransformImpl } from "jazz-tools";
import { transforms } from "./jazz.computed.js";

export type TaskUrgency = "low" | "high";

export type Task = {
  readonly id: string;
  title: string;
  urgency: TaskUrgency;
};

export type TaskInsert = {
  title: string;
  urgency: TaskUrgency;
};

interface Types extends AppTypes {
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      urgency: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    transforms: {
      urgency: "urgency";
    };
  };
}

interface Transforms {
  urgency: { stored: number; view: "low" | "high" };
}

export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:9d38a6996d942f2beceff573e8fdc24c21a806f8c9f8bdb208a9e0a8227dd2c3",
  "schema": {
    "current": "837542db92f1b739d3b22223f90561a794b3667c648c5a1778b3d6f5d0853a88",
    "schemas": [
      {
        "hash": "837542db92f1b739d3b22223f90561a794b3667c648c5a1778b3d6f5d0853a88",
        "schema": {
          "tables": {
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "urgency", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "transforms": { "tasks": { "urgency": "urgency" } },
  },
  "relations": { "tasks": {} },
  transforms,
});
