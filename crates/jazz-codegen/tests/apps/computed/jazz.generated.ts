// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl, type TransformImpl } from "jazz-tools";
import { computed, transforms } from "./jazz.computed.js";

export type TaskPriority = "low" | "high";

export type Person = {
  readonly id: string;
  name: string;
  email: string;
};

export type PersonInsert = {
  name: string;
  email: string;
};

export type Task = {
  readonly id: string;
  title: string;
  score: number;
  dueAt: string | null;
  readonly priority: TaskPriority;
  readonly label: string | null;
};

export type TaskInsert = {
  title: string;
  score?: number;
  dueAt?: string | null;
};

interface Types extends AppTypes {
  people: {
    row: Person;
    insert: PersonInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      email: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
    transforms: {
      email: "lowercase";
    };
  };
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      score: { kind: "int"; nullable: false; value: number };
      dueAt: { kind: "timestamp"; nullable: true; value: Date };
    };
    relations: {};
    computed: {
      priority: { from: { score: number }; value: TaskPriority };
      label: { from: { title: string; dueAt: string | null }; value: string | null };
    };
    transforms: {
      dueAt: "isoDate";
    };
  };
}

interface Transforms {
  isoDate: { stored: Date; view: string };
  lowercase: { stored: string; view: string };
}

export type AppComputed = ComputedImpl<Types>;
export type AppTransforms = TransformImpl<Transforms, Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types, Transforms>({
  "source": "sha256:4587bb8b3c405a2df2cdfaa9ad19e3bcaf0d60aa8f66a474f1d2952f750ee113",
  "schema": {
    "current": "32783f3c823d66cf74576ea34152eb8be00330bd70e4dddb550ff2c8bdb5eca3",
    "schemas": [
      {
        "hash": "32783f3c823d66cf74576ea34152eb8be00330bd70e4dddb550ff2c8bdb5eca3",
        "schema": {
          "tables": {
            "people": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "email", "nullable": false },
              ],
            },
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "type": "Integer" },
                  "default": { "type": "Integer", "value": 0 },
                  "name": "score",
                  "nullable": false,
                },
                { "column_type": { "type": "Timestamp" }, "name": "dueAt", "nullable": true },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "tasks": { "priority": ["score"], "label": ["title", "dueAt"] } },
    "transforms": { "people": { "email": "lowercase" }, "tasks": { "dueAt": "isoDate" } },
  },
  "relations": { "people": {}, "tasks": {} },
  computed,
  transforms,
});
