// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes, type ComputedImpl } from "jazz-tools";
import { computed } from "./jazz.computed.js";

export type TaskPriority = "low" | "medium" | "high";

export type Task = {
  readonly id: string;
  title: string;
  score: number;
  readonly priority: TaskPriority;
};

export type TaskInsert = {
  title: string;
  score: number;
};

interface Types extends AppTypes {
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      score: { kind: "int"; nullable: false; value: number };
    };
    relations: {};
    computed: {
      priority: { from: { score: number }; value: TaskPriority };
    };
  };
}

export type AppComputed = ComputedImpl<Types>;

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:93dbb98d2f447d6d10493e2c1da12fd1383244c501c52b25504eee17a616c626",
  "schema": {
    "current": "df7dc34b179aa08c550bec12308bb6e4f9bdeaed016cd896db4dc156eeed0145",
    "schemas": [
      {
        "hash": "df7dc34b179aa08c550bec12308bb6e4f9bdeaed016cd896db4dc156eeed0145",
        "schema": {
          "tables": {
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "score", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
    "computed": { "tasks": { "priority": ["score"] } },
  },
  "relations": { "tasks": {} },
  computed,
});
