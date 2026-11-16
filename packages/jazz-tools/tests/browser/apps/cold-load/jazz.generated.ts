// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output de71e36f7b50a1dd. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Task = {
  readonly id: string;
  title: string;
  done: boolean;
};

export type TaskInsert = {
  title: string;
  done: boolean;
};

interface Types extends AppTypes {
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      done: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:eaef8cfda3f4116284e8d3b1676f249075f699ad0cade4d6a3415410177e16a4",
  "schema": {
    "current": "75c940683c22c2fcc3c38afbe5ae9da38982cfee58dcb079b153a6be9ff55a6a",
    "schemas": [
      {
        "hash": "75c940683c22c2fcc3c38afbe5ae9da38982cfee58dcb079b153a6be9ff55a6a",
        "schema": {
          "tables": {
            "tasks": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Boolean" }, "name": "done", "nullable": false },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "tasks": {} },
});
