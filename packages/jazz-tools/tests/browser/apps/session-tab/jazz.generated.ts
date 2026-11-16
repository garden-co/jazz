// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Task = {
  readonly id: string;
  title: string;
};

export type TaskInsert = {
  title: string;
};

interface Types extends AppTypes {
  tasks: {
    row: Task;
    insert: TaskInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:825319819dc8a916d461c218b4c938ddd41d15e48e3f7b6fe43b1a3dc44a13d7",
  "schema": {
    "current": "d59d2bdfb74fa267ee17d383f1ea01cf47da4b520b991c90e99510e6a0ccad30",
    "schemas": [
      {
        "hash": "d59d2bdfb74fa267ee17d383f1ea01cf47da4b520b991c90e99510e6a0ccad30",
        "schema": {
          "tables": {
            "tasks": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "tasks": {} },
});
