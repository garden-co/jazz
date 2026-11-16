// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 3038c8c4ffcc2ba4. Formatters may reformat this file, except `app` below.
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
  "source": "sha256:618ae1dea75800d8458de9f63adc421ae60060638b671c5082944241e7fd3212",
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
