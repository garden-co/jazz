// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type History = {
  readonly id: string;
  title: string;
};

export type HistoryInsert = {
  title: string;
};

interface Types extends AppTypes {
  history: {
    row: History;
    insert: HistoryInsert;
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
  "source": "sha256:f97dc6c89ef3a71b19b9cdf66c60dd9ce61a959f3e2a1b78a3069321f9857dc6",
  "schema": {
    "current": "d943472ee312cb9b95abb2bdd44f40134502bcc4386a5a801201f0a4af721f80",
    "schemas": [
      {
        "hash": "d943472ee312cb9b95abb2bdd44f40134502bcc4386a5a801201f0a4af721f80",
        "schema": {
          "tables": {
            "history": {
              "columns": [{ "column_type": { "type": "Text" }, "name": "title", "nullable": false }],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "history": {} },
});
