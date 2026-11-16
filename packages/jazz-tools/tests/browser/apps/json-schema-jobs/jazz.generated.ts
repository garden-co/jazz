// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output 2c95164581ad061a. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type JobMeta = { code: string; count?: number; [key: string]: unknown };

export type Job = {
  readonly id: string;
  meta: JobMeta;
};

export type JobInsert = {
  meta: JobMeta;
};

interface Types extends AppTypes {
  jobs: {
    row: Job;
    insert: JobInsert;
    columns: {
      meta: { kind: "json"; nullable: false; value: JobMeta };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:a07a08378d6b6b4b9ab73ec35d6c1789534f26a0d979fe8efcd68229aa3a562b",
  "schema": {
    "current": "75a3483584d9f6ba4c24f1ff5107cfc93f862b323a89d9211c86cac551a33ed1",
    "schemas": [
      {
        "hash": "75a3483584d9f6ba4c24f1ff5107cfc93f862b323a89d9211c86cac551a33ed1",
        "schema": {
          "tables": {
            "jobs": {
              "columns": [
                {
                  "column_type": {
                    "schema": {
                      "properties": {
                        "code": { "pattern": "^[a-z]+\\-[0-9]+$", "type": "string" },
                        "count": { "minimum": 0, "type": "integer" },
                      },
                      "required": ["code"],
                      "type": "object",
                    },
                    "type": "Json",
                  },
                  "name": "meta",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [],
  },
  "relations": { "jobs": {} },
});
