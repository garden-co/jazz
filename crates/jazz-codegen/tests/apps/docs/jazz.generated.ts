// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Doc = {
  readonly id: string;
  title: string;
  embedding: Float32Array;
  draftEmbedding: Float32Array | null;
  seed: Float32Array;
};

export type DocInsert = {
  title: string;
  embedding: Float32Array;
  draftEmbedding?: Float32Array | null;
  seed?: Float32Array;
};

interface Types extends AppTypes {
  docs: {
    row: Doc;
    insert: DocInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      embedding: { kind: "vector"; nullable: false; value: Float32Array };
      draftEmbedding: { kind: "vector"; nullable: true; value: Float32Array };
      seed: { kind: "vector"; nullable: false; value: Float32Array };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:c96729bc448df827ab477792768535d62c0557ff8b31c0c55b23e9f794310e48",
  "schema": {
    "current": "c55a2568df46f558e80d9e715d502a3c0356470c4ec2653d3afb2f34000f038a",
    "schemas": [
      {
        "hash": "c55a2568df46f558e80d9e715d502a3c0356470c4ec2653d3afb2f34000f038a",
        "schema": {
          "tables": {
            "docs": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                {
                  "column_type": { "dimensions": 4, "type": "Vector" },
                  "name": "embedding",
                  "nullable": false,
                },
                {
                  "column_type": { "dimensions": 2, "type": "Vector" },
                  "name": "draftEmbedding",
                  "nullable": true,
                },
                {
                  "column_type": { "dimensions": 2, "type": "Vector" },
                  "default": { "type": "Bytea", "value": [0, 0, 0, 63, 0, 0, 128, 191] },
                  "name": "seed",
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
  "relations": { "docs": {} },
});
