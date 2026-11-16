// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Doc = {
  readonly id: string;
  title: string;
  embedding: Float32Array;
  draftEmbedding: Float32Array | null;
  seed: Float32Array;
  location: readonly [lng: number, lat: number];
};

export type DocInsert = {
  title: string;
  embedding: Float32Array;
  draftEmbedding?: Float32Array | null;
  seed?: Float32Array;
  location: readonly [lng: number, lat: number];
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
      location: { kind: "point"; nullable: false; value: readonly [lng: number, lat: number] };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:2128c3ead5e740a6d6d1b666b5d4b003ced8db3913095d22314f27de9fa9ec0c",
  "schema": {
    "current": "55b77389ffee0abda9f1eb4e4940ab9c220cda1d01fe8c1114a16051376b4b6b",
    "schemas": [
      {
        "hash": "55b77389ffee0abda9f1eb4e4940ab9c220cda1d01fe8c1114a16051376b4b6b",
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
                { "column_type": { "type": "Point" }, "name": "location", "nullable": false },
              ],
              "specialised_indexes": [
                { "kind": { "geo": { "column": "location" } }, "name": "place" },
                {
                  "kind": {
                    "text": {
                      "analyzer": "jazz-text/1:en",
                      "fields": { "title": { "length": 32, "weight": 1 } },
                      "prefix": true,
                    },
                  },
                  "name": "search",
                },
                {
                  "kind": {
                    "vector": { "ann": "exact", "column": "embedding", "metric": "cosine" },
                  },
                  "name": "similar",
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
