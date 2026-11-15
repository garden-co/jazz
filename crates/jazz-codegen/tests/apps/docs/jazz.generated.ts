// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Doc = {
  readonly id: string;
  title: string;
  embedding: Float32Array;
  draftEmbedding: Float32Array | null;
  seed: Float32Array;
  lat: number;
  lng: number;
};

export type DocInsert = {
  title: string;
  embedding: Float32Array;
  draftEmbedding?: Float32Array | null;
  seed?: Float32Array;
  lat: number;
  lng: number;
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
      lat: { kind: "float"; nullable: false; value: number };
      lng: { kind: "float"; nullable: false; value: number };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:ee3d8a7a8b4eb56096ec2e0e9c548336b83baeec02f60a17bbe9ebc46c98c774",
  "schema": {
    "current": "29859e00ecde59703bf20a35c66f14bc487c2453fe5ef8e804743c21c77f07fe",
    "schemas": [
      {
        "hash": "29859e00ecde59703bf20a35c66f14bc487c2453fe5ef8e804743c21c77f07fe",
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
                { "column_type": { "type": "Double" }, "name": "lat", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "lng", "nullable": false },
              ],
              "specialised_indexes": [
                { "kind": { "geo": { "lat": "lat", "lng": "lng" } }, "name": "place" },
                {
                  "kind": {
                    "text": {
                      "analyzer": "jazz-text/1:en",
                      "fields": { "title": 1 },
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
