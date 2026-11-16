// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Place = {
  readonly id: string;
  name: string;
  location: readonly [lng: number, lat: number];
  pickup: readonly [lng: number, lat: number] | null;
  home: readonly [lng: number, lat: number];
};

export type PlaceInsert = {
  name: string;
  location: readonly [lng: number, lat: number];
  pickup?: readonly [lng: number, lat: number] | null;
  home?: readonly [lng: number, lat: number];
};

interface Types extends AppTypes {
  places: {
    row: Place;
    insert: PlaceInsert;
    columns: {
      name: { kind: "string"; nullable: false; value: string };
      location: { kind: "point"; nullable: false; value: readonly [lng: number, lat: number] };
      pickup: { kind: "point"; nullable: true; value: readonly [lng: number, lat: number] };
      home: { kind: "point"; nullable: false; value: readonly [lng: number, lat: number] };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:8f7a074feb1ed77724a88737165f527151b3702119575bf88728f029cd6c1c46",
  "schema": {
    "current": "f319a0983dae6f359ab20a13538c538589758e056d948d2dd33cdfc70250aeab",
    "schemas": [
      {
        "hash": "f319a0983dae6f359ab20a13538c538589758e056d948d2dd33cdfc70250aeab",
        "schema": {
          "tables": {
            "places": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "name", "nullable": false },
                { "column_type": { "type": "Point" }, "name": "location", "nullable": false },
                { "column_type": { "type": "Point" }, "name": "pickup", "nullable": true },
                {
                  "column_type": { "type": "Point" },
                  "default": {
                    "type": "Bytea",
                    "value": [143, 194, 245, 40, 92, 207, 42, 64, 195, 245, 40, 92, 143, 66, 74, 64],
                  },
                  "name": "home",
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
  "relations": { "places": {} },
});
