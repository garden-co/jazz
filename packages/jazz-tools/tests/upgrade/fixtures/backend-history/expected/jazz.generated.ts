// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Output db807275dadad423. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Note = {
  readonly id: string;
  title: string;
  body: string;
  pinned: boolean;
};

export type NoteInsert = {
  title: string;
  body: string;
  pinned?: boolean;
};

interface Types extends AppTypes {
  notes: {
    row: Note;
    insert: NoteInsert;
    columns: {
      title: { kind: "string"; nullable: false; value: string };
      body: { kind: "string"; nullable: false; value: string };
      pinned: { kind: "boolean"; nullable: false; value: boolean };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:771d8dc2507f1be9bc6fa078a7aa79c6bb26a4bed328149ad6594a7d543ce63f",
  "schema": {
    "current": "21cb7bdde685c2e1589ccb87479e5e300a2312be107c0538f1d6eeb8e06ea79a",
    "schemas": [
      {
        "hash": "ee20b38fcdbed9356975f88712758399be6daf7cd214f09cc430cc642e8a751f",
        "schema": {
          "tables": {
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
              ],
            },
          },
        },
      },
      {
        "hash": "21cb7bdde685c2e1589ccb87479e5e300a2312be107c0538f1d6eeb8e06ea79a",
        "schema": {
          "tables": {
            "notes": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "title", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "body", "nullable": false },
                {
                  "column_type": { "type": "Boolean" },
                  "default": { "type": "Boolean", "value": false },
                  "name": "pinned",
                  "nullable": false,
                },
              ],
            },
          },
        },
      },
    ],
    "migrations": [
      {
        "forward": [
          {
            "added": false,
            "operations": [
              {
                "column": "pinned",
                "column_type": { "type": "Boolean" },
                "type": "introduce",
                "value": { "type": "Boolean", "value": false },
              },
            ],
            "removed": false,
            "renamedFrom": null,
            "table": "notes",
          },
        ],
        "fromHash": "ee20b38fcdbed9356975f88712758399be6daf7cd214f09cc430cc642e8a751f",
        "toHash": "21cb7bdde685c2e1589ccb87479e5e300a2312be107c0538f1d6eeb8e06ea79a",
      },
    ],
  },
  "relations": { "notes": {} },
});
