// jazz.generated.ts: generated from jazz.app.json by `jazz-tools generate`. Do not edit.
// Bindings format 1. Formatters may reformat this file, except `app` below.
import { bindApp, type AppTypes } from "jazz-tools";

export type Result = {
  readonly id: string;
  tools: string;
  kotlin: number | null;
  kotlinx: boolean;
  java: number;
  query: string | null;
  object: string | null;
  due_at: Date | null;
};

export type ResultInsert = {
  tools: string;
  kotlin?: number | null;
  kotlinx: boolean;
  java: number;
  query?: string | null;
  object?: string | null;
  due_at?: Date | null;
};

export type Unit = {
  readonly id: string;
  tableName: string;
  result: string | null;
  tags: string[];
};

export type UnitInsert = {
  tableName: string;
  result?: string | null;
  tags: string[];
};

interface Types extends AppTypes {
  results: {
    row: Result;
    insert: ResultInsert;
    columns: {
      tools: { kind: "string"; nullable: false; value: string };
      kotlin: { kind: "int"; nullable: true; value: number };
      kotlinx: { kind: "boolean"; nullable: false; value: boolean };
      java: { kind: "float"; nullable: false; value: number };
      query: { kind: "string"; nullable: true; value: string };
      object: { kind: "string"; nullable: true; value: string };
      due_at: { kind: "timestamp"; nullable: true; value: Date };
    };
    relations: {};
  };
  units: {
    row: Unit;
    insert: UnitInsert;
    columns: {
      tableName: { kind: "string"; nullable: false; value: string };
      result: { kind: "uuid"; nullable: true; value: string };
      tags: { kind: "array"; nullable: false; value: string[] };
    };
    relations: {};
  };
}

// The compiled schema, as JSON that jazz-tools reads back: formatters leave it as it is.
// biome-ignore format: generated JSON
// prettier-ignore
export const app = bindApp<Types>({
  "source": "sha256:13613b0132c17b0e3b37a865eceb5d2c4ed6b8c77a6bda454869edfa3df26d4a",
  "schema": {
    "current": "538985d39b923218c5ec638dda0694def93063218141f7dafad50f246faf48d5",
    "schemas": [
      {
        "hash": "538985d39b923218c5ec638dda0694def93063218141f7dafad50f246faf48d5",
        "schema": {
          "tables": {
            "results": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "tools", "nullable": false },
                { "column_type": { "type": "Integer" }, "name": "kotlin", "nullable": true },
                { "column_type": { "type": "Boolean" }, "name": "kotlinx", "nullable": false },
                { "column_type": { "type": "Double" }, "name": "java", "nullable": false },
                { "column_type": { "type": "Text" }, "name": "query", "nullable": true },
                { "column_type": { "type": "Text" }, "name": "object", "nullable": true },
                { "column_type": { "type": "Timestamp" }, "name": "due_at", "nullable": true },
              ],
            },
            "units": {
              "columns": [
                { "column_type": { "type": "Text" }, "name": "tableName", "nullable": false },
                { "column_type": { "type": "Uuid" }, "name": "result", "nullable": true },
                {
                  "column_type": { "element": { "type": "Text" }, "type": "Array" },
                  "name": "tags",
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
  "relations": { "results": {}, "units": {} },
});
