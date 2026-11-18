import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { analyzeRelations } from "./relation-analyzer.js";
import type { ColumnDescriptor, ColumnType, WasmSchema } from "../drivers/types.js";

// `crates/jazz/tests/fixtures/relations_parity.json` says how each app's
// relations resolve; `crates/jazz/tests/relations_parity.rs` pins jazz's
// resolution (the loader's and the FFI's) to it. This runs the same apps
// through TS's `analyzeRelations`. A refused app may be refused by the
// loader (compiling it) or by the analyzer.

type Case = {
  description: string;
  tables: Record<string, unknown>;
  relations?: Record<string, unknown[]>;
  refused?: boolean;
};

const cases: Case[] = JSON.parse(
  readFileSync(
    new URL("../../../../crates/jazz/tests/fixtures/relations_parity.json", import.meta.url),
    "utf8",
  ),
);

function resolve(tables: Case["tables"]) {
  return Object.fromEntries(analyzeRelations(testApp({ tables } as never).wasmSchema));
}

// The fixture's column syntax, as far as it uses it, in WasmSchema form, so
// `analyzeRelations` runs on apps the loader would refuse first.
function columnDescriptor(name: string, spec: unknown): ColumnDescriptor {
  const scalar = (kind: string): ColumnType => {
    if (kind === "uuid") return { type: "Uuid" };
    if (kind === "string") return { type: "Text" };
    throw new Error(`fixture column kind ${kind}`);
  };
  if (typeof spec === "string") {
    const optional = spec.endsWith("?");
    return { name, column_type: scalar(spec.replace("?", "")), nullable: optional };
  }
  const object = spec as { type?: string; array?: string; optional?: boolean; references?: string };
  return {
    name,
    column_type: object.array
      ? { type: "Array", element: scalar(object.array) }
      : scalar(object.type!),
    nullable: object.optional === true,
    ...(object.references ? { references: object.references } : {}),
  };
}

function wasmSchema(tables: Case["tables"]): WasmSchema {
  const schema: Record<string, unknown> = {};
  for (const [table, spec] of Object.entries(tables)) {
    const { columns, relations } = spec as {
      columns: Record<string, unknown>;
      relations?: Record<
        string,
        { rel?: string; column?: string; reverse?: string; relation?: string }
      >;
    };
    const declared: Record<string, unknown> = {};
    for (const [name, relation] of Object.entries(relations ?? {})) {
      Object.defineProperty(declared, name, {
        enumerable: true,
        value: relation.rel
          ? { kind: "forward", table: relation.rel, column: relation.column }
          : { kind: "reverse", table: relation.reverse, relation: relation.relation },
      });
    }
    schema[table] = {
      columns: Object.entries(columns).map(([name, column]) => columnDescriptor(name, column)),
      relations: declared,
    };
  }
  return schema as unknown as WasmSchema;
}

describe("relations resolve as jazz resolves them, on the parity fixture", () => {
  for (const testCase of cases) {
    it(`${testCase.description} (through the loader)`, () => {
      if (testCase.refused) {
        expect(() => resolve(testCase.tables)).toThrow();
      } else {
        expect(resolve(testCase.tables)).toEqual(testCase.relations);
      }
    });
    // Directly, so TS's own refusals are pinned too: compiling through the
    // loader would refuse first.
    it(`${testCase.description} (analyzeRelations alone)`, () => {
      const analyze = () => Object.fromEntries(analyzeRelations(wasmSchema(testCase.tables)));
      if (testCase.refused) {
        expect(analyze).toThrow();
      } else {
        expect(analyze()).toEqual(testCase.relations);
      }
    });
  }
});
