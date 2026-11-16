import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { analyzeRelations } from "./relation-analyzer.js";

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

describe("relations resolve as jazz resolves them, on the parity fixture", () => {
  for (const testCase of cases) {
    it(testCase.description, () => {
      if (testCase.refused) {
        expect(() => resolve(testCase.tables)).toThrow();
      } else {
        expect(resolve(testCase.tables)).toEqual(testCase.relations);
      }
    });
  }
});
