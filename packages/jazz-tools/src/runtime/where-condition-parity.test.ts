import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { translateQuery } from "./query-adapter.js";

// `crates/jazz/layers/model/tests/fixtures/where_condition_parity.json` says
// what each `{ column, op, value }` where condition means; the Rust decoder
// (`jazz_model::query::where_condition`, used by the FFI) is pinned to it by
// `crates/jazz/layers/model/tests/where_condition_parity.rs`. This runs the
// same cases through TS's lowering. `tsToday` records where TS still differs,
// until TS lowers conditions through the Rust decoder too.

type Meaning =
  | "refused"
  | { compare: string }
  | { isNull: boolean }
  | { in: number; negated: boolean }
  | { contains: true };

type Case = {
  description: string;
  condition: { column: string; op: string; value?: unknown };
  means: Meaning;
  tsToday?: Meaning;
};

const cases: Case[] = JSON.parse(
  readFileSync(
    new URL(
      "../../../../crates/jazz/layers/model/tests/fixtures/where_condition_parity.json",
      import.meta.url,
    ),
    "utf8",
  ),
);

const app = testApp({
  tables: {
    todos: {
      columns: {
        title: "string",
        priority: "int",
        doneAt: "timestamp?",
        tags: { array: "string" },
      },
    },
  },
});

const COMPARE: Record<string, string> = {
  Eq: "eq",
  Ne: "ne",
  Lt: "lt",
  Le: "lte",
  Gt: "gt",
  Ge: "gte",
};

function meaning(predicate: Record<string, any>): Meaning {
  if (predicate.Cmp) return { compare: COMPARE[predicate.Cmp.op]! };
  if (predicate.IsNull) return { isNull: true };
  if (predicate.IsNotNull) return { isNull: false };
  if (predicate.In) return { in: predicate.In.values.length, negated: false };
  if (predicate.Not?.In) return { in: predicate.Not.In.values.length, negated: true };
  if (predicate.Contains) return { contains: true };
  throw new Error(`unexpected predicate ${JSON.stringify(predicate)}`);
}

function lower(condition: Case["condition"]): Meaning {
  const built = JSON.parse(app.todos.where({})._build());
  built.conditions = [condition];
  try {
    return meaning(JSON.parse(translateQuery(JSON.stringify(built), app.wasmSchema)).conditions[0]);
  } catch {
    return "refused";
  }
}

describe("where conditions match the Rust decoder on the parity fixture", () => {
  for (const testCase of cases) {
    it(testCase.description, () => {
      expect(lower(testCase.condition)).toEqual(testCase.tsToday ?? testCase.means);
    });
  }
});
