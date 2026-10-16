import assert from "node:assert/strict";
import test from "node:test";
import { heroExamples } from "./catalogue.ts";
import { memoryCeilings } from "./field-metrics.ts";

const heroIds = new Set(heroExamples.map((example) => example.id));

test("memory ceilings belong to hero examples", () => {
  for (const id of Object.keys(memoryCeilings.byExample))
    assert.ok(heroIds.has(id), `${id} is not a hero example`);
});

test("the memory ceilings say when and on what build they were measured", () => {
  assert.match(memoryCeilings.measured, /^\d{4}-\d{2}-\d{2}$/);
  assert.ok(memoryCeilings.build, "build");
  assert.ok(memoryCeilings.method, "method");
});

test("memory ceilings are positive whole amounts in the app's own unit", () => {
  for (const [id, ceilings] of Object.entries(memoryCeilings.byExample)) {
    for (const ceiling of ceilings) {
      assert.ok(
        Number.isInteger(ceiling.ceiling) && ceiling.ceiling > 0,
        `${id}: ${ceiling.scenario}`,
      );
      assert.ok(ceiling.unit && ceiling.firstToCross, `${id}: ${ceiling.scenario}`);
    }
  }
});
