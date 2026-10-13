import assert from "node:assert/strict";
import test from "node:test";
import { heroExamples } from "./catalogue.ts";
import { loadTimes, memoryCeilings } from "./field-metrics.ts";

const heroIds = new Set(heroExamples.map((example) => example.id));

test("load times and memory ceilings belong to hero examples", () => {
  for (const id of [...Object.keys(loadTimes.byExample), ...Object.keys(memoryCeilings.byExample)])
    assert.ok(heroIds.has(id), `${id} is not a hero example`);
});

test("every measurement set says when and on what build it was measured", () => {
  for (const set of [loadTimes, memoryCeilings]) {
    if (!Object.keys(set.byExample).length) continue;
    assert.match(set.measured, /^\d{4}-\d{2}-\d{2}$/);
    assert.ok(set.build, "build");
    assert.ok(set.method, "method");
  }
});

test("load times are positive and scenarios are unique per example", () => {
  for (const [id, rows] of Object.entries(loadTimes.byExample)) {
    assert.equal(new Set(rows.map((row) => row.scenario)).size, rows.length, id);
    for (const row of rows) {
      assert.ok(row.localMs > 0, `${id}: ${row.scenario}`);
      assert.ok(row.rttMs === null || row.rttMs > 0, `${id}: ${row.scenario}`);
    }
  }
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
