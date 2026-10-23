import { afterAll, beforeAll, it } from "vitest";
import {
  createEqualityValuesFixture,
  runEqualityValueScenario,
} from "./fixtures/encrypted-equality-values.js";

let fixture: Awaited<ReturnType<typeof createEqualityValuesFixture>>;
beforeAll(async () => {
  fixture = await createEqualityValuesFixture();
}, 120_000);
afterAll(async () => {
  await fixture?.close();
});

it.each([
  "equality-negative-zero",
  "equality-array-zero",
  "equality-enum-zero",
  "equality-bigint",
  "equality-null",
  "equality-json",
  "equality-nonfinite",
  "equality-omitted-optional",
  "equality-composite-literals",
] as const)(
  "preserves encrypted equality semantics (%s)",
  (timing) => runEqualityValueScenario(timing, fixture),
  120_000,
);
