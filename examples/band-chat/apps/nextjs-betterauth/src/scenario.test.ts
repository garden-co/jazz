import { expect, it } from "vitest";
import { createSmokeScenario } from "./scenario.js";

it("defines a deterministic public topology contract", () => {
  expect(createSmokeScenario("repeatable")).toEqual(createSmokeScenario("repeatable"));
  expect(createSmokeScenario().operations).toContain("reconnect-and-replay");
  expect(createSmokeScenario().faults).toContain("disconnect");
});
