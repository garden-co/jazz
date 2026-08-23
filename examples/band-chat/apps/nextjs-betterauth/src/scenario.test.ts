import { expect, it } from "vitest";
import { createSmokeScenario } from "./scenario.js";

it("defines a deterministic public smoke contract", () => {
  expect(createSmokeScenario("repeatable")).toEqual(createSmokeScenario("repeatable"));
  expect(createSmokeScenario().operations).toContain("send-message");
});
