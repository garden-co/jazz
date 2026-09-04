import assert from "node:assert/strict";
import test from "node:test";
import { encodeResult, parseResult, result } from "./protocol.ts";
import { scenarioPlan, scenariosForAcceptancePhase } from "./scenarios.ts";

test("a TODO is machine-readable but is not a pass", () => {
  const value = result({
    protocol: 1,
    scenario: "local-write",
    state: "todo",
    detail: "native fixture pending",
  });
  assert.deepEqual(parseResult(encodeResult(value)), value);
});

test("passing requires an observed physical-platform receipt", () => {
  assert.throws(() =>
    result({ protocol: 1, scenario: "local-write", state: "passed", detail: "no receipt" }),
  );
});

test("device acceptance claims native relay lifecycle, A-to-B observation, and scope isolation", () => {
  const states = new Map(scenarioPlan.map((item) => [item.scenario, item.state]));
  assert.equal(states.get("linked-abi-admission"), "passed");
  assert.equal(states.get("foreground-byte-abi"), "passed");
  assert.equal(states.get("foreground-write-transaction"), "passed");
  assert.equal(states.get("local-write-subscription"), "passed");
  assert.equal(states.get("logout-revocation"), "passed");
  assert.equal(states.get("logout-auth-switch"), "passed");
  assert.equal(states.get("scope-isolation"), "passed");
  assert.equal(states.get("reopen"), "passed");
  assert.deepEqual(
    scenariosForAcceptancePhase("verify").map((scenario) => scenario.scenario),
    ["reopen"],
  );
  assert.equal(
    scenariosForAcceptancePhase("seed").some((scenario) => scenario.scenario === "reopen"),
    false,
  );
});
