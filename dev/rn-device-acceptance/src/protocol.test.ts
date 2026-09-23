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
  assert.equal(states.get("typing-composer"), "passed");
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

const deviceReceipt = {
  platform: "ios" as const,
  deviceIdentifier: "device",
  buildFingerprint: "a".repeat(64),
  runNonce: "nonce",
  sequence: 1,
  observedAt: new Date(0).toISOString(),
};

const composerMetrics = {
  keystrokes: 29,
  burst: 18,
  blockedP50Ms: 0.4,
  blockedP95Ms: 1.2,
  blockedMaxMs: 3,
  echoP50Ms: 8,
  echoP95Ms: 16,
  echoMaxMs: 20,
  burstEchoMs: 24,
  dropped: 0,
  reordered: 0,
  reappeared: 0,
};

test("a typing-composer pass requires its metrics and clean echo counters", () => {
  const base = {
    protocol: 1 as const,
    scenario: "typing-composer",
    state: "passed" as const,
    detail: "typing",
    receipt: deviceReceipt,
  };
  assert.throws(() => result(base), /without its device metrics/);
  assert.deepEqual(
    parseResult(encodeResult({ ...base, metrics: composerMetrics }))?.metrics,
    composerMetrics,
  );
  assert.throws(() => result({ ...base, metrics: { ...composerMetrics, reappeared: 1 } }));
  assert.throws(() => result({ ...base, metrics: { ...composerMetrics, echoP95Ms: -1 } }));
  const { burstEchoMs: _missing, ...incomplete } = composerMetrics;
  assert.throws(() => result({ ...base, metrics: incomplete }), /missing metric burstEchoMs/);
  assert.throws(() =>
    result({ protocol: 1, scenario: "x", state: "todo", detail: "d", metrics: { a: 1 } }),
  );
});
