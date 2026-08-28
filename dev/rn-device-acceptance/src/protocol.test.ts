import assert from "node:assert/strict";
import test from "node:test";
import { encodeResult, parseResult, result } from "./protocol.ts";

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
