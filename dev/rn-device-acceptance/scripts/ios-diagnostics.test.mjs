import assert from "node:assert/strict";
import test from "node:test";
import { boundedDiagnostic, relevantAppLogs, sanitizedCommandFailure } from "./ios-diagnostics.mjs";

test("iOS diagnostics exclude unrelated logs and cap oversized app output", () => {
  const output = [
    "2026 unrelated-process sensitive-looking-token=do-not-report",
    ...Array.from(
      { length: 200 },
      (_, index) => `JazzRNdeviceacceptance line ${index} ${"x".repeat(256)}`,
    ),
  ].join("\n");
  const diagnostic = relevantAppLogs(output, "JazzRNdeviceacceptance");
  assert.doesNotMatch(diagnostic, /sensitive-looking-token/);
  assert.match(diagnostic, /JazzRNdeviceacceptance line 0/);
  assert.ok(diagnostic.split("\n").length <= 120);
  assert.ok(Buffer.byteLength(boundedDiagnostic("x".repeat(20_000))) <= 16 * 1024 + 32);
});

test("iOS diagnostics do not echo raw command errors", () => {
  const diagnostic = sanitizedCommandFailure({ status: 70, message: "secret simulator output" });
  assert.equal(diagnostic, "command failed (exit 70)");
  assert.doesNotMatch(diagnostic, /secret/);
});
