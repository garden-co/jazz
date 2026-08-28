import assert from "node:assert/strict";
import test from "node:test";
import {
  boundedDiagnostic,
  parseLaunchProcessId,
  relevantAppLogs,
  sanitizedCommandFailure,
} from "./ios-diagnostics.mjs";

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

test("iOS launch parser accepts only the expected bundle and positive sole PID", () => {
  assert.equal(parseLaunchProcessId("dev.jazz.rndeviceacceptance: 4999"), 4999);
  for (const malformed of [
    "other.bundle: 4999",
    "dev.jazz.rndeviceacceptance: 0",
    "dev.jazz.rndeviceacceptance: 4999\nunexpected text",
    "4999",
  ]) {
    assert.throws(() => parseLaunchProcessId(malformed), /unexpected bundle\/process id/);
  }
});
