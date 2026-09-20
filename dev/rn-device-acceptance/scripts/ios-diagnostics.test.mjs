import assert from "node:assert/strict";
import test from "node:test";
import {
  boundedDiagnostic,
  scopeMembershipDiagnostic,
  foregroundWakeDiagnostic,
  parseLaunchProcessId,
  relevantAppLogs,
  safeDeviceDiagnostic,
  sanitizedCommandFailure,
} from "./ios-diagnostics.mjs";

test("bounded diagnostics retain the newest stage when callers request a tail", () => {
  const lines = Array.from({ length: 130 }, (_, index) => `stage-${index}`).join("\n");
  const output = boundedDiagnostic(lines, { tail: true });
  assert.doesNotMatch(output, /stage-0/);
  assert.match(output, /stage-129/);
});

test("iOS foreground wake diagnostics retain only fixed stages from a narrow query", () => {
  assert.equal(
    foregroundWakeDiagnostic(
      "JazzRNdeviceacceptance JazzForegroundWake armed\nJazzRNdeviceacceptance JazzForegroundWake requested\nJazzRNdeviceacceptance JazzForegroundWake delivered",
    ),
    "requested,delivered",
  );
  assert.equal(foregroundWakeDiagnostic("JazzForegroundWake armed"), "armed-no-wake");
  assert.equal(
    foregroundWakeDiagnostic(
      "JazzForegroundWake armed\nJazzForegroundWake enabled\nJazzForegroundWake foreground-mismatch",
    ),
    "enabled,foreground-mismatch",
  );
  assert.equal(foregroundWakeDiagnostic("unrelated secret=never-printed"), "no-arm");
});

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
  assert.doesNotMatch(diagnostic, /JazzRNdeviceacceptance line 0/);
  assert.match(diagnostic, /JazzRNdeviceacceptance line 199/);
  assert.ok(diagnostic.split("\n").length <= 120);
  assert.ok(Buffer.byteLength(boundedDiagnostic("x".repeat(20_000))) <= 16 * 1024 + 32);
});

test("iOS diagnostics do not echo raw command errors", () => {
  const diagnostic = sanitizedCommandFailure({ status: 70, message: "secret simulator output" });
  assert.equal(diagnostic, "command failed (exit 70)");
  assert.doesNotMatch(diagnostic, /secret/);
});

test("device diagnostic printing is an allowlist, not bounded arbitrary text", () => {
  assert.equal(safeDeviceDiagnostic("native-admission-failed\n"), "native-admission-failed");
  const plantedSecret = "capability=secret-device-token";
  const printed = safeDeviceDiagnostic(plantedSecret);
  assert.equal(printed, "[no recognized device diagnostic]");
  assert.doesNotMatch(printed, /secret-device-token/);
});

test("iOS launch parser accepts only the expected bundle and positive sole PID", () => {
  assert.equal(parseLaunchProcessId("dev.jazz.rndeviceacceptance: 4999"), 4999);
  assert.equal(parseLaunchProcessId("dev.jazz.rndeviceacceptance: 4999\n"), 4999);
  assert.equal(parseLaunchProcessId("dev.jazz.rndeviceacceptance: 4999\r\n"), 4999);
  for (const malformed of [
    "other.bundle: 4999",
    "devXjazzYrndeviceacceptance: 4999",
    "dev-jazz-rndeviceacceptance: 4999",
    " dev.jazz.rndeviceacceptance: 4999",
    "dev.jazz.rndeviceacceptance: 4999 ",
    "dev.jazz.rndeviceacceptance: 0",
    "dev.jazz.rndeviceacceptance: -1",
    "dev.jazz.rndeviceacceptance: 9007199254740992",
    "dev.jazz.rndeviceacceptance: 4999\n\n",
    "dev.jazz.rndeviceacceptance: 4999\r",
    "dev.jazz.rndeviceacceptance: 4999\nunexpected text",
    "4999",
  ]) {
    assert.throws(() => parseLaunchProcessId(malformed), /unexpected bundle\/process id/);
  }
});

test("scope writer counters are exact and reject arbitrary or trailing data", async () => {
  const { scopeWriterReadDiagnostic } = await import("./ios-diagnostics.mjs");
  const detail =
    "scope-isolation-writer-read-detail:last-pending-wakes-2-polls-1-row-responses-0-ready-no";
  assert.equal(scopeWriterReadDiagnostic(detail), detail);
  for (const invalid of [
    "secret",
    `${detail}\n`,
    `${detail}-secret`,
    detail.replace("wakes-2", "wakes-1234567"),
  ])
    assert.equal(
      scopeWriterReadDiagnostic(invalid),
      "[no recognized scope writer read diagnostic]",
    );
});

test("scope rejection category sanitizers agree across hosts and reject raw details", async () => {
  const { scopeWriterReadDiagnostic } = await import("./ios-diagnostics.mjs");
  const { androidScopeWriterReadDiagnostic } = await import("./android-diagnostics.mjs");
  const prefix =
    "scope-isolation-writer-read-detail:last-rejected-wakes-0-polls-0-row-responses-0-ready-no-reason-";
  for (const category of [
    "unsupported-shape",
    "catalogue-pending",
    "table-not-found",
    "schema-resolution",
    "query-validation",
    "query-lowering",
    "policy-evaluation",
    "internal",
    "invalid-authority-closure",
    "closure-revision",
    "closure-coordinate",
    "closure-opening-data",
    "closure-retired-members",
    "closure-row-version",
    "closure-outside-scope",
    "closure-duplicate-add",
    "closure-absent-remove",
    "closure-unwitnessed-body",
    "closure-payload-bundle-run",
    "closure-payload-count",
    "closure-payload-durability",
    "closure-payload-schema",
    "closure-payload-table",
    "closure-payload-descriptor",
    "closure-payload-hlc",
    "closure-payload-branch",
    "closure-payload-receipt",
    "closure-payload-coordinates",

    "unknown",
  ]) {
    const detail = prefix + category;
    assert.equal(scopeWriterReadDiagnostic(detail), detail);
    assert.equal(
      androidScopeWriterReadDiagnostic(
        `08-29 22:52:21.495 4268 4288 E JazzScopeWriterRead: ${detail}`,
      ),
      detail,
    );
  }
  for (const category of [
    "private-fixture-detail",
    "internal-private-fixture-detail",
    "internal\nprivate-fixture-detail",
  ]) {
    assert.equal(
      scopeWriterReadDiagnostic(prefix + category),
      "[no recognized scope writer read diagnostic]",
    );
    // Android logs are line-oriented; appended detail on the same line must fail closed.
    if (!category.includes("\n"))
      assert.equal(
        androidScopeWriterReadDiagnostic(
          `08-29 22:52:21.495 4268 4288 E JazzScopeWriterRead: ${prefix}${category}`,
        ),
        undefined,
      );
  }
});

test("scope membership stderr accepts only bounded structural records", () => {
  const record =
    "JAZZ_SCOPE_MEMBERSHIP tables=1 name_in_scope=true physical_in_schema=false name_matches_physical=false cached=true scoped_relay=false";
  assert.equal(scopeMembershipDiagnostic(`private raw detail\n${record}\nunknown=value`), record);
  for (const invalid of [
    record + " private",
    record.replace("tables=1", "tables=1000000"),
    record.replace("true", "secret"),
    "arbitrary private text",
  ]) {
    assert.equal(scopeMembershipDiagnostic(invalid), "[no recognized scope membership diagnostic]");
  }
  assert.equal(scopeMembershipDiagnostic(Array(20).fill(record).join("\n")).split("\n").length, 16);
});
