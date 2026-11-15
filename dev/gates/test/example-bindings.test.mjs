import test from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { checkBindings, exampleAppDirs, isRustCrate, root } from "../example-bindings.mjs";
import { planFor } from "../local-ci-equivalent.mjs";

test("finds every example app that commits a jazz.app.json, recipe apps included", () => {
  const dirs = exampleAppDirs();
  assert.ok(dirs.includes("examples/chat-react"));
  assert.ok(dirs.includes("examples/docs/todo-client-localfirst-react/session-app"));
  assert.ok(dirs.some((dir) => dir.startsWith("dev/stress-tests/")));
  assert.ok(dirs.length >= 60, `only ${dirs.length} apps found`);
});

test("reports each app whose generate --check fails, and skips Rust crates", () => {
  const scratch = mkdtempSync(join(tmpdir(), "example-bindings-"));
  try {
    for (const dir of ["fresh", "stale", "crate"]) mkdirSync(join(scratch, dir));
    writeFileSync(join(scratch, "crate", "Cargo.toml"), '[package]\nname = "x"\n');
    assert.equal(isRustCrate(join(scratch, "crate")), true);
    assert.equal(isRustCrate(join(scratch, "fresh")), false);
    const run = (dir) =>
      dir.endsWith("stale")
        ? { status: 1, stdout: "", stderr: "bindings out of date: run `jazz-tools generate`\n" }
        : { status: 0, stdout: "up to date\n", stderr: "" };
    const result = checkBindings(["fresh", "stale", "crate"], { cwd: scratch, run });
    assert.deepEqual(result, {
      failures: ["stale: bindings out of date: run `jazz-tools generate`"],
      skipped: ["crate"],
      checked: 2,
    });
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});

test("the TypeScript partition runs it, through the correctness consumer, after the jazz-tools build", () => {
  const labels = planFor({ partition: "typescript" }).map(({ label }) => label);
  const step = planFor({ partition: "typescript" }).find(
    ({ label }) => label === "example bindings",
  );
  assert.ok(step, "TypeScript partition must check example bindings");
  assert.deepEqual(step.args, [
    "dev/gates/run-correctness-consumer.mjs",
    "--",
    "node",
    "dev/gates/example-bindings.mjs",
  ]);
  assert.ok(labels.indexOf("example bindings") > labels.indexOf("TypeScript consumers"));
  assert.ok(root.length > 0);
});
