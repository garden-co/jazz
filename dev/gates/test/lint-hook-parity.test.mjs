import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

// The pre-commit hook must not disagree with CI's lint partition (`pnpm lint`):
// a commit the hook accepts should not fail CI's lint, and the hook should not
// block or rewrite code that CI accepts.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const hooks = parse(readFileSync(path.join(root, "lefthook.yml"), "utf8"))["pre-commit"].commands;
const scripts = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8")).scripts;

test("the clippy hook runs CI's exact Rust lint command", () => {
  // Cargo unifies features over the selected packages, so `--package <crate>`
  // compiles a different crate than `--workspace` does. Only the identical
  // command gives identical results.
  assert.match(scripts.lint, /(^|&& )pnpm lint:rust$/);
  assert.equal(hooks.clippy.run, scripts["lint:rust"]);
});

test("the oxlint hook uses CI's ignore file and no ignore list of its own", () => {
  assert.match(scripts.lint, /^oxlint \. --ignore-path \.oxlintignore /);
  assert.match(hooks.oxlint.run, / --ignore-path \.oxlintignore /);
  assert.equal(hooks.oxlint.exclude, undefined);
});

test("oxlint skips explicitly passed files that the ignore file covers", () => {
  // The hook passes staged paths explicitly, so this is what keeps it from
  // autofixing a file CI never lints.
  const dir = mkdtempSync(path.join(tmpdir(), "oxlint-ignore-"));
  try {
    const source = "export function f(): void {\n  debugger;\n}\n";
    writeFileSync(path.join(dir, "ignored.ts"), source);
    writeFileSync(path.join(dir, "linted.ts"), source);
    writeFileSync(path.join(dir, ".oxlintignore"), "ignored.ts\n");
    for (const file of ["ignored.ts", "linted.ts"])
      execFileSync(
        path.join(root, "node_modules/.bin/oxlint"),
        ["--fix", "--ignore-path", ".oxlintignore", file],
        { cwd: dir, stdio: "pipe" },
      );
    assert.equal(readFileSync(path.join(dir, "ignored.ts"), "utf8"), source);
    // Control: the same file outside the ignore list is autofixed.
    assert.notEqual(readFileSync(path.join(dir, "linted.ts"), "utf8"), source);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
