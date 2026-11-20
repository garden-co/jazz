import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import config from "../../../vite.config.ts";

// The pre-commit hook must not disagree with CI's lint partition (`pnpm lint`):
// a commit the hook accepts should not fail CI's lint, and the hook should not
// block or rewrite code that CI accepts.
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const hooks = config.staged;
const scripts = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8")).scripts;

test("the clippy hook runs CI's exact Rust lint command", () => {
  // Cargo unifies features over the selected packages, so `--package <crate>`
  // compiles a different crate than `--workspace` does. Only the identical
  // command gives identical results.
  assert.match(scripts.lint, /(^|&& )pnpm lint:rust$/);
  assert.equal(hooks["{*.rs,Cargo.toml}"](), scripts["lint:rust"]);
});

test("the oxlint hook uses CI's root config and no ignore list of its own", () => {
  assert.match(scripts.lint, /^vp lint \. /);
  assert.equal(hooks["*.{ts,tsx,js,jsx}"], "vp lint --fix --no-error-on-unmatched-pattern");
});

test("oxlint skips explicitly passed files covered by the root config", () => {
  // The hook passes staged paths explicitly, so this is what keeps it from
  // autofixing a file CI never lints.
  const dir = mkdtempSync(path.join(root, "examples", "lint-hook-"));
  try {
    const source = "export const isMissing = (x: unknown) => typeof x === undefined;\n";
    mkdirSync(path.join(dir, "schema"));
    writeFileSync(path.join(dir, "schema/app.ts"), source);
    writeFileSync(path.join(dir, "linted.ts"), source);
    for (const file of ["schema/app.ts", "linted.ts"])
      execFileSync(
        path.join(root, "node_modules/.bin/vp"),
        ["lint", "--fix", "--no-error-on-unmatched-pattern", path.join(dir, file)],
        { cwd: root, stdio: "pipe" },
      );
    assert.equal(readFileSync(path.join(dir, "schema/app.ts"), "utf8"), source);
    // Control: the same file outside the ignore list is autofixed.
    assert.notEqual(readFileSync(path.join(dir, "linted.ts"), "utf8"), source);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
