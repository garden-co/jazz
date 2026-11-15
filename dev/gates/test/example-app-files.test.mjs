// Every example and fixture app is its files (SPEC 10.10): jazz.app.json,
// jazz.permissions.json and the jazz.generated.ts `jazz-tools generate`
// writes from the first, all committed. None keeps an alpha.64 source
// (schema.ts, permissions.ts, migrations/) beside them: `jazz-tools upgrade`
// deletes those, and a leftover would read as a second source of truth.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/**
 * TS apps: every app with a jazz.app.json, except Rust crates (a Cargo.toml
 * beside it), whose bindings are `src/jazz_app.rs` and which the `jazz-app`
 * crate's checks cover.
 */
function projects() {
  return execFileSync(
    "git",
    ["ls-files", "--", "examples/**/jazz.app.json", "dev/stress-tests/**/jazz.app.json"],
    { cwd: root, encoding: "utf8" },
  )
    .split("\n")
    .filter(Boolean)
    .map(dirname)
    .filter((dir) => !existsSync(join(root, dir, "Cargo.toml")));
}

test("every example app commits its files and its bindings", () => {
  const listed = projects();
  assert.ok(listed.length > 30, `found only ${listed.length} projects`);
  const missing = listed.flatMap((dir) =>
    ["jazz.permissions.json", "jazz.generated.ts"]
      .filter((file) => !existsSync(join(root, dir, file)))
      .map((file) => `${dir}/${file}`),
  );
  assert.deepEqual(
    missing,
    [],
    `Run \`jazz-tools generate\` in these and commit what it writes:\n${missing.join("\n")}`,
  );
});

test("no example keeps an alpha.64 schema source beside its files", () => {
  const leftovers = projects().flatMap((dir) =>
    ["schema.ts", "permissions.ts", "migrations"]
      .filter((file) => existsSync(join(root, dir, file)))
      .map((file) => `${dir}/${file}`),
  );
  assert.deepEqual(
    leftovers,
    [],
    `jazz.app.json and jazz.permissions.json are the app's only source; \`jazz-tools upgrade\` removes these:\n${leftovers.join("\n")}`,
  );
});
