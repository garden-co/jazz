// Every example and fixture app commits its jazz.app.json and
// jazz.permissions.json (SPEC 10.10). Every .62 app has a form in the files,
// so none may be left without them: validate refuses an app it can't write,
// and CI's example builds rerun validate, so a stale file dirties the tree.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

/** Validate's projects: a schema.ts beside a permissions.ts that ships migrations. */
const FIXTURES = [
  "packages/jazz-tools/tests/ts-dsl/fixtures/basic",
  "packages/jazz-tools/tests/ts-dsl/fixtures/inferred-relations",
];

function projects() {
  const schemas = execFileSync(
    "git",
    ["ls-files", "--", "examples/**/schema.ts", "dev/stress-tests/**/schema.ts"],
    { cwd: root, encoding: "utf8" },
  )
    .split("\n")
    .filter(Boolean)
    .filter((path) => !/(?:^|\/)(?:tests?|docs|node_modules)\//.test(path))
    .filter((path) => existsSync(join(root, dirname(path), "permissions.ts")))
    .filter((path) => /from\s+["']\.\/migrations/.test(readFileSync(join(root, path), "utf8")));
  return [...schemas.map(dirname), ...FIXTURES];
}

test("every example and fixture app commits both readable files", () => {
  const listed = projects();
  assert.ok(listed.length > 30, `found only ${listed.length} projects`);
  const missing = listed.flatMap((dir) =>
    ["jazz.app.json", "jazz.permissions.json"]
      .filter((file) => !existsSync(join(root, dir, file)))
      .map((file) => `${dir}/${file}`),
  );
  assert.deepEqual(
    missing,
    [],
    `Run \`jazz-tools validate\` in these and commit:\n${missing.join("\n")}`,
  );
});
