import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { syncGoVersion } from "./sync-go-version.mjs";

const root = fileURLToPath(new URL("../..", import.meta.url));

function fixture(toolsVersion, goVersion) {
  const dir = mkdtempSync(join(tmpdir(), "go-version-"));
  mkdirSync(join(dir, "packages", "jazz-tools"), { recursive: true });
  mkdirSync(join(dir, "packages", "jazz-go"), { recursive: true });
  writeFileSync(
    join(dir, "packages", "jazz-tools", "package.json"),
    JSON.stringify({ version: toolsVersion }),
  );
  writeFileSync(
    join(dir, "packages", "jazz-go", "version.go"),
    `package jazz\n\n// Version …\nconst Version = "${goVersion}"\n`,
  );
  return dir;
}

test("sets jazz-go's Version to jazz-tools' version", () => {
  const dir = fixture("2.0.0-alpha.66", "2.0.0-alpha.65");
  assert.equal(syncGoVersion(dir), true);
  assert.match(
    readFileSync(join(dir, "packages", "jazz-go", "version.go"), "utf8"),
    /^const Version = "2\.0\.0-alpha\.66"$/m,
  );
  assert.equal(syncGoVersion(dir), false);
});

test("refuses a version.go without the constant", () => {
  const dir = fixture("2.0.0-alpha.66", "x");
  writeFileSync(join(dir, "packages", "jazz-go", "version.go"), "package jazz\n");
  assert.throws(() => syncGoVersion(dir), /no `const Version/);
});

test("the repository's jazz-go Version is jazz-tools' version", () => {
  const { version } = JSON.parse(
    readFileSync(join(root, "packages", "jazz-tools", "package.json"), "utf8"),
  );
  assert.match(
    readFileSync(join(root, "packages", "jazz-go", "version.go"), "utf8"),
    new RegExp(`^const Version = "${version.replaceAll(".", "\\.")}"$`, "m"),
  );
});

test("release:version syncs it after changeset version", () => {
  const { scripts } = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  for (const name of ["release:version", "release:version:alpha"]) {
    assert.match(scripts[name], /^changeset version.* && node dev\/scripts\/sync-go-version\.mjs$/);
  }
});
