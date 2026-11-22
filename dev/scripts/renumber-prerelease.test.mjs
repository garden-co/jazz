import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { renumbering, rewrite } from "./renumber-prerelease.mjs";

const script = fileURLToPath(new URL("./renumber-prerelease.mjs", import.meta.url));

test("the first version on a new prerelease tag starts at .0", () => {
  const before = new Map([["jazz-tools", "2.0.0-alpha.70"], ["create-jazz", "2.0.0-alpha.70"]]);
  const after = new Map([["jazz-tools", "2.0.0-beta.71"], ["create-jazz", "2.0.0-beta.71"]]);
  assert.deepEqual([...renumbering("beta", before, after)], [["2.0.0-beta.71", "2.0.0-beta.0"]]);
});

test("later versions on the same tag, and unchanged packages, are left alone", () => {
  assert.equal(renumbering("beta", new Map([["a", "2.0.0-beta.0"]]), new Map([["a", "2.0.0-beta.1"]])).size, 0);
  assert.equal(renumbering("beta", new Map([["a", "2.0.0-alpha.70"]]), new Map([["a", "2.0.0-alpha.70"]])).size, 0);
  assert.equal(renumbering("alpha", new Map([["a", "2.0.0-alpha.70"]]), new Map([["a", "2.0.0-alpha.71"]])).size, 0);
  assert.equal(renumbering("beta", new Map([["a", "0.1.0"]]), new Map([["a", "0.1.1-beta.0"]])).size, 0);
});

test("rewrite replaces whole versions only", () => {
  const renames = new Map([["2.0.0-beta.71", "2.0.0-beta.0"]]);
  assert.equal(
    rewrite("## 2.0.0-beta.71\n- jazz-wasm@2.0.0-beta.71.\n2.0.0-beta.710 2.0.0-beta.71.1 2.0.0-beta.71-rc.1", renames),
    "## 2.0.0-beta.0\n- jazz-wasm@2.0.0-beta.0.\n2.0.0-beta.710 2.0.0-beta.71.1 2.0.0-beta.71-rc.1",
  );
});

test("the CLI renumbers what changeset version changed, against git HEAD", () => {
  const root = mkdtempSync(join(tmpdir(), "renumber-"));
  const git = (...args) => execFileSync("git", ["-C", root, ...args], { encoding: "utf8" });
  git("init", "-q");
  mkdirSync(join(root, ".changeset"));
  mkdirSync(join(root, "pkg"));
  writeFileSync(join(root, ".changeset/pre.json"), JSON.stringify({ mode: "pre", tag: "beta" }));
  const pkg = (version) => `${JSON.stringify({ name: "jazz-tools", version, dependencies: { "jazz-wasm": version } }, null, 2)}\n`;
  writeFileSync(join(root, "pkg/package.json"), pkg("2.0.0-alpha.70"));
  writeFileSync(join(root, "pkg/CHANGELOG.md"), "# jazz-tools\n\n## 2.0.0-alpha.70\n");
  git("add", ".");
  git("-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base");
  writeFileSync(join(root, "pkg/package.json"), pkg("2.0.0-beta.71"));
  writeFileSync(join(root, "pkg/CHANGELOG.md"), "# jazz-tools\n\n## 2.0.0-beta.71\n\n- jazz-wasm@2.0.0-beta.71\n\n## 2.0.0-alpha.70\n");
  execFileSync("node", [script, "--root", root]);
  assert.equal(readFileSync(join(root, "pkg/package.json"), "utf8"), pkg("2.0.0-beta.0"));
  assert.equal(
    readFileSync(join(root, "pkg/CHANGELOG.md"), "utf8"),
    "# jazz-tools\n\n## 2.0.0-beta.0\n\n- jazz-wasm@2.0.0-beta.0\n\n## 2.0.0-alpha.70\n",
  );
});
