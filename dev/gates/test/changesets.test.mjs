import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { planRelease } from "../changesets.mjs";

function workspace(changeset) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "changesets-gate-"));
  const write = (file, text) => {
    fs.mkdirSync(path.dirname(path.join(root, file)), { recursive: true });
    fs.writeFileSync(path.join(root, file), text);
  };
  write("package.json", JSON.stringify({ name: "root", private: true, workspaces: ["pkgs/*"] }));
  write("pkgs/public/package.json", JSON.stringify({ name: "public-pkg", version: "1.0.0" }));
  write(
    "pkgs/private/package.json",
    JSON.stringify({ name: "private-pkg", version: "0.0.0", private: true }),
  );
  write(
    ".changeset/config.json",
    JSON.stringify({
      changelog: false,
      commit: false,
      fixed: [],
      linked: [],
      access: "public",
      baseBranch: "main",
      updateInternalDependencies: "patch",
      ignore: [],
      privatePackages: { version: false, tag: false },
    }),
  );
  write(".changeset/change.md", changeset);
  return root;
}

test("plans a changeset that names only public packages", async () => {
  const plan = await planRelease(workspace('---\n"public-pkg": patch\n---\n\nA fix.\n'));
  assert.deepEqual(
    plan.releases.map((release) => [release.name, release.type]),
    [["public-pkg", "patch"]],
  );
});

test("refuses a changeset that also names a private package, as `changeset version` does", async () => {
  await assert.rejects(
    planRelease(workspace('---\n"public-pkg": patch\n"private-pkg": patch\n---\n\nA fix.\n')),
    /Mixed changesets/,
  );
});
