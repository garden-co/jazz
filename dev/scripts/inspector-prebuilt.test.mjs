import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { sealInspector, verifyInspector } from "./inspector-prebuilt.mjs";
const sha = "a".repeat(40);
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "inspector-prebuilt-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const dist = join(root, "dist");
  const staged = join(root, "staged");
  mkdirSync(dist);
  writeFileSync(join(dist, "index.html"), '<script src="/app.js"></script>');
  writeFileSync(join(dist, "app.js"), 'console.log("synthetic")');
  return { dist, staged };
}
test("sealed output verifies without a Git checkout and preserves exact source identity", (t) => {
  const { dist, staged } = fixture(t);
  sealInspector(dist, staged, sha);
  assert.equal(verifyInspector(staged, sha).sourceSha, sha);
  assert.throws(() => verifyInspector(staged, "b".repeat(40)), /source SHA mismatch/);
  assert.throws(() => sealInspector(dist, staged, sha), /already exists/);
});
for (const mutation of ["changed", "extra", "missing", "symlink"]) {
  test(`rejects ${mutation} output before deployment`, (t) => {
    const { dist, staged } = fixture(t);
    sealInspector(dist, staged, sha);
    const asset = join(staged, ".vercel/output/static/app.js");
    if (mutation === "changed") writeFileSync(asset, "changed");
    if (mutation === "extra") writeFileSync(asset + ".extra", "extra");
    if (mutation === "missing" || mutation === "symlink") rmSync(asset);
    if (mutation === "symlink") symlinkSync(join(dist, "app.js"), asset);
    assert.throws(() => verifyInspector(staged, sha), /inventory\/hash mismatch|non-regular file/);
  });
}
