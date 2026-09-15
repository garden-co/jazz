import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";
import {
  cpSync,
  readFileSync,
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  symlinkSync,
} from "node:fs";
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

test("archive source builds stop before dependency installation with prebuilt guidance", (t) => {
  const { dist } = fixture(t);
  const root = join(dist, "archive");
  mkdirSync(join(root, "dev/scripts"), { recursive: true });
  cpSync(
    new URL("./build-inspector-vercel.sh", import.meta.url),
    join(root, "dev/scripts/build-inspector-vercel.sh"),
  );
  const result = spawnSync("bash", [join(root, "dev/scripts/build-inspector-vercel.sh")], {
    encoding: "utf8",
  });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /source archive, use the verified inspector-prebuilt artifact/);
});

test("source fallback retains complete native producer and fingerprint prerequisites", () => {
  const root = new URL("../../", import.meta.url);
  const pkg = JSON.parse(readFileSync(new URL("packages/inspector/package.json", root)));
  assert.equal(pkg.scripts["build:vercel"], "bash ../../dev/scripts/build-inspector-vercel.sh");
  const script = readFileSync(new URL("dev/scripts/build-inspector-vercel.sh", root), "utf8");
  assert.ok(script.indexOf("build:ci") < script.indexOf("run build:web"));
  const build = JSON.parse(readFileSync(new URL("package.json", root))).scripts["build:ci"];
  assert.match(
    build,
    /--filter=jazz-napi --only.*stage-native-fingerprints.mjs --workspace.*--filter=jazz-tools/,
  );
});
