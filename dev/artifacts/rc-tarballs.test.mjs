import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { link, packageDirs, stamp } from "./rc-tarballs.mjs";

const version = "2.0.0-alpha.60-rc.2";
const base = "https://github.com/garden-co/jazz/releases/download/v2.0.0-alpha.60-rc.2";

function tarball(dir, file, manifest) {
  const src = mkdtempSync(join(tmpdir(), "jazz-rc-pkg-"));
  mkdirSync(join(src, "package"));
  writeFileSync(join(src, "package", "package.json"), JSON.stringify(manifest));
  writeFileSync(join(src, "package", "index.js"), "module.exports = 1;\n");
  execFileSync("tar", ["-czf", join(dir, file), "-C", src, "package"]);
  rmSync(src, { recursive: true, force: true });
}

const manifestOf = (dir, file) =>
  JSON.parse(
    execFileSync("tar", ["-xzOf", join(dir, file), "package/package.json"], { encoding: "utf8" }),
  );

test("stamp sets every package and jazz-napi's platform deps to the rc version", () => {
  const root = mkdtempSync(join(tmpdir(), "jazz-rc-stamp-"));
  try {
    mkdirSync(join(root, "crates/jazz-napi/npm/linux-x64-gnu"), { recursive: true });
    mkdirSync(join(root, "crates/jazz-napi/npm/darwin-arm64"), { recursive: true });
    for (const [platform, name] of [
      ["linux-x64-gnu", "@garden-co/jazz-napi-linux-x64-gnu"],
      ["darwin-arm64", "@garden-co/jazz-napi-darwin-arm64"],
    ])
      writeFileSync(
        join(root, `crates/jazz-napi/npm/${platform}/package.json`),
        JSON.stringify({ name, version: "2.0.0-alpha.59" }),
      );
    for (const dir of packageDirs(root)) {
      mkdirSync(dir, { recursive: true });
      try {
        readFileSync(join(dir, "package.json"));
      } catch {
        writeFileSync(
          join(dir, "package.json"),
          JSON.stringify({ name: dir, version: "2.0.0-alpha.59" }),
        );
      }
    }
    stamp(version, root);
    for (const dir of packageDirs(root))
      assert.equal(JSON.parse(readFileSync(join(dir, "package.json"), "utf8")).version, version);
    assert.deepEqual(
      JSON.parse(readFileSync(join(root, "crates/jazz-napi/package.json"), "utf8"))
        .optionalDependencies,
      {
        "@garden-co/jazz-napi-darwin-arm64": version,
        "@garden-co/jazz-napi-linux-x64-gnu": version,
      },
    );
    assert.throws(() => stamp("2.0.0-alpha.60", root), /rc\.M/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("link points internal deps at the release assets and writes checksums", () => {
  const dir = mkdtempSync(join(tmpdir(), "jazz-rc-link-"));
  try {
    const napiPlatform = "garden-co-jazz-napi-linux-x64-gnu-2.0.0-alpha.60-rc.2.tgz";
    tarball(dir, `jazz-wasm-${version}.tgz`, { name: "jazz-wasm", version });
    tarball(dir, napiPlatform, { name: "@garden-co/jazz-napi-linux-x64-gnu", version });
    tarball(dir, `jazz-napi-${version}.tgz`, {
      name: "jazz-napi",
      version,
      optionalDependencies: { "@garden-co/jazz-napi-linux-x64-gnu": version },
    });
    tarball(dir, `jazz-tools-${version}.tgz`, {
      name: "jazz-tools",
      version,
      dependencies: { "jazz-wasm": version, zod: "^3.0.0" },
      optionalDependencies: { "jazz-napi": version },
      peerDependencies: { "jazz-rn": version, react: "*" },
    });
    tarball(dir, `jazz-rn-${version}.tgz`, { name: "jazz-rn", version });
    link(dir, base);
    const tools = manifestOf(dir, `jazz-tools-${version}.tgz`);
    assert.deepEqual(tools.dependencies, {
      "jazz-wasm": `${base}/jazz-wasm-${version}.tgz`,
      zod: "^3.0.0",
    });
    assert.deepEqual(tools.optionalDependencies, {
      "jazz-napi": `${base}/jazz-napi-${version}.tgz`,
    });
    assert.deepEqual(tools.peerDependencies, { "jazz-rn": version, react: "*" });
    assert.deepEqual(manifestOf(dir, `jazz-napi-${version}.tgz`).optionalDependencies, {
      "@garden-co/jazz-napi-linux-x64-gnu": `${base}/${napiPlatform}`,
    });
    const sums = readFileSync(join(dir, "NPM-SHA256SUMS"), "utf8").trim().split("\n");
    assert.equal(sums.length, 5);
    assert.ok(sums.every((line) => /^[0-9a-f]{64} {2}\S+\.tgz$/.test(line)));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("link refuses an internal dep that is not the shared rc version", () => {
  const dir = mkdtempSync(join(tmpdir(), "jazz-rc-link-"));
  try {
    tarball(dir, `jazz-wasm-${version}.tgz`, { name: "jazz-wasm", version });
    tarball(dir, `jazz-tools-${version}.tgz`, {
      name: "jazz-tools",
      version,
      dependencies: { "jazz-wasm": "2.0.0-alpha.59" },
    });
    assert.throws(() => link(dir, base), /jazz-wasm is 2\.0\.0-alpha\.59/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
