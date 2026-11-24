import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const script = fileURLToPath(new URL("./check-kotlin-sources-jar.sh", import.meta.url));
const sources = { "uniffi/jazz_ffi/jazz_ffi.kt": "package uniffi.jazz_ffi" };

function check(files) {
  const dir = mkdtempSync(join(tmpdir(), "sources-jar-"));
  try {
    for (const [name, content] of Object.entries(files)) {
      mkdirSync(join(dir, dirname(name)), { recursive: true });
      writeFileSync(join(dir, name), content);
    }
    execFileSync("zip", ["-q", "sources.jar", ...Object.keys(files)], { cwd: dir });
    const { status, stderr } = spawnSync("bash", [script, join(dir, "sources.jar")], {
      encoding: "utf8",
    });
    return { status, stderr };
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("a jar of sources passes", () => {
  assert.equal(check(sources).status, 0);
});

test("a native library fails, for every platform the Kotlin packages ship", () => {
  for (const native of [
    "linux-x86-64/libjazz_ffi.so",
    "darwin-aarch64/libjazz_ffi.dylib",
    "win32-x86-64/jazz_ffi.dll",
    "jni/arm64-v8a/libjazz_ffi.so",
  ]) {
    const { status, stderr } = check({ ...sources, [native]: "native" });
    assert.equal(status, 1, native);
    assert.match(stderr, /holds native libraries/);
  }
});

test("a jar too big to be sources fails", () => {
  const { status, stderr } = check({ ...sources, "big.bin": randomBytes(3 * 1024 * 1024) });
  assert.equal(status, 1);
  assert.match(stderr, /too big/);
});

test("a jar without the generated bindings fails", () => {
  const { status, stderr } = check({ "tools/jazz/Jazz.kt": "package tools.jazz" });
  assert.equal(status, 1);
  assert.match(stderr, /lacks the generated bindings/);
});

test("Kotlin Multiplatform sources jars keep the bindings under their source set", () => {
  assert.equal(check({ "jvmMain/uniffi/jazz_ffi/jazz_ffi.kt": "package uniffi.jazz_ffi" }).status, 0);
  assert.equal(check({ "androidMain/uniffi/jazz_ffi/jazz_ffi.kt": "package uniffi.jazz_ffi" }).status, 0);
  assert.notEqual(check({ "jvmMain/other/jazz_ffi.kt": "x" }).status, 0);
});
