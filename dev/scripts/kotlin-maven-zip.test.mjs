import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

const pack = fileURLToPath(new URL("./pack-kotlin-maven-zip.sh", import.meta.url));
const checkScript = fileURLToPath(new URL("./check-kotlin-maven-zip.sh", import.meta.url));
const V = "2.0.0-beta.2";

function jar(dir, name, files) {
  const src = mkdtempSync(join(tmpdir(), "jar-"));
  for (const [f, c] of Object.entries(files)) {
    mkdirSync(join(src, dirname(f)), { recursive: true });
    writeFileSync(join(src, f), c);
  }
  mkdirSync(dir, { recursive: true });
  execFileSync("zip", ["-q", join(dir, name), ...Object.keys(files)], { cwd: src });
  rmSync(src, { recursive: true, force: true });
}

/** A local Maven repository shaped like the KMP publish; `omit` drops a path, `extra` adds files. */
function repo({ omit = [], extra = {} } = {}) {
  const root = mkdtempSync(join(tmpdir(), "m2-"));
  const put = (path, content) => {
    if (omit.includes(path)) return;
    mkdirSync(join(root, dirname(path)), { recursive: true });
    writeFileSync(join(root, path), content);
  };
  const base = (a) => `tools/jazz/${a}/${V}/${a}-${V}`;
  put(`${base("jazz-kotlin")}.module`, JSON.stringify({
    variants: ["jazz-kotlin-jvm", "jazz-kotlin-android"].map((m) => ({
      "available-at": { url: `../../${m}/${V}/${m}-${V}.module`, group: "tools.jazz", module: m, version: V },
    })),
  }, null, 2));
  put(`${base("jazz-kotlin")}.pom`, "<project/>");
  put(`${base("jazz-kotlin")}.module.asc`, "throwaway");
  put(`tools/jazz/jazz-kotlin/maven-metadata-local.xml`, "<metadata/>");
  if (!omit.includes("root-sources"))
    jar(join(root, `tools/jazz/jazz-kotlin/${V}`), `jazz-kotlin-${V}-sources.jar`, { "commonMain/tools/jazz/Db.kt": "x" });
  for (const [a, main, set] of [["jazz-kotlin-jvm", "jar", "jvmMain"], ["jazz-kotlin-android", "aar", "androidMain"]]) {
    put(`${base(a)}.${main}`, "bin");
    put(`${base(a)}.pom`, "<project/>");
    put(`${base(a)}.module`, "{}");
    if (!omit.includes(`${a}-sources`))
      jar(join(root, `tools/jazz/${a}/${V}`), `${a}-${V}-sources.jar`, {
        [`${set}/uniffi/jazz_ffi/jazz_ffi.kt`]: "package uniffi.jazz_ffi",
        ...(extra[`${a}-sources`] ?? {}),
      });
  }
  return root;
}

function packAndCheck(root) {
  const out = join(root, "out", `jazz-kotlin-maven-${V}.zip`);
  execFileSync("bash", [pack, root, V, out], { stdio: "pipe" });
  const r = spawnSync("bash", [checkScript, out, V], { encoding: "utf8" });
  return { ...r, out };
}

test("the KMP repository packs and passes, without signatures or metadata", () => {
  const root = repo();
  try {
    const { status, stderr, out } = packAndCheck(root);
    assert.equal(status, 0, stderr);
    const entries = execFileSync("unzip", ["-Z1", out], { encoding: "utf8" });
    assert.doesNotMatch(entries, /\.asc$|maven-metadata/m);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("packing is reproducible", () => {
  const a = repo();
  const b = repo();
  try {
    assert.deepEqual(readFileSync(packAndCheck(a).out), readFileSync(packAndCheck(b).out));
  } finally {
    rmSync(a, { recursive: true, force: true });
    rmSync(b, { recursive: true, force: true });
  }
});

for (const omit of [
  `tools/jazz/jazz-kotlin/${V}/jazz-kotlin-${V}.module`,
  `tools/jazz/jazz-kotlin-jvm/${V}/jazz-kotlin-jvm-${V}.jar`,
  `tools/jazz/jazz-kotlin-android/${V}/jazz-kotlin-android-${V}.aar`,
  "jazz-kotlin-jvm-sources",
]) {
  test(`a zip without ${omit.split("/").pop()} fails`, () => {
    const root = repo({ omit: [omit] });
    try {
      assert.notEqual(packAndCheck(root).status, 0);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}

test("a sources jar holding a native library fails", () => {
  const root = repo({ extra: { "jazz-kotlin-android-sources": { "jni/arm64-v8a/libjazz_ffi.so": "elf" } } });
  try {
    const { status, stderr } = packAndCheck(root);
    assert.notEqual(status, 0);
    assert.match(stderr, /native libraries/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("an optional overlay (iOS klibs) joins the zip, and the zip still passes", () => {
  const root = repo();
  const ios = mkdtempSync(join(tmpdir(), "m2-ios-"));
  try {
    const d = join(ios, `tools/jazz/jazz-kotlin-iosarm64/${V}`);
    mkdirSync(d, { recursive: true });
    writeFileSync(join(d, `jazz-kotlin-iosarm64-${V}.klib`), "klib");
    const out = join(root, "out", "z.zip");
    execFileSync("bash", [pack, root, V, out, ios], { stdio: "pipe" });
    const r = spawnSync("bash", [checkScript, out, V], { encoding: "utf8" });
    assert.equal(r.status, 0, r.stderr);
    assert.match(execFileSync("unzip", ["-Z1", out], { encoding: "utf8" }), /jazz-kotlin-iosarm64-.*\.klib/);
  } finally {
    rmSync(root, { recursive: true, force: true });
    rmSync(ios, { recursive: true, force: true });
  }
});
