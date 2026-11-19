import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  NATIVE_BUILD_CACHE_DIR,
  captureNativeBuild,
  compilerEnvironment,
  nativeBuildCache,
  nativeBuildCacheKey,
  restoreNativeBuild,
} from "./native-build-cache.mjs";

const root = new URL("../..", import.meta.url).pathname;
const tools = { rustc: "rustc 1.93.1", napi: "napi 3" };
const keyFor = (overrides = {}) =>
  nativeBuildCacheKey(root, {
    kind: "napi",
    profile: "release",
    target: "x86_64-unknown-linux-gnu",
    args: ["napi", "build", "--release"],
    env: {},
    tools,
    ...overrides,
  });

function scratch(t) {
  const directory = mkdtempSync(join(tmpdir(), "jazz-native-build-cache-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function stage(directory, files) {
  mkdirSync(directory, { recursive: true });
  for (const [name, contents] of Object.entries(files)) {
    mkdirSync(join(directory, name, ".."), { recursive: true });
    writeFileSync(join(directory, name), contents);
  }
  return directory;
}

const read = (directory) =>
  Object.fromEntries(
    readdirSync(directory, { recursive: true, withFileTypes: true })
      .filter((entry) => entry.isFile())
      .map((entry) => {
        const path = join(entry.parentPath, entry.name);
        return [path.slice(directory.length + 1), readFileSync(path, "utf8")];
      }),
  );

test("the key changes with every input that changes compiler output", () => {
  const base = keyFor();
  assert.equal(keyFor(), base, "the key is deterministic");
  for (const [label, overrides] of [
    ["profile", { profile: "debug" }],
    ["target", { target: "aarch64-unknown-linux-gnu" }],
    ["arguments", { args: ["napi", "build"] }],
    ["tool versions", { tools: { ...tools, rustc: "rustc 1.94.0" } }],
    ["RUSTFLAGS", { env: { RUSTFLAGS: "-C opt-level=1" } }],
    ["a Cargo profile override", { env: { CARGO_PROFILE_RELEASE_LTO: "true" } }],
    ["the C compiler", { env: { CC: "clang" } }],
    ["the RN test bridge", { env: { JAZZ_RN_TEST_BRIDGE: "1" } }],
  ])
    assert.notEqual(keyFor(overrides), base, `${label} must change the key`);
  for (const env of [
    { RUSTC_WRAPPER: "sccache" },
    { CARGO_BUILD_JOBS: "16" },
    { CARGO_TARGET_DIR: "/tmp/target" },
    { HOME: "/root" },
  ])
    assert.equal(keyFor({ env }), base, `${Object.keys(env)[0]} must not change the key`);
  assert.deepEqual(Object.keys(compilerEnvironment({ RUSTFLAGS: "x", PATH: "/bin" })), [
    "RUSTFLAGS",
  ]);
});

test("a captured build becomes an entry only on commit, and restores byte for byte", (t) => {
  const directory = scratch(t);
  const cacheRoot = join(directory, "cache");
  const key = "a".repeat(64);
  const output = stage(join(directory, "output"), {
    "jazz.node": "binary",
    "nested/index.d.ts": "types",
  });

  const discarded = captureNativeBuild(cacheRoot, key, output);
  discarded.discard();
  const empty = stage(join(directory, "empty"), {});
  assert.equal(restoreNativeBuild(cacheRoot, key, empty), false);

  captureNativeBuild(cacheRoot, key, output).commit();
  const restored = stage(join(directory, "restored"), {});
  assert.equal(restoreNativeBuild(cacheRoot, key, restored), true);
  assert.deepEqual(read(restored), read(output));
});

test("a tampered or partial entry is rejected, evicted and leaves the stage empty", (t) => {
  const directory = scratch(t);
  const cacheRoot = join(directory, "cache");
  const key = "b".repeat(64);
  const output = stage(join(directory, "output"), { "jazz.node": "binary" });
  for (const tamper of [
    () => writeFileSync(join(cacheRoot, key, "jazz.node"), "other binary"),
    () => writeFileSync(join(cacheRoot, key, "extra.js"), "planted"),
    () => rmSync(join(cacheRoot, key, "jazz.node")),
    () => writeFileSync(join(cacheRoot, `${key}.json`), "{"),
  ]) {
    captureNativeBuild(cacheRoot, key, output).commit();
    tamper();
    const destination = stage(join(directory, `stage-${Math.random()}`), {});
    assert.equal(restoreNativeBuild(cacheRoot, key, destination), false);
    assert.deepEqual(readdirSync(destination), []);
    assert.deepEqual(
      readdirSync(cacheRoot).filter((name) => name.startsWith(key)),
      [],
      "the bad entry is evicted",
    );
  }
});

test("the cache keeps only its newest entries", (t) => {
  const directory = scratch(t);
  const cacheRoot = join(directory, "cache");
  const output = stage(join(directory, "output"), { "jazz.node": "binary" });
  for (let index = 0; index < 9; index++)
    captureNativeBuild(cacheRoot, index.toString(16).repeat(64), output).commit();
  assert.equal(readdirSync(cacheRoot).filter((name) => name.endsWith(".json")).length, 6);
});

test("only producer builds from a clean checkout use the cache", () => {
  const build = {
    kind: "napi",
    profile: "release",
    target: "x86_64-unknown-linux-gnu",
    args: [],
    tools,
  };
  assert.equal(nativeBuildCache(root, build, { env: {}, clean: () => true }), undefined);
  assert.equal(
    nativeBuildCache(root, build, {
      env: { JAZZ_ARTIFACT_BUILD_LEASE: "lease" },
      clean: () => false,
    }),
    undefined,
  );
  const cache = nativeBuildCache(root, build, {
    env: { JAZZ_ARTIFACT_BUILD_LEASE: "lease" },
    clean: () => true,
  });
  assert.equal(cache.key, keyFor({ args: [], env: { JAZZ_ARTIFACT_BUILD_LEASE: "lease" } }));
  assert.equal(NATIVE_BUILD_CACHE_DIR, "node_modules/.cache/jazz-native-builds");
  // The Durable Object host build runs under the TS consumer, not the
  // producer's lease; it opts out of the lease, never out of the clean check.
  assert.ok(nativeBuildCache(root, build, { env: {}, clean: () => true, requireLease: false }));
  assert.equal(
    nativeBuildCache(root, build, { env: {}, clean: () => false, requireLease: false }),
    undefined,
  );
});
