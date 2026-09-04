import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  expectedManifest,
  manifestPath,
  nativeArtifactFingerprint,
  verifyManifest,
  verifyPublishedNapiManifest,
  writeManifest,
} from "./provenance.mjs";
import { stageNapiManifests } from "./stage-napi-manifests.mjs";

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-artifact-provenance-"));
  for (const dir of [
    ".cargo",
    "crates/jazz-wasm/pkg",
    "crates/jazz-wasm/src",
    "crates/jazz/src",
    "crates/groove/src",
    "crates/wasm-tracing/src",
    "crates/jazz-napi/src",
    "crates/jazz-server/src",
    "crates/jazz-native-transport/src",
    "crates/jazz-storage-rocksdb/src",
    "crates/jazz-otel/src",
    "crates/jazz-compression/src",
    "crates/benchmark-guard/src",
    "crates/idb-tree/src",
  ])
    mkdirSync(join(root, dir), { recursive: true });
  for (const [path, content] of Object.entries({
    "Cargo.toml": "[workspace]\nmembers = ['crates/*']\nresolver = '2'\n",
    "rust-toolchain.toml": "[toolchain]\nchannel = 'stable'\n",
    ".cargo/config.toml": "[build]\n",
    "package.json": "{}\n",
    "pnpm-lock.yaml": "lockfileVersion: '9.0'\n",
    "crates/jazz-wasm/Cargo.toml": "[package]\nname = 'wasm'\n",
    "crates/jazz-wasm/src/lib.rs": "// source\n",
  }))
    writeFileSync(join(root, path), content);
  const dependencies = {
    "jazz-wasm": ["jazz", "wasm-tracing", "idb-tree"],
    "jazz-napi": [
      "jazz",
      "jazz-server",
      "jazz-native-transport",
      "jazz-storage-rocksdb",
      "jazz-otel",
    ],
    jazz: ["groove", "jazz-compression", "benchmark-guard"],
  };
  for (const name of [
    "jazz-napi",
    "jazz-server",
    "jazz-native-transport",
    "jazz-storage-rocksdb",
    "jazz-otel",
    "jazz-compression",
    "benchmark-guard",
    "idb-tree",
    "jazz",
    "groove",
    "wasm-tracing",
  ]) {
    const manifest = join(root, `crates/${name}/Cargo.toml`);
    if (name === "jazz-wasm") continue;
    const deps = (dependencies[name] ?? [])
      .map((dependency) => `${dependency} = { path = '../${dependency}' }`)
      .join("\n");
    writeFileSync(
      manifest,
      `[package]\nname = '${name}'\nversion = '0.0.0'\nedition = '2021'\n[dependencies]\n${deps}\n`,
    );
    writeFileSync(join(root, `crates/${name}/src/lib.rs`), `// ${name}\n`);
  }
  writeFileSync(
    join(root, "crates/jazz-wasm/Cargo.toml"),
    `[package]\nname = 'jazz-wasm'\nversion = '0.0.0'\nedition = '2021'\n[dependencies]\n${dependencies[
      "jazz-wasm"
    ]
      .map((dependency) => `${dependency} = { path = '../${dependency}' }`)
      .join("\n")}\n`,
  );
  execFileSync("cargo", ["generate-lockfile"], { cwd: root, stdio: "ignore" });
  return root;
}

function git(root, args) {
  execFileSync("git", args, { cwd: root, stdio: "ignore" });
}

function withRepositoryGitProvenance(callback) {
  const names = [
    "JAZZ_ARTIFACT_GIT_HEAD",
    "JAZZ_ARTIFACT_GIT_TREE",
    "JAZZ_ARTIFACT_GIT_DIRTY_DIFF",
  ];
  const previous = Object.fromEntries(names.map((name) => [name, process.env[name]]));
  for (const name of names) delete process.env[name];
  try {
    return callback();
  } finally {
    for (const name of names) {
      if (previous[name] === undefined) delete process.env[name];
      else process.env[name] = previous[name];
    }
  }
}

process.env.JAZZ_ARTIFACT_GIT_HEAD = "test-head";
process.env.JAZZ_ARTIFACT_GIT_TREE = "test-tree";
process.env.JAZZ_ARTIFACT_GIT_DIRTY_DIFF = "test-dirty";
process.env.JAZZ_ARTIFACT_TOOL_RUSTC = "rustc test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_PACK = "wasm-pack test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_BINDGEN = "wasm-bindgen test";
process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT = "wasm-opt test";
process.env.JAZZ_ARTIFACT_TOOL_NAPI = "napi test";

test("provenance rejects stale tree, lock, toolchain, and profile", () => {
  const root = fixture();
  writeManifest(root, "wasm", "fast");
  assert.equal(verifyManifest(root, "wasm", "fast"), null);
  assert.match(verifyManifest(root, "wasm", "release"), /profile differs/);

  const manifest = JSON.parse(readFileSync(manifestPath(root, "wasm"), "utf8"));
  manifest.git.tree = "stale";
  writeFileSync(manifestPath(root, "wasm"), JSON.stringify(manifest));
  assert.match(verifyManifest(root, "wasm", "fast"), /git.tree differs/);

  writeManifest(root, "wasm", "fast");
  const cargoLock = readFileSync(join(root, "Cargo.lock"), "utf8");
  writeFileSync(join(root, "Cargo.lock"), `${cargoLock}\n# planted drift\n`);
  assert.match(verifyManifest(root, "wasm", "fast"), /cargoLock differs/);
  writeFileSync(join(root, "Cargo.lock"), cargoLock);
  writeFileSync(join(root, "rust-toolchain.toml"), "[toolchain]\nchannel = 'beta'\n");
  assert.match(verifyManifest(root, "wasm", "fast"), /rustToolchain differs/);
});

test("provenance rejects a fingerprint-only sealed manifest drift", () => {
  const root = fixture();
  writeManifest(root, "wasm", "fast");
  const path = manifestPath(root, "wasm");
  const stale = JSON.parse(readFileSync(path, "utf8"));
  stale.nativeArtifactFingerprint = "0".repeat(64);
  writeFileSync(path, JSON.stringify(stale));
  assert.match(verifyManifest(root, "wasm", "fast"), /nativeArtifactFingerprint differs/);
  writeManifest(root, "wasm", "fast");
  assert.equal(verifyManifest(root, "wasm", "fast"), null);
});

test("dirty source changes invalidate the manifest", () => {
  const root = fixture();
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, "crates/jazz-wasm/src/lib.rs"), "// changed\n");
  assert.match(
    verifyManifest(root, "wasm", "release"),
    /packageInputs differs|git.dirtyDiff differs/,
  );
});

test("WASM provenance ignores local generated fingerprints but not tracked source changes", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    const runtime = join(root, "packages/jazz-tools/src/runtime");
    mkdirSync(runtime, { recursive: true });
    for (const file of [
      "native-artifact-fingerprint-napi.ts",
      "native-artifact-fingerprint-wasm.ts",
    ])
      writeFileSync(join(runtime, file), "// generated baseline\n");
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);

    const clean = expectedManifest(root, "wasm", "fast").git.dirtyDiff;
    writeFileSync(join(runtime, "native-artifact-fingerprint-napi.ts"), "// generated NAPI\n");
    writeFileSync(join(runtime, "native-artifact-fingerprint-wasm.ts"), "// generated WASM\n");
    assert.equal(expectedManifest(root, "wasm", "fast").git.dirtyDiff, clean);

    writeFileSync(join(root, "crates/jazz-wasm/src/lib.rs"), "// real source change\n");
    assert.notEqual(expectedManifest(root, "wasm", "fast").git.dirtyDiff, clean);
    rmSync(root, { recursive: true, force: true });
  }));

test("native compatibility identity is stable across committed generated expectations", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    const runtime = join(root, "packages/jazz-tools/src/runtime");
    mkdirSync(runtime, { recursive: true });
    const napiExpectation = join(runtime, "native-artifact-fingerprint-napi.ts");
    const wasmExpectation = join(runtime, "native-artifact-fingerprint-wasm.ts");
    writeFileSync(napiExpectation, "// generated baseline NAPI\n");
    writeFileSync(wasmExpectation, "// generated baseline WASM\n");
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);

    const before = nativeArtifactFingerprint(root, "wasm", "fast");
    const beforeHead = expectedManifest(root, "wasm", "fast").git.head;
    writeFileSync(napiExpectation, "// regenerated NAPI\n");
    writeFileSync(wasmExpectation, "// regenerated WASM\n");
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "commit generated expectations"]);

    assert.notEqual(expectedManifest(root, "wasm", "fast").git.head, beforeHead);
    assert.equal(nativeArtifactFingerprint(root, "wasm", "fast"), before);
    assert.equal(nativeArtifactFingerprint(root, "wasm", "fast"), before);

    writeFileSync(join(root, "crates/jazz-wasm/src/lib.rs"), "// planted native change\n");
    assert.notEqual(nativeArtifactFingerprint(root, "wasm", "fast"), before);
    rmSync(root, { recursive: true, force: true });
  }));

test("native fingerprints include authoritative artifact tools but not receipt or test outputs", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    const artifacts = join(root, "dev/artifacts");
    mkdirSync(artifacts, { recursive: true });
    for (const file of [
      "build.mjs",
      "provenance.mjs",
      "stage-napi-loader.mjs",
      "stage-native-fingerprints.mjs",
      "stage-napi-manifests.mjs",
      "provenance.test.mjs",
      "correctness-artifact-producer.mjs",
    ])
      writeFileSync(join(artifacts, file), `// ${file}\n`);
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);

    const before = nativeArtifactFingerprint(root, "napi", "release");
    writeFileSync(join(artifacts, "provenance.test.mjs"), "// changed test output\n");
    writeFileSync(
      join(artifacts, "correctness-artifact-producer.mjs"),
      "// changed producer receipt helper\n",
    );
    assert.equal(nativeArtifactFingerprint(root, "napi", "release"), before);

    writeFileSync(join(artifacts, "build.mjs"), "// changed authoritative builder\n");
    assert.notEqual(nativeArtifactFingerprint(root, "napi", "release"), before);
    rmSync(root, { recursive: true, force: true });
  }));

test("NAPI provenance excludes only the wrapper's ephemeral staged binding", () => {
  const root = fixture();
  writeManifest(root, "napi", "release");
  const before = expectedManifest(root, "napi", "release").packageInputs;

  // build.mjs uses this exact name while replacing a target binding.  Its
  // presence must not make a manifest stale immediately after the build.
  writeFileSync(
    join(root, "crates/jazz-napi/jazz-napi.linux-x64-gnu.node.staged-123-456"),
    "previous native binding",
  );
  assert.equal(expectedManifest(root, "napi", "release").packageInputs, before);
  assert.equal(verifyManifest(root, "napi", "release"), null);
  rmSync(join(root, "crates/jazz-napi/jazz-napi.linux-x64-gnu.node.staged-123-456"));

  // Turbo writes this receipt after the inner NAPI build wrapper seals its
  // manifest. It is a package-local build output, not a build input.
  mkdirSync(join(root, "crates/jazz-napi/.turbo"));
  writeFileSync(join(root, "crates/jazz-napi/.turbo/turbo-build.log"), "outer task receipt");
  assert.equal(expectedManifest(root, "napi", "release").packageInputs, before);
  assert.equal(verifyManifest(root, "napi", "release"), null);

  // napi-rs writes this ignored target manifest next to the binding. It is a
  // post-build receipt, not a source input for the artifact it describes.
  writeFileSync(
    join(root, "crates/jazz-napi/jazz-napi.linux-x64-gnu.manifest.json"),
    '{"generated":true}\n',
  );
  assert.equal(expectedManifest(root, "napi", "release").packageInputs, before);
  assert.equal(verifyManifest(root, "napi", "release"), null);

  // Near misses are ordinary inputs: accepting any made-up binding name,
  // suffix, or appended extension would let generated source evade freshness.
  for (const path of [
    "jazz-napi.linux-x64-gnu.node.staged-123-456.rs",
    "jazz-napi.attacker.node.staged-123-456",
    "jazz-napi.linux-x64-gnu.node.staged-not-a-wrapper",
    "jazz-napi.attacker.manifest.json",
  ]) {
    const file = join(root, "crates/jazz-napi", path);
    writeFileSync(file, "must remain an input");
    assert.notEqual(expectedManifest(root, "napi", "release").packageInputs, before, path);
    rmSync(file);
  }

  // Planted positive: an actual NAPI source remains a provenance input.
  writeFileSync(join(root, "crates/jazz-napi/src/lib.rs"), "// changed native source\n");
  assert.match(verifyManifest(root, "napi", "release"), /packageInputs differs/);
});

test("NAPI fingerprint ignores an ignored nested generated index.js", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    writeFileSync(join(root, "crates/jazz-napi/.gitignore"), "index.js\n");
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);

    const before = nativeArtifactFingerprint(root, "napi", "release");
    const nestedOutput = join(root, "crates/jazz-napi/crates/jazz-napi/index.js");
    mkdirSync(join(root, "crates/jazz-napi/crates/jazz-napi"), { recursive: true });
    writeFileSync(nestedOutput, "// stale nested generated binding\n");

    assert.equal(
      nativeArtifactFingerprint(root, "napi", "release"),
      before,
      "ignored generated output below a workspace package must not alter the ABI fingerprint",
    );
    rmSync(root, { recursive: true, force: true });
  }));

test("NAPI provenance covers every reachable local Cargo dependency", () => {
  const root = fixture();
  writeManifest(root, "napi", "release");
  for (const crate of [
    "jazz-server",
    "jazz-native-transport",
    "jazz-storage-rocksdb",
    "jazz-otel",
    "jazz-compression",
    "benchmark-guard",
  ]) {
    const path = join(root, `crates/${crate}/src/lib.rs`);
    writeFileSync(path, "// planted dependency change\n");
    assert.match(verifyManifest(root, "napi", "release"), /packageInputs differs/, crate);
    writeManifest(root, "napi", "release");
  }
});

test("WASM provenance derives transitive workspace inputs and ignores local task logs", () => {
  const root = fixture();
  writeManifest(root, "wasm", "fast");

  mkdirSync(join(root, "crates/jazz-wasm/.turbo"), { recursive: true });
  writeFileSync(join(root, "crates/jazz-wasm/.turbo/build.log"), "lane-local noise\n");
  assert.equal(verifyManifest(root, "wasm", "fast"), null);

  writeFileSync(join(root, "crates/jazz-compression/src/lib.rs"), "// planted transitive change\n");
  assert.match(verifyManifest(root, "wasm", "fast"), /packageInputs differs/);
});

test("native fingerprints agree across clean worktree locations", () => {
  const first = fixture();
  const second = fixture();
  mkdirSync(join(second, "crates/jazz-wasm/.turbo"), { recursive: true });
  writeFileSync(join(second, "crates/jazz-wasm/.turbo/build.log"), "different local receipt\n");
  mkdirSync(join(second, "crates/jazz-napi/.turbo"), { recursive: true });
  writeFileSync(join(second, "crates/jazz-napi/.turbo/test.log"), "different local receipt\n");

  assert.equal(nativeArtifactFingerprint(first, "wasm"), nativeArtifactFingerprint(second, "wasm"));
  assert.equal(nativeArtifactFingerprint(first, "napi"), nativeArtifactFingerprint(second, "napi"));
});

test("clean CRLF checkout retains the committed NAPI ABI identity", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);
    const baseline = nativeArtifactFingerprint(root, "napi", "release");

    git(root, ["config", "core.autocrlf", "true"]);
    const source = join(root, "crates/jazz-napi/src/lib.rs");
    rmSync(source);
    git(root, ["checkout", "--", "crates/jazz-napi/src/lib.rs"]);
    assert.match(readFileSync(source, "utf8"), /\r\n/);
    assert.equal(nativeArtifactFingerprint(root, "napi", "release"), baseline);

    writeFileSync(join(root, "crates/jazz-napi/src/lib.rs"), "// planted dirty source\r\n");
    assert.notEqual(nativeArtifactFingerprint(root, "napi", "release"), baseline);
    rmSync(root, { recursive: true, force: true });
  }));

test("NAPI provenance batches real-size CRLF inputs without masking staged or dirty sources", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    const source = join(root, "crates/jazz-napi/src/lib.rs");
    writeFileSync(source, "// real-size provenance input\n".repeat(60_000));
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "fixture"]);
    const baseline = nativeArtifactFingerprint(root, "napi", "release");

    git(root, ["config", "core.autocrlf", "true"]);
    rmSync(source);
    git(root, ["checkout", "--", "crates/jazz-napi/src/lib.rs"]);
    assert.match(readFileSync(source, "utf8"), /\r\n/);
    assert.equal(nativeArtifactFingerprint(root, "napi", "release"), baseline);

    const original = readFileSync(source);
    writeFileSync(source, "// staged replacement\r\n");
    git(root, ["add", "crates/jazz-napi/src/lib.rs"]);
    const staged = nativeArtifactFingerprint(root, "napi", "release");
    writeFileSync(source, original);
    assert.notEqual(nativeArtifactFingerprint(root, "napi", "release"), staged);
    rmSync(root, { recursive: true, force: true });
  }));

test("NAPI provenance reads compiler-visible symlink and clean-filter source bytes", () =>
  withRepositoryGitProvenance(() => {
    const root = fixture();
    const source = join(root, "crates/jazz-napi/src/lib.rs");
    const target = join(root, "shared-source.rs");
    writeFileSync(target, "// source A\n");
    rmSync(source);
    symlinkSync("../../../shared-source.rs", source);
    git(root, ["init", "--quiet"]);
    git(root, ["config", "user.email", "tests@example.invalid"]);
    git(root, ["config", "user.name", "Jazz tests"]);
    git(root, ["add", "."]);
    git(root, ["commit", "--quiet", "-m", "symlink fixture"]);
    const symlinkBaseline = nativeArtifactFingerprint(root, "napi", "release");
    writeFileSync(target, "// source B\n");
    assert.notEqual(nativeArtifactFingerprint(root, "napi", "release"), symlinkBaseline);
    rmSync(root, { recursive: true, force: true });

    const filtered = fixture();
    const filteredSource = join(filtered, "crates/jazz-napi/src/lib.rs");
    writeFileSync(filteredSource, "pub const FLAG: bool = true;\n");
    writeFileSync(join(filtered, ".gitattributes"), "crates/jazz-napi/src/lib.rs filter=review\n");
    git(filtered, ["init", "--quiet"]);
    git(filtered, ["config", "user.email", "tests@example.invalid"]);
    git(filtered, ["config", "user.name", "Jazz tests"]);
    git(filtered, ["config", "filter.review.clean", "sed s/false/true/g"]);
    git(filtered, ["config", "filter.review.smudge", "cat"]);
    git(filtered, ["add", "."]);
    git(filtered, ["commit", "--quiet", "-m", "filtered fixture"]);
    const filterBaseline = nativeArtifactFingerprint(filtered, "napi", "release");
    writeFileSync(filteredSource, "pub const FLAG: bool = false;\n");
    assert.equal(
      execFileSync("git", ["diff", "--name-only", "HEAD"], { cwd: filtered, encoding: "utf8" }),
      "",
    );
    assert.notEqual(nativeArtifactFingerprint(filtered, "napi", "release"), filterBaseline);
    rmSync(filtered, { recursive: true, force: true });
  }));

test("tracked NAPI bootstrap changes invalidate sealed provenance and its ABI fingerprint", () => {
  const root = fixture();
  const bootstrap = join(root, "crates/jazz-napi/native-binding.cjs");
  writeFileSync(bootstrap, "module.exports = {};\n");
  writeManifest(root, "napi", "release");
  const inputs = expectedManifest(root, "napi", "release").packageInputs;
  const fingerprint = nativeArtifactFingerprint(root, "napi", "release");
  writeFileSync(bootstrap, "module.exports = { changed: true };\n");
  assert.notEqual(expectedManifest(root, "napi", "release").packageInputs, inputs);
  assert.notEqual(nativeArtifactFingerprint(root, "napi", "release"), fingerprint);
  assert.match(verifyManifest(root, "napi", "release"), /packageInputs differs/);
});

test("WASM provenance covers generated glue and declarations, not only the binary", () => {
  const root = fixture();
  for (const file of [
    "jazz_wasm_bg.wasm",
    "jazz_wasm.js",
    "jazz_wasm.d.ts",
    "jazz_wasm_bg.wasm.d.ts",
  ])
    writeFileSync(join(root, "crates/jazz-wasm/pkg", file), "current");
  writeManifest(root, "wasm", "fast");
  writeFileSync(join(root, "crates/jazz-wasm/pkg/jazz_wasm.js"), "stale glue");
  assert.match(verifyManifest(root, "wasm", "fast"), /artifacts differs/);
});

test("provenance rejects tool and root-package configuration drift", () => {
  const root = fixture();
  writeManifest(root, "wasm", "release");
  const manifest = JSON.parse(readFileSync(manifestPath(root, "wasm"), "utf8"));
  for (const tool of ["rustc", "wasmPack", "wasmBindgen", "wasmOpt"]) {
    const changed = structuredClone(manifest);
    changed.tools[tool] = "stale";
    writeFileSync(manifestPath(root, "wasm"), JSON.stringify(changed));
    assert.match(verifyManifest(root, "wasm", "release"), new RegExp(`tools\\.${tool} differs`));
  }
  const toolHashChanged = structuredClone(manifest);
  toolHashChanged.toolchainInputs = "stale";
  writeFileSync(manifestPath(root, "wasm"), JSON.stringify(toolHashChanged));
  assert.match(verifyManifest(root, "wasm", "release"), /toolchainInputs differs/);
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, "package.json"), '{"changed":true}\n');
  assert.match(verifyManifest(root, "wasm", "release"), /packageInputs differs/);
  writeManifest(root, "wasm", "release");
  writeFileSync(join(root, ".cargo/config.toml"), "[build]\ntarget-dir = 'other'\n");
  assert.match(verifyManifest(root, "wasm", "release"), /packageInputs differs/);

  writeManifest(root, "napi", "release");
  const napi = JSON.parse(readFileSync(manifestPath(root, "napi"), "utf8"));
  napi.tools.napi = "stale";
  writeFileSync(manifestPath(root, "napi"), JSON.stringify(napi));
  assert.match(verifyManifest(root, "napi", "release"), /tools.napi differs/);

  writeManifest(root, "napi", "release", "aarch64-apple-darwin");
  assert.match(verifyManifest(root, "napi", "release", "x86_64-apple-darwin"), /target differs/);
});

test("missing optional tools have an explicit remediation value", () => {
  const root = fixture();
  delete process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT;
  process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE = "1";
  assert.match(
    expectedManifest(root, "wasm", "fast").tools.wasmOpt,
    /unavailable: wasm-opt is supplied by wasm-pack; rebuild via pnpm --filter jazz-wasm build/,
  );
  process.env.JAZZ_ARTIFACT_TOOL_WASM_OPT = "wasm-opt test";
  delete process.env.JAZZ_ARTIFACT_DISABLE_WASM_PACK_CACHE;
});

test("release NAPI CI builds use the manifest-producing wrapper", () => {
  const workflow = readFileSync(
    new URL("../../.github/workflows/build-jazz-packages.yml", import.meta.url),
    "utf8",
  );
  assert.match(
    workflow,
    /node dev\/artifacts\/build\.mjs napi release --target \$\{\{ matrix\.target \}\}/,
  );
  assert.match(
    workflow,
    /node dev\/artifacts\/provenance\.mjs verify napi release --target \$\{\{ matrix\.target \}\}/,
  );
});

test("assembled NAPI packages carry only matching manifests and reject stale or missing inputs", () => {
  const root = fixture();
  const platforms = {
    "linux-x64-gnu": "x86_64-unknown-linux-gnu",
    "darwin-x64": "x86_64-apple-darwin",
    "darwin-arm64": "aarch64-apple-darwin",
    "win32-x64-msvc": "x86_64-pc-windows-msvc",
  };
  for (const platform of Object.keys(platforms)) {
    const dir = join(root, "crates/jazz-napi/npm", platform);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "package.json"), '{"files":["*.node"]}\n');
    writeFileSync(join(root, "crates/jazz-napi", `jazz-napi.${platform}.node`), platform);
    writeFileSync(join(dir, `jazz-napi.${platform}.node`), platform);
  }
  writeFileSync(join(root, "crates/jazz-napi/package.json"), '{"files":["index.js"]}\n');
  mkdirSync(join(root, "crates/jazz-napi/artifacts"), { recursive: true });
  for (const [platform, target] of Object.entries(platforms)) {
    const manifest = expectedManifest(root, "napi", "release", target);
    manifest.nativeArtifactFingerprint = "a".repeat(64);
    manifest.packageInputs = "b".repeat(64);
    writeFileSync(
      join(root, "crates/jazz-napi/artifacts", `jazz-napi.${platform}.manifest.json`),
      JSON.stringify(manifest),
    );
  }
  stageNapiManifests(root);
  const node = join(root, "crates/jazz-napi/npm/linux-x64-gnu/jazz-napi.linux-x64-gnu.node");
  const manifest = JSON.parse(
    readFileSync(
      join(root, "crates/jazz-napi/npm/linux-x64-gnu/jazz-napi.linux-x64-gnu.manifest.json"),
      "utf8",
    ),
  );
  assert.equal(verifyPublishedNapiManifest(manifest, platforms["linux-x64-gnu"], node), null);
  assert.match(
    readFileSync(join(root, "crates/jazz-napi/package.json"), "utf8"),
    /provenance\/\*\.manifest\.json/,
  );

  const previewPlatforms = Object.fromEntries(
    Object.entries(platforms).filter(([platform]) => platform !== "win32-x64-msvc"),
  );
  const windowsManifest = join(
    root,
    "crates/jazz-napi/artifacts/jazz-napi.win32-x64-msvc.manifest.json",
  );
  const windowsManifestContents = readFileSync(windowsManifest, "utf8");
  rmSync(windowsManifest);
  assert.doesNotThrow(() => stageNapiManifests(root, previewPlatforms));
  assert.throws(() => stageNapiManifests(root), /missing provenance manifest for win32-x64-msvc/);
  writeFileSync(windowsManifest, windowsManifestContents);

  const darwinManifest = join(
    root,
    "crates/jazz-napi/artifacts/jazz-napi.darwin-x64.manifest.json",
  );
  const crossTargetMismatch = JSON.parse(readFileSync(darwinManifest, "utf8"));
  crossTargetMismatch.nativeArtifactFingerprint = "c".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));
  assert.throws(() => stageNapiManifests(root), /different ABI fingerprint or package inputs/);
  crossTargetMismatch.nativeArtifactFingerprint = "a".repeat(64);
  crossTargetMismatch.packageInputs = "d".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));
  assert.throws(() => stageNapiManifests(root), /different ABI fingerprint or package inputs/);
  crossTargetMismatch.packageInputs = "b".repeat(64);
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));

  for (const [field, value] of [["nativeArtifactFingerprint", "not-a-fingerprint"]]) {
    // Start every case from the same otherwise-valid sealed manifest. In
    // particular, a missing packageInputs receipt must not be masked by a
    // prior invalid fingerprint failure.
    const malformed = structuredClone(crossTargetMismatch);
    malformed[field] = value;
    writeFileSync(darwinManifest, JSON.stringify(malformed));
    assert.throws(() => stageNapiManifests(root), /missing native fingerprint or package inputs/);
  }
  writeFileSync(darwinManifest, JSON.stringify(crossTargetMismatch));

  // Equality alone cannot reject this: every target carries the same missing
  // value. The field validator must reject it before cross-target comparison.
  for (const platform of Object.keys(platforms)) {
    const path = join(root, "crates/jazz-napi/artifacts", `jazz-napi.${platform}.manifest.json`);
    const missingInputs = JSON.parse(readFileSync(path, "utf8"));
    delete missingInputs.packageInputs;
    writeFileSync(path, JSON.stringify(missingInputs));
  }
  assert.throws(() => stageNapiManifests(root), /missing native fingerprint or package inputs/);
  for (const [platform, target] of Object.entries(platforms)) {
    const restored = expectedManifest(root, "napi", "release", target);
    restored.nativeArtifactFingerprint = "a".repeat(64);
    restored.packageInputs = "b".repeat(64);
    writeFileSync(
      join(root, "crates/jazz-napi/artifacts", `jazz-napi.${platform}.manifest.json`),
      JSON.stringify(restored),
    );
  }

  writeFileSync(node, "stale");
  assert.match(
    verifyPublishedNapiManifest(manifest, platforms["linux-x64-gnu"], node),
    /does not match/,
  );
  writeFileSync(node, "linux-x64-gnu");
  rmSync(join(root, "crates/jazz-napi/artifacts/jazz-napi.darwin-x64.manifest.json"), {
    force: true,
  });
  assert.throws(() => stageNapiManifests(root), /missing provenance manifest/);
});
