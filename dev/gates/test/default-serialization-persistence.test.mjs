import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { describeEndpoint, rustTokens, serializerReferences } from "../rust-token-audit.mjs";

const repository = path.resolve(import.meta.dirname, "../../..");
const gate = path.join(repository, "dev/gates/default-serialization-persistence.mjs");
const roots = new Set(["postcard", "serde_json", "ciborium", "bincode", "rmp_serde"]);

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-default-serde-gate-"));
  fs.mkdirSync(path.join(root, "dev/storage"), { recursive: true });
  fs.mkdirSync(path.join(root, "crates/jazz/src/node"), { recursive: true });
  fs.writeFileSync(
    path.join(root, "Cargo.toml"),
    '[workspace]\nmembers = ["crates/jazz"]\nresolver = "2"\n',
  );
  fs.writeFileSync(
    path.join(root, "crates/jazz/Cargo.toml"),
    '[package]\nname = "jazz"\nversion = "0.0.0"\nedition = "2021"\n\n[dependencies]\npostcard = { version = "1", features = ["alloc"] }\nserde_json = "1"\nserde = "1"\n',
  );
  fs.writeFileSync(path.join(root, "crates/jazz/src/lib.rs"), "pub mod node;\n");
  fs.writeFileSync(path.join(root, "crates/jazz/src/node/mod.rs"), "pub mod codec;\n");
  fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), "pub fn codec() {}\n");
  writeRegistry(root, []);
  return root;
}

function snapshots(extra = []) {
  return [
    {
      crate: "jazz",
      manifest: "crates/jazz/Cargo.toml",
      dependencies: [
        { crate: "postcard", package: "postcard" },
        { crate: "serde", package: "serde" },
        { crate: "serde_json", package: "serde_json" },
        ...extra,
      ].sort((left, right) => left.crate.localeCompare(right.crate)),
    },
  ];
}

function endpoints(source) {
  const relative = "crates/jazz/src/node/codec.rs";
  const tokens = rustTokens(source);
  return serializerReferences(tokens, roots).map((reference) =>
    describeEndpoint(tokens, reference, relative),
  );
}

function writeRegistry(root, allowed, direct = snapshots()) {
  fs.writeFileSync(
    path.join(root, "dev/storage/default-serialization-registry.json"),
    JSON.stringify({
      schemaVersion: 3,
      scope: { paths: ["crates/jazz/src/node"], serializerCrates: ["postcard", "serde_json"] },
      directDependencySnapshots: direct,
      allowances: allowed.map((endpoint, index) => ({
        id: "reviewed-" + index,
        classification: "fixture-only exact endpoint",
        endpoints: [endpoint],
      })),
    }),
  );
}

function writeSource(root, source, allowed = []) {
  fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), source);
  writeRegistry(root, allowed);
}

function run(root) {
  return spawnSync("node", [gate, "--root", root], { encoding: "utf8" });
}

function compile(root) {
  return spawnSync("cargo", ["check"], { cwd: root, encoding: "utf8" });
}

function assertRejectedAndCompiles(root, source, pattern) {
  writeSource(root, source);
  const result = run(root);
  assert.notEqual(result.status, 0, result.stderr);
  assert.match(result.stderr, pattern);
  const checked = compile(root);
  assert.equal(checked.status, 0, checked.stderr);
}

test("rejects raw, comment-separated, function-item, and macro serializer paths", () => {
  const root = fixture();
  try {
    assertRejectedAndCompiles(
      root,
      "pub fn f<T: serde::Serialize>(v: &T) { let _ = postcard /* comment */ :: to_allocvec(v); }\n",
      /unregistered default serialization reference postcard::to_allocvec/,
    );
    assertRejectedAndCompiles(
      root,
      "pub fn f<T: serde::Serialize>(v: &T) { let encode = postcard::to_allocvec::<T>; let _ = encode(v); }\n",
      /postcard::to_allocvec/,
    );
    assertRejectedAndCompiles(
      root,
      "macro_rules! encode { ($v:expr) => { postcard::to_allocvec($v) }; }\npub fn f<T: serde::Serialize>(v: &T) { let _ = encode!(v); }\n",
      /postcard::to_allocvec/,
    );
    assertRejectedAndCompiles(
      root,
      "macro_rules! encode { ($root:ident, $v:expr) => { $root::to_allocvec($v) }; }\npub fn f<T: serde::Serialize>(v: &T) { let _ = encode!(postcard, v); }\n",
      /unregistered default serialization reference postcard/,
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects import and extern aliases, raw identifiers, and alternate serializers", () => {
  const root = fixture();
  try {
    assertRejectedAndCompiles(
      root,
      "pub fn f<T: serde::Serialize>(v: &T) { let _ = r#postcard::to_allocvec(v); }\n",
      /postcard::to_allocvec/,
    );
    assertRejectedAndCompiles(
      root,
      "extern crate postcard as serializer_audit_alias;\npub fn f<T: serde::Serialize>(v: &T) { let _ = serializer_audit_alias::to_allocvec(v); }\n",
      /serializer extern crates are prohibited/,
    );
    assertRejectedAndCompiles(
      root,
      "use r#postcard as pc;\npub fn f<T: serde::Serialize>(v: &T) { let _ = pc::to_allocvec(v); }\n",
      /serializer imports are prohibited/,
    );
    assertRejectedAndCompiles(
      root,
      "use serde_json::{Value as JsonValue};\npub fn f(value: JsonValue) -> JsonValue { value }\n",
      /serializer imports are prohibited/,
    );
    const cargo = path.join(root, "crates/jazz/Cargo.toml");
    fs.appendFileSync(cargo, 'bincode = "1"\n');
    assertRejectedAndCompiles(
      root,
      "pub fn f<T: serde::Serialize>(v: &T) { let _ = bincode::serialize(v); }\n",
      /directDependencySnapshots differs/,
    );
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("requires an exact location, enclosing item, and cfg boundary for every allowance", () => {
  const root = fixture();
  try {
    const reviewed =
      '#[cfg(test)]\nmod tests {\n  fn semantic() { let _ = serde_json::from_str::<serde_json::Value>("null"); }\n}\n';
    const reviewedEndpoints = endpoints(reviewed);
    assert.ok(reviewedEndpoints.every((endpoint) => endpoint.boundary === "test"));
    assert.ok(
      reviewedEndpoints.every(
        (endpoint) =>
          endpoint.enclosing.modules.includes("tests") && endpoint.enclosing.item === "fn semantic",
      ),
    );
    writeSource(root, reviewed, reviewedEndpoints);
    assert.equal(run(root).status, 0, run(root).stderr);
    assert.equal(compile(root).status, 0);

    const moved =
      '#[cfg(test)]\nmod tests {\n  fn production_name() { let _ = serde_json::from_str::<serde_json::Value>("null"); }\n}\n';
    fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), moved);
    let result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      /unregistered default serialization reference|registered serializer endpoint is absent/,
    );
    assert.equal(compile(root).status, 0);

    const cfgRemoved =
      'mod tests {\n  fn semantic() { let _ = serde_json::from_str::<serde_json::Value>("null"); }\n}\n';
    fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), cfgRemoved);
    result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      /unregistered default serialization reference|registered serializer endpoint is absent/,
    );
    assert.equal(compile(root).status, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("requires an explicit exact registry entry for type-only serializer paths", () => {
  const root = fixture();
  try {
    const source = "pub fn f(value: serde_json::Value) -> serde_json::Value { value }\n";
    writeSource(root, source);
    let result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /serde_json::Value/);
    writeSource(root, source, endpoints(source));
    result = run(root);
    assert.equal(result.status, 0, result.stderr);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects a new direct serializer dependency even before source mentions it", () => {
  const root = fixture();
  try {
    fs.appendFileSync(path.join(root, "crates/jazz/Cargo.toml"), 'bincode = "1"\n');
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /directDependencySnapshots differs/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects a renamed direct dependency before its alias can be imported", () => {
  const root = fixture();
  try {
    const manifest = path.join(root, "crates/jazz/Cargo.toml");
    fs.writeFileSync(
      manifest,
      fs
        .readFileSync(manifest, "utf8")
        .replace('serde_json = "1"', 'json_codec = { package = "serde_json", version = "1" }'),
    );
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /directDependencySnapshots differs/);
    const checked = compile(root);
    assert.equal(checked.status, 0, checked.stderr);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
