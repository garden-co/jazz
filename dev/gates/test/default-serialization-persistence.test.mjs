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
      // The gate's explicit refresh command is the only mechanical migration
      // helper. The resulting policy still needs review; fixtures use it so
      // their Cargo-resolved dependency snapshot matches this machine.
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
  const refreshed = spawnSync(
    "node",
    [gate, "--root", root, "--refresh-registry-snapshot"],
    { encoding: "utf8" },
  );
  assert.equal(refreshed.status, 0, refreshed.stderr);
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
    assertRejectedAndCompiles(
      root,
      "macro_rules! nested { ($group:tt) => {}; }\nmacro_rules! outer { ($group:tt) => {}; }\npub fn f() { outer!({ nested!([postcard]) }); }\n",
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
      /unregistered default serialization reference bincode::serialize/,
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

test("does not leak cfg(test) through every semicolon item kind into production endpoints", () => {
  const root = fixture();
  try {
    const source =
      '#[cfg(test)] const TEST_CONST: usize = 1;\n#[cfg(test)] static TEST_STATIC: usize = 1;\n#[cfg(test)] type TestAlias = usize;\n#[cfg(test)] use std::fmt::Debug;\n#[cfg(test)] extern "C" { fn fixture_only(); }\n#[cfg(test)] macro_rules! fixture_only { () => {}; }\npub fn semantic() { let _ = serde_json::from_str::<serde_json::Value>("null"); }\n';
    const reviewed = endpoints(source);
    assert.equal(reviewed.length, 2);
    assert.ok(reviewed.every((endpoint) => endpoint.boundary === "production"));
    writeSource(root, source, reviewed);
    assert.equal(run(root).status, 0, run(root).stderr);
    assert.equal(compile(root).status, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("cfg-bound exact endpoints survive every declaration form and reject a same-span cfg removal", () => {
  const root = fixture();
  try {
    // These are deliberately real Rust declarations, not token-only snippets.
    // In particular, the semicolon declarations used to have no enclosing
    // structural identity, so replacing their cfg while preserving the
    // serializer path's location transferred a reviewed test allowance to
    // production.
    const source =
      'pub struct Holder;\n' +
      '#[cfg(test)] type Alias = serde_json::Value;\n' +
      '#[cfg(test)] const VALUE: Option<serde_json::Value> = None;\n' +
      '#[cfg(test)] static STATIC_VALUE: Option<serde_json::Value> = None;\n' +
      '#[cfg(test)] struct Tuple(serde_json::Value);\n' +
      '#[cfg(test)] struct Unit;\n' +
      '#[cfg(test)] struct Named { #[cfg(test)] field: serde_json::Value }\n' +
      '#[cfg(test)] extern "Rust" { fn imported(_: serde_json::Value); }\n' +
      'pub trait Associated { #[cfg(test)] type Value; #[cfg(test)] const VALUE: Option<serde_json::Value>; }\n' +
      'impl Associated for Holder { #[cfg(test)] type Value = serde_json::Value; #[cfg(test)] const VALUE: Option<serde_json::Value> = None; }\n' +
      '#[cfg(test)] mod nested { pub type Alias = serde_json::Value; }\n';
    const reviewed = endpoints(source);
    assert.ok(reviewed.length >= 10);
    assert.ok(reviewed.every((endpoint) => endpoint.boundary === "test"));
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.item === "type Alias"));
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.item === "const VALUE"));
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.item === "static STATIC_VALUE"));
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.item === "struct Tuple"));
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.item === "field field"));
    assert.ok(
      reviewed.some(
        (endpoint) =>
          endpoint.enclosing.items.includes("impl Associated for Holder") &&
          endpoint.enclosing.items.includes("type Value"),
      ),
    );
    assert.ok(
      reviewed.some(
        (endpoint) =>
          endpoint.enclosing.modules.includes("nested") && endpoint.enclosing.item === "type Alias",
      ),
    );
    writeSource(root, source, reviewed);
    let result = run(root);
    assert.equal(result.status, 0, result.stderr);
    assert.equal(compile(root).status, 0, compile(root).stderr);

    // Spaces preserve every serializer token's byte/line/column/span.  Only
    // the declaration boundary changes; the old allowance must therefore be
    // rejected rather than silently becoming a production endpoint.
    fs.writeFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      source.replaceAll("#[cfg(test)]", "            "),
    );
    result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(
      result.stderr,
      /unregistered default serialization reference|registered serializer endpoint is absent/,
    );
    assert.equal(compile(root).status, 0, compile(root).stderr);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("retains cfg(test) across ordinary nested blocks without leaking it afterward", () => {
  const root = fixture();
  try {
    const source =
      '#[cfg(test)] fn receipt() { if true { let _ = 1; } let _ = serde_json::from_str::<serde_json::Value>("null"); }\npub fn production() { let _ = serde_json::from_str::<serde_json::Value>("null"); }\n';
    const reviewed = endpoints(source);
    assert.equal(reviewed.length, 4);
    assert.ok(reviewed.slice(0, 2).every((endpoint) => endpoint.boundary === "test"));
    assert.ok(reviewed.slice(2).every((endpoint) => endpoint.boundary === "production"));
    writeSource(root, source, reviewed);
    assert.equal(run(root).status, 0, run(root).stderr);
    assert.equal(compile(root).status, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("endpoint identity contains the full impl stack, not only a function name", () => {
  const root = fixture();
  try {
    const source =
      'struct First; struct Second; struct Other;\nimpl First { fn encode() { let _ = serde_json::from_str::<serde_json::Value>("null"); } }\nimpl Second { fn encode() { let _ = serde_json::from_str::<serde_json::Value>("null"); } }\n';
    const reviewed = endpoints(source);
    assert.ok(reviewed.some((endpoint) => endpoint.enclosing.items.length === 2));
    writeSource(root, source, reviewed);
    assert.equal(run(root).status, 0, run(root).stderr);
    const swapped = source.replace("impl First", "impl Other");
    fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), swapped);
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /unregistered default serialization reference|registered serializer endpoint is absent/);
    assert.equal(compile(root).status, 0, compile(root).stderr);
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

test("dependency feature, requirement, source, and resolved-version snapshot drift fails before scanning source", () => {
  const root = fixture();
  try {
    const manifest = path.join(root, "crates/jazz/Cargo.toml");
    fs.writeFileSync(
      manifest,
      fs
        .readFileSync(manifest, "utf8")
        .replace('postcard = { version = "1", features = ["alloc"] }', 'postcard = { version = "1", default-features = false, features = ["alloc", "use-std"] }'),
    );
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

test("accepted renamed serializer aliases remain governed and unclassified dependency drift fails closed", () => {
  const root = fixture();
  try {
    const manifest = path.join(root, "crates/jazz/Cargo.toml");
    fs.writeFileSync(
      manifest,
      fs
        .readFileSync(manifest, "utf8")
        .replace('serde_json = "1"', 'json_codec = { package = "serde_json", version = "1" }')
        .replace('serde = "1"', 'serde = "1"\nron = "0.8"'),
    );
    // Accept the dependency snapshot first, then demonstrate that a renamed
    // root is still a governed serializer and that removing any direct-entry
    // classification fails closed.
    writeRegistry(root, []);
    fs.writeFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      "pub fn f() { let _ = json_codec::to_string(&42); }\n",
    );
    let result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /json_codec::to_string/);
    const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
    const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
    registry.dependencyClassifications = registry.dependencyClassifications.filter(
      (entry) => !entry.dependency.includes("ron|ron|"),
    );
    fs.writeFileSync(registryPath, JSON.stringify(registry));
    result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /every direct external dependency|unclassified direct dependency/);
    assert.equal(compile(root).status, 0, compile(root).stderr);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
