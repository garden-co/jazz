import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";

const repository = path.resolve(import.meta.dirname, "../../..");
const gate = path.join(repository, "dev/gates/default-serialization-persistence.mjs");

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-default-serde-gate-"));
  fs.mkdirSync(path.join(root, "dev/storage"), { recursive: true });
  fs.mkdirSync(path.join(root, "crates/jazz/src/node"), { recursive: true });
  fs.writeFileSync(
    path.join(root, "dev/storage/default-serialization-registry.json"),
    JSON.stringify({
      schemaVersion: 2,
      scope: { paths: ["crates/jazz/src/node"] },
      allowances: [],
    }),
  );
  fs.writeFileSync(path.join(root, "crates/jazz/src/node/codec.rs"), "fn codec() {}\n");
  return root;
}

function run(root) {
  return spawnSync("node", [gate, "--root", root], { encoding: "utf8" });
}

test("rejects an unregistered raw serializer in a persistence-owning module", () => {
  const root = fixture();
  try {
    fs.appendFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      "let bytes = postcard::to_allocvec(&value);\n",
    );
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /unregistered default serialization postcard::to_allocvec/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("requires an exact registry receipt for a deliberate non-durable use", () => {
  const root = fixture();
  try {
    fs.writeFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      "let bytes = postcard::to_allocvec(&value);\n",
    );
    const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
    fs.writeFileSync(
      registryPath,
      JSON.stringify({
        schemaVersion: 2,
        scope: { paths: ["crates/jazz/src/node"] },
        allowances: [
          {
            id: "test-only",
            path: "crates/jazz/src/node/codec.rs",
            api: "postcard::to_allocvec",
            expectedOccurrences: 1,
            classification: "test-only temporary bytes",
          },
        ],
      }),
    );
    const clean = run(root);
    assert.equal(clean.status, 0, clean.stderr);
    fs.appendFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      "let more = postcard::to_allocvec(&other);\n",
    );
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /expected 1/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects every registered convenience family, including to_writer", () => {
  const root = fixture();
  try {
    const file = path.join(root, "crates/jazz/src/node/codec.rs");
    for (const api of [
      "postcard::to_stdvec",
      "postcard::to_slice",
      "postcard::to_extend",
      "postcard::to_io",
      "postcard::to_allocvec_cobs",
      "postcard::to_stdvec_cobs",
      "postcard::to_slice_cobs",
      "postcard::from_bytes_cobs",
      "postcard::take_from_bytes_cobs",
      "serde_json::to_writer",
      "serde_json::to_writer_pretty",
      "serde_json::to_string_pretty",
      "serde_json::from_reader",
      "serde_json::Serializer::new",
      "serde_json::Serializer::pretty",
      "serde_json::Deserializer::from_slice",
      "serde_json::Deserializer::from_str",
      "serde_json::Deserializer::from_reader",
      "bincode::serialize_into",
      "bincode::deserialize_from",
      "rmp_serde::to_vec_named",
      "rmp_serde::from_read",
    ]) {
      fs.writeFileSync(file, `fn codec() { let _ = ${api}(&value); }\n`);
      const result = run(root);
      assert.notEqual(result.status, 0, api);
      assert.match(result.stderr, new RegExp(`unregistered default serialization ${api}`));
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects serializer imports in every alias, group, glob, and raw form", () => {
  const root = fixture();
  try {
    const file = path.join(root, "crates/jazz/src/node/codec.rs");
    for (const [name, source] of [
      ["namespace alias", "use postcard as pc; fn f() { pc::to_allocvec(&value); }"],
      ["direct alias", "use postcard::to_allocvec as encode; fn f() { encode(&value); }"],
      [
        "grouped direct",
        "use postcard::{to_allocvec as encode, from_bytes}; fn f() { encode(&value); }",
      ],
      [
        "nested group",
        "use postcard::{experimental::{serialized_size as size}}; fn f() { size(&value); }",
      ],
      ["nested glob", "use postcard::{experimental::*}; fn f() { serialized_size(&value); }"],
      ["glob", "use postcard::*; fn f() { to_allocvec(&value); }"],
      ["leading crate", "use ::postcard::to_allocvec; fn f() { to_allocvec(&value); }"],
      ["raw alias", "use postcard::to_allocvec as r#encode; fn f() { r#encode(&value); }"],
      [
        "json namespace alias",
        "use serde_json as json; fn f() { json::to_writer(&mut out, &value); }",
      ],
      [
        "json deserializer type",
        "use serde_json::Deserializer as JsonDecoder; fn f() { JsonDecoder::from_slice(&value); }",
      ],
    ]) {
      fs.writeFileSync(file, `${source}\n`);
      const result = run(root);
      assert.notEqual(result.status, 0, name);
      assert.match(result.stderr, /serializer imports are prohibited at the persistence boundary/);
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("does not mistake locally named values for serializer imports", () => {
  const root = fixture();
  try {
    fs.writeFileSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      "fn f() { let postcard = (); let serde_json = (); let encode = || (); encode(); }\n",
    );
    assert.equal(run(root).status, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("ignores serializer spellings in Rust comments and string literals", () => {
  const root = fixture();
  try {
    const file = path.join(root, "crates/jazz/src/node/codec.rs");
    fs.writeFileSync(
      file,
      [
        "/* postcard::to_slice(&value); /* serde_json::to_writer(&mut out, &value); */ */",
        "// postcard::to_allocvec(&value);",
        'const DOC: &str = "serde_json::from_str(&value)";',
        'const RAW: &str = r#"postcard::to_allocvec(&value)"#;',
        "fn f() {}",
      ].join("\n"),
    );
    const clean = run(root);
    assert.equal(clean.status, 0, clean.stderr);
    fs.appendFileSync(file, "\nfn f() { postcard::to_slice(&value, &mut output); }\n");
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /postcard::to_slice/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("permits an explicitly registered fully-qualified canonical endpoint", () => {
  const root = fixture();
  try {
    const file = path.join(root, "crates/jazz/src/node/codec.rs");
    fs.writeFileSync(file, "fn f() { serde_json::from_str(&value); }\n");
    const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
    fs.writeFileSync(
      registryPath,
      JSON.stringify({
        schemaVersion: 2,
        scope: { paths: ["crates/jazz/src/node"] },
        allowances: [
          {
            id: "semantic-json-endpoint",
            path: "crates/jazz/src/node/codec.rs",
            api: "serde_json::from_str",
            expectedOccurrences: 1,
            classification: "explicit test endpoint",
          },
        ],
      }),
    );
    assert.equal(run(root).status, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects broad registry patterns and a persistence owner added after the registry", () => {
  const root = fixture();
  try {
    const registryPath = path.join(root, "dev/storage/default-serialization-registry.json");
    const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
    registry.allowances.push({
      id: "broad-is-not-an-api",
      path: "crates/jazz/src/node/codec.rs",
      api: "serde_json::.*",
      expectedOccurrences: 1,
      classification: "must fail schema validation",
    });
    fs.writeFileSync(registryPath, JSON.stringify(registry));
    let result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /invalid allowance/);

    registry.allowances = [];
    fs.writeFileSync(registryPath, JSON.stringify(registry));
    fs.writeFileSync(
      path.join(root, "crates/jazz/src/node/new_persistence_owner.rs"),
      "fn write() { let _ = serde_json::to_writer(&mut output, &value); }\n",
    );
    result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /new_persistence_owner.rs.*serde_json::to_writer/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("rejects a restored flat-join postcard digest in its persistence owner", () => {
  const root = fixture();
  try {
    fs.renameSync(
      path.join(root, "crates/jazz/src/node/codec.rs"),
      path.join(root, "crates/jazz/src/node/maintained_subscription_view.rs"),
    );
    fs.appendFileSync(
      path.join(root, "crates/jazz/src/node/maintained_subscription_view.rs"),
      "fn digest(values: Vec<u8>) { let _ = postcard::to_allocvec(&values); }\n",
    );
    const result = run(root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /maintained_subscription_view.rs.*postcard::to_allocvec/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
