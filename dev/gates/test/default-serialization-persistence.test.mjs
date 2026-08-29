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
      schemaVersion: 1,
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
        schemaVersion: 1,
        scope: { paths: ["crates/jazz/src/node"] },
        allowances: [
          {
            id: "test-only",
            path: "crates/jazz/src/node/codec.rs",
            pattern: "postcard::to_allocvec",
            expectedOccurrences: 1,
            classification: "test-only temporary bytes",
          },
        ],
      }),
    );
    assert.equal(run(root).status, 0);
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
