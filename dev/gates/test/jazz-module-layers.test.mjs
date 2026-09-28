import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { analyze, layerOf } from "../jazz-module-layers.mjs";

// Fixtures name their own files' layers, so they stay valid as real files move
// out into layer crates.
const fixtureLayers = {
  "schema.rs": "model",
  "tx.rs": "model",
  "protocol.rs": "protocol",
  "db.rs": "db",
};
function fixtureLayerOf(rel) {
  if (fixtureLayers[rel]) return fixtureLayers[rel];
  if (rel.startsWith("node/")) return "node";
  if (rel.startsWith("db/")) return "db";
  return "facade";
}
const analyzeFixture = (options) => analyze({ layerOf: fixtureLayerOf, ...options });

function crateFixture(files) {
  const src = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-layers-"));
  for (const [rel, body] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(src, rel)), { recursive: true });
    fs.writeFileSync(path.join(src, rel), body);
  }
  return src;
}

test("the jazz crate has no production upward references", () => {
  assert.deepEqual(
    analyze().map((v) => `${v.from}:${v.line} ${v.path}`),
    [],
  );
});

test("files map to the layers that will become crates", () => {
  assert.equal(layerOf("tools/client.rs"), "facade");
});

test("an upward reference is reported with its file and line", () => {
  const src = crateFixture({
    "lib.rs": "pub mod schema;\npub mod db;\n",
    "schema.rs":
      '// crate::db::Ignored in a comment\nconst S: &str = "crate::db::Ignored";\nuse crate::db::Handle;\n',
    "db.rs": "pub struct Handle;\n",
  });
  const violations = analyzeFixture({ src });
  assert.deepEqual(
    violations.map((v) => [v.from, v.line, v.to, v.fromLayer, v.toLayer, v.test]),
    [["schema.rs", 3, "db.rs", "model", "db", false]],
  );
});

test("a re-export counts against the module that re-exports it", () => {
  const src = crateFixture({
    "lib.rs": "pub mod tx;\npub mod tools;\npub mod protocol;\n",
    "tx.rs": "pub struct TxId;\n",
    "tools/mod.rs": "pub use crate::tx::TxId;\n",
    "protocol.rs": "use crate::tools::TxId;\nuse crate::tx::TxId as Direct;\n",
  });
  assert.deepEqual(
    analyzeFixture({ src }).map((v) => [v.from, v.to]),
    [["protocol.rs", "tools/mod.rs"]],
  );
});

test("super paths resolve through inline modules and test code is separated", () => {
  const src = crateFixture({
    "lib.rs": "pub mod node;\npub mod db;\n",
    "db.rs": "pub struct Handle;\n",
    "node/mod.rs":
      "mod inner {\n    use super::super::db::Handle;\n}\n#[cfg(test)]\nmod tests;\n#[cfg(test)]\nfn helper() { let _ = crate::db::Handle; }\n",
    "node/tests.rs": "use crate::db::Handle;\n",
  });
  const production = analyzeFixture({ src });
  assert.deepEqual(
    production.map((v) => [v.from, v.line]),
    [["node/mod.rs", 2]],
  );
  const all = analyzeFixture({ src, includeTests: true });
  assert.deepEqual(
    all
      .filter((v) => v.test)
      .map((v) => [v.from, v.line])
      .sort(),
    [
      ["node/mod.rs", 7],
      ["node/tests.rs", 1],
    ],
  );
});

test("super paths in an included file resolve from the includer's module", () => {
  const src = crateFixture({
    "lib.rs": "pub mod node;\npub mod db;\n",
    "db.rs": "pub struct Handle;\n",
    "node/mod.rs": 'include!("state/commit.rs");\n',
    "node/state/commit.rs": "fn probe() { let _ = super::db::Handle; }\n",
  });
  assert.deepEqual(
    analyzeFixture({ src }).map((v) => [v.from, v.line, v.to, v.path]),
    [["node/state/commit.rs", 1, "db.rs", "super::db::Handle"]],
  );
});

test("impls a split crate could not hold are reported", () => {
  const src = crateFixture({
    "lib.rs": "pub mod schema;\npub mod node;\n",
    "schema.rs": "pub struct RowUuid;\npub struct Alias;\npub struct Tag;\n",
    "node/mod.rs": [
      "use crate::schema::{Alias, RowUuid, Tag};",
      "pub struct Local;",
      "pub trait NodeExt {}",
      "impl RowUuid { fn inherent(&self) {} }",
      "impl groove::records::RecordField for Alias {}",
      "groove::impl_record_field_u64!(Tag);",
      "impl NodeExt for RowUuid {}",
      "impl From<Local> for RowUuid { fn from(_: Local) -> Self { RowUuid } }",
      "impl Local {}",
      "",
    ].join("\n"),
  });
  assert.deepEqual(
    analyzeFixture({ src })
      .map((v) => [v.line, v.to, v.path])
      .sort((a, b) => a[0] - b[0]),
    [
      [4, "schema.rs", "impl RowUuid"],
      [5, "schema.rs", "impl groove::records::RecordField for Alias"],
      [6, "schema.rs", "impl groove::records::RecordField for Tag"],
    ],
  );
});

test("modules re-exported from an extracted crate are not layer references", () => {
  const src = crateFixture({
    "lib.rs": "pub use jazz_types::ids;\npub mod schema;\npub mod node;\n",
    "schema.rs": "use crate::ids::RowUuid;\n",
    "node/mod.rs": "mod inner {\n    use super::super::ids::RowUuid;\n}\n",
  });
  assert.deepEqual(analyzeFixture({ src }), []);
});
