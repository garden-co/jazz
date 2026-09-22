import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const scanner = fileURLToPath(new URL("../rust-test-index.pl", import.meta.url));

function index(source) {
  const directory = mkdtempSync(join(tmpdir(), "jazz-test-index-"));
  const file = join(directory, "fixture.rs");
  try {
    writeFileSync(file, source);
    const result = spawnSync("perl", [scanner], { input: `${file}\n`, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        const [name, path] = line.split("\x1f");
        assert.equal(path, file);
        return name;
      });
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("indexes ordinary and async test attributes through declaration metadata", () => {
  assert.deepEqual(
    index(`
#[test]
fn ordinary() {}
#[tokio::test(
    flavor = "current_thread"
)]
#[ignore = "#123: manual"]
/// Documentation can separate attributes from the declaration.
async fn asynchronous() {}
`),
    ["ordinary", "asynchronous"],
  );
});

test("indexes local_tokio_test declarations with long documentation and attributes", () => {
  assert.deepEqual(
    index(`
local_tokio_test! {
/// A wrapped test.
/// With more than four lines of documentation.
/// Actors:
/// Alice writes.
/// Bob reads.
#[ignore = "#123: manual"]
async fn wrapped() {
    fn nested_helper() {}
}
}
fn adjacent_helper() {}
local_tokio_test! {
#[cfg(
    feature = "testing"
)]
async fn second() {}
}
`),
    ["wrapped", "second"],
  );
});

test("does not classify helpers, unrelated macros, or test mentions as tests", () => {
  assert.deepEqual(
    index(`
#[test]
fn actual_test() {}
fn adjacent_helper() {}
/// This documentation mentions #[test].
fn documented_helper() {}
/* #[tokio::test] is only a comment. */
async fn commented_helper() {}
unrelated_macro! {
    async fn not_a_test() {}
}
local_tokio_test! {
    const VALUE: u8 = 1;
    async fn invalid_wrapper_body() {}
}
`),
    ["actual_test"],
  );
});

test("indexes the three array-FK regressions cited by INV-QUERY-22", () => {
  const file = fileURLToPath(
    new URL("../../../crates/jazz-testkit/tests/query/subqueries.rs", import.meta.url),
  );
  const result = spawnSync("perl", [scanner], { input: `${file}\n`, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  const names = new Set(
    result.stdout
      .trim()
      .split("\n")
      .map((line) => line.split("\x1f")[0]),
  );
  for (const name of [
    "array_subquery_materializes_uuid_array_refs_in_order_with_duplicates",
    "array_fk_subscription_preserves_occurrences_through_updates_and_reorders",
    "array_fk_filters_and_windows_apply_to_reference_occurrences",
  ])
    assert.ok(names.has(name), `missing test: ${name}`);
});
