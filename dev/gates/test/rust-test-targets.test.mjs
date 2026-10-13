import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");

// These crates set `autotests = false` and build their flat integration-test
// files as modules of one binary. Cargo no longer discovers `tests/*.rs` on its
// own there, so a new flat file that nobody lists would silently never compile
// or run. Every flat file must be a declared [[test]] target or be included by
// exactly one declared target's `#[path = "../<file>.rs"]` module.
const MERGED_TEST_CRATES = ["crates/jazz", "crates/groove", "crates/jazz-testkit"];

function declaredTestPaths(manifest) {
  // A [[test]] without `path` is inferred from its name, as Cargo does.
  return manifest
    .split(/^\[\[test\]\]\s*$/m)
    .slice(1)
    .map((block) => block.split(/^\[/m)[0])
    .map(
      (block) =>
        block.match(/^path\s*=\s*"([^"]+)"/m)?.[1] ??
        `tests/${block.match(/^name\s*=\s*"([^"]+)"/m)[1]}.rs`,
    );
}

export function unlistedFlatTests(crateDir) {
  const manifest = fs.readFileSync(path.join(crateDir, "Cargo.toml"), "utf8");
  if (!/^autotests\s*=\s*false$/m.test(manifest)) return { autotests: true, problems: [] };
  const declared = declaredTestPaths(manifest);
  const included = new Map();
  for (const target of declared) {
    const source = fs.readFileSync(path.join(crateDir, target), "utf8");
    for (const [, file] of source.matchAll(/#\[path = "\.\.\/([^"/]+\.rs)"\]/g))
      included.set(file, [...(included.get(file) ?? []), target]);
  }
  const problems = [];
  const testsDir = path.join(crateDir, "tests");
  for (const entry of fs.readdirSync(testsDir, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.endsWith(".rs")) continue;
    const relative = `tests/${entry.name}`;
    const owners = (declared.includes(relative) ? 1 : 0) + (included.get(entry.name)?.length ?? 0);
    if (owners !== 1)
      problems.push(`${path.relative(root, crateDir)}/${relative} is built by ${owners} targets`);
  }
  for (const file of included.keys())
    if (!fs.existsSync(path.join(testsDir, file)))
      problems.push(`${path.relative(root, crateDir)} includes missing tests/${file}`);
  return { autotests: false, problems };
}

test("every flat integration-test file in a merged-binary crate is built exactly once", () => {
  for (const crate of MERGED_TEST_CRATES) {
    const { autotests, problems } = unlistedFlatTests(path.join(root, crate));
    assert.equal(autotests, false, `${crate} is expected to build merged test binaries`);
    assert.deepEqual(problems, [], problems.join("\n"));
  }
});

test("an unlisted flat test file is reported", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rust-test-targets-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.mkdirSync(path.join(dir, "tests", "integration"), { recursive: true });
  fs.writeFileSync(
    path.join(dir, "Cargo.toml"),
    '[package]\nname = "x"\nautotests = false\n\n[[test]]\nname = "integration"\npath = "tests/integration/main.rs"\n',
  );
  fs.writeFileSync(
    path.join(dir, "tests", "integration", "main.rs"),
    '#[path = "../listed.rs"]\nmod listed;\n',
  );
  fs.writeFileSync(path.join(dir, "tests", "listed.rs"), "");
  assert.deepEqual(unlistedFlatTests(dir).problems, []);
  fs.writeFileSync(path.join(dir, "tests", "forgotten.rs"), "");
  assert.match(
    unlistedFlatTests(dir).problems.join("\n"),
    /tests\/forgotten\.rs is built by 0 targets/,
  );
});
