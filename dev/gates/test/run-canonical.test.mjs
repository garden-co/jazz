import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const runner = path.join(root, "dev/gates/run-canonical.sh");

test("canonical gates do not advertise the removed jazz-server package gate", () => {
  const source = fs.readFileSync(runner, "utf8");
  assert.doesNotMatch(source, /cargo-test-jazz-server/);
  assert.doesNotMatch(source, /cargo test -p\s+jazz-server/);
});

test("the removed jazz-server gate is rejected as an unknown selector", () => {
  const result = spawnSync("bash", [runner, "--only", "cargo-test-jazz-server"], {
    cwd: root,
    encoding: "utf8",
  });
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}\n${result.stderr}`, /unknown gate id/);
});

test("canonical and documented soaks execute the exact ignored oracle and reject a missing selection", (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-soak-contract-"));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const oracle = "node::tests::harness::m3_maintained_one_shot_differential_oracle";
  const source = path.join(directory, "oracle.rs");
  const binary = path.join(directory, "oracle");
  fs.writeFileSync(
    source,
    `mod node { mod tests { mod harness {
    #[test] #[ignore] fn m3_maintained_one_shot_differential_oracle() {
      panic!("planted ignored oracle failure");
    }
    #[test] fn m3_maintained_one_shot_differential_oracle_control() {}
  } } }`,
  );
  const compiled = spawnSync("rustc", ["--test", source, "-o", binary], { encoding: "utf8" });
  assert.equal(compiled.status, 0, compiled.stderr);
  // Replace only Cargo's compile/package layer; use real libtest selection.
  fs.writeFileSync(
    path.join(directory, "cargo"),
    `#!/usr/bin/env bash
set -euo pipefail
while (($#)); do
  if [[ "$1" == -- ]]; then shift; exec "$ORACLE_BINARY" "\${filter:-}" "$@"; fi
  case "$1" in
    test|--no-default-features|--lib) shift ;;
    -p|--features) shift 2 ;;
    *) filter="$1"; shift ;;
  esac
done
exit 2
`,
    { mode: 0o755 },
  );
  const env = { ...process.env, PATH: `${directory}:${process.env.PATH}`, ORACLE_BINARY: binary };
  const run = () =>
    spawnSync("bash", [runner, "--only", "m3-maintained-one-shot", "--output-dir", directory], {
      cwd: root,
      encoding: "utf8",
      env,
    });
  const result = run();
  assert.equal(result.status, 1, result.stdout + result.stderr);
  assert.match(result.stdout, /planted ignored oracle failure/);
  assert.match(result.stdout, /0 passed; 1 failed; 0 ignored;.*1 filtered out/);
  assert.match(result.stdout, /inventory matched 1 test/);

  // A renamed oracle must fail inventory resolution, rather than pass with zero tests.
  fs.writeFileSync(source, "#[test] fn unrelated() {}\n");
  const recompiled = spawnSync("rustc", ["--test", source, "-o", binary], { encoding: "utf8" });
  assert.equal(recompiled.status, 0, recompiled.stderr);
  const missing = run();
  assert.equal(missing.status, 1, missing.stdout + missing.stderr);
  assert.match(missing.stdout, /filter matched no selected test inventory entries/);

  const docs = fs.readFileSync(path.join(root, "AGENTS.md"), "utf8");
  for (const count of [300, 2000]) {
    assert.ok(docs.includes(`JAZZ_SEED_COUNT=${count} dev/t --exact ${oracle} -- --ignored`));
  }
});
