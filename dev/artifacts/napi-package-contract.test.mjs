import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { stageNapiLoader } from "./stage-napi-loader.mjs";

const packageDir = resolve(import.meta.dirname, "../../crates/jazz-napi");
const packageSource = JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8"));
const target = "linux-x64-gnu";
const fingerprint = "a".repeat(64);

function fixture({ profile = "release" } = {}) {
  const root = mkdtempSync(join(tmpdir(), "jazz-napi-package-"));
  const packageDir = join(root, "crates", "jazz-napi");
  const generation = join(packageDir, ".native-artifacts", "generation-release");
  mkdirSync(generation, { recursive: true });
  const packageJson = { ...packageSource, scripts: {} };
  writeFileSync(join(packageDir, "package.json"), JSON.stringify(packageJson));
  for (const file of ["index.cjs", "index.mjs", "index.js", "index.d.ts", "native-binding.cjs"])
    writeFileSync(join(packageDir, file), "fixture\n");
  writeFileSync(
    join(packageDir, "native-binding.pointer.cjs"),
    `const nativeBinding = require("./.native-artifacts/generation-release/index.js");\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: "${fingerprint}" };\n`,
  );
  writeFileSync(join(generation, `jazz-napi.${target}.node`), "fixture native bytes\n");
  writeFileSync(join(generation, "index.js"), "module.exports = {};\n");
  writeFileSync(
    join(generation, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile, nativeArtifactFingerprint: fingerprint }),
  );
  // A prior platform assembly can leave these ignored root-level outputs next
  // to the selected platform. They must be pruned before npm evaluates files.
  writeFileSync(join(packageDir, "jazz-napi.darwin-x64.node"), "stale darwin native bytes\n");
  writeFileSync(
    join(packageDir, "jazz-napi.darwin-x64.manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: "b".repeat(64) }),
  );
  // The package glob accepts arbitrary suffixes too; one must never survive
  // merely because it is not a currently supported target triple.
  writeFileSync(join(packageDir, "jazz-napi.attacker.node"), "unexpected native bytes\n");
  writeFileSync(
    join(packageDir, "jazz-napi.attacker.manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: "c".repeat(64) }),
  );
  // This is deliberately an ignored historical output. A package glob must
  // never make it into the final inventory.
  mkdirSync(join(packageDir, ".native-artifacts", "generation-stale"));
  writeFileSync(join(packageDir, ".native-artifacts", "generation-stale", "old.node"), "stale");
  return { root, packageDir };
}

test("NAPI packing stages one sealed release generation and excludes historical generations", () => {
  const fixtureRoot = fixture();
  try {
    assert.equal(packageSource.scripts.prepack, "node ../../dev/artifacts/stage-napi-loader.mjs");
    stageNapiLoader(fixtureRoot.root, target);
    assert.equal(existsSync(join(fixtureRoot.packageDir, "jazz-napi.darwin-x64.node")), false);
    assert.equal(
      existsSync(join(fixtureRoot.packageDir, "jazz-napi.darwin-x64.manifest.json")),
      false,
    );
    assert.equal(existsSync(join(fixtureRoot.packageDir, "jazz-napi.attacker.node")), false);
    assert.equal(
      existsSync(join(fixtureRoot.packageDir, "jazz-napi.attacker.manifest.json")),
      false,
    );
    const output = execFileSync("npm", ["pack", "--dry-run", "--json"], {
      cwd: fixtureRoot.packageDir,
      encoding: "utf8",
    });
  const jsonStart = output.indexOf("[\n");
  assert.notEqual(jsonStart, -1, `npm pack did not emit its JSON inventory:\n${output}`);
  const [receipt] = JSON.parse(output.slice(jsonStart));
  assert.ok(receipt, "npm pack did not report a package receipt");
  const files = new Set(receipt.files.map((file) => file.path));

    assert.ok(files.has("native-loader.cjs"));
    assert.ok(files.has("native-artifact-fingerprint.cjs"));
    assert.deepEqual(
      [...files].filter((file) => /^jazz-napi\.[^.]+\.node$/.test(file)),
      [`jazz-napi.${target}.node`],
    );
    assert.deepEqual(
      [...files].filter((file) => /^jazz-napi\.[^.]+\.manifest\.json$/.test(file)),
      [`jazz-napi.${target}.manifest.json`],
    );
  assert.ok(
    [...files].every((file) => !file.startsWith(".native-artifacts/")),
    `packed historical native generation: ${[...files].find((file) => file.startsWith(".native-artifacts/"))}`,
  );
  // A platform package contains exactly one selected native binary plus its
  // small JS/declaration surface.  This catches a future broad files glob even
  // if it happens to omit a particular historic directory in a fixture.
  assert.ok(receipt.unpackedSize < 110_000_000, `unexpected unpacked size: ${receipt.unpackedSize}`);
  } finally {
    rmSync(fixtureRoot.root, { recursive: true, force: true });
  }
});

test("NAPI prepack refuses a debug generation before package inventory", () => {
  const fixtureRoot = fixture({ profile: "debug" });
  try {
    assert.throws(() => stageNapiLoader(fixtureRoot.root, target), /wrong kind\/profile/);
    assert.equal(
      readFileSync(join(fixtureRoot.packageDir, "native-binding.pointer.cjs"), "utf8").includes(
        ".native-artifacts/generation-release",
      ),
      true,
    );
  } finally {
    rmSync(fixtureRoot.root, { recursive: true, force: true });
  }
});
