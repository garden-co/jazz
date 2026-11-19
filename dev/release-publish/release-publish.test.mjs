import assert from "node:assert/strict";
import { test } from "node:test";
import { checkManifest, publishLevels } from "./npm-publish.mjs";
import { isPayload, requiredFiles } from "./maven-central.mjs";
import { summarize } from "./rc-verdict.mjs";

const pkg = (name, deps = []) => ({ name, deps });

test("npm packages publish after the packages they depend on", () => {
  const levels = publishLevels([
    pkg("create-jazz"),
    pkg("jazz-tools", ["jazz-wasm", "jazz-napi", "jazz-rn", "react"]),
    pkg("jazz-napi", ["@garden-co/jazz-napi-linux-x64-gnu"]),
    pkg("@garden-co/jazz-napi-linux-x64-gnu"),
    pkg("jazz-wasm"),
    pkg("jazz-rn", ["jazz-rn-android", "jazz-rn-ios"]),
    pkg("jazz-rn-android"),
    pkg("jazz-rn-ios"),
  ]).map((level) => level.map((p) => p.name));
  assert.deepEqual(levels, [
    ["@garden-co/jazz-napi-linux-x64-gnu", "jazz-rn-android", "jazz-rn-ios", "jazz-wasm"],
    ["jazz-napi", "jazz-rn"],
    ["jazz-tools"],
    ["create-jazz"],
  ]);
});

test("a dependency cycle is refused", () => {
  assert.throws(() => publishLevels([pkg("a", ["b"]), pkg("b", ["a"])]), /cycle/);
});

test("manifests must be at the version, without workspace ranges", () => {
  checkManifest({ name: "x", version: "1.0.0", dependencies: { y: "1.0.0" } }, "1.0.0");
  assert.throws(() => checkManifest({ name: "x", version: "1.0.1" }, "1.0.0"), /not 1.0.0/);
  assert.throws(
    () =>
      checkManifest(
        { name: "x", version: "1.0.0", optionalDependencies: { y: "workspace:*" } },
        "1.0.0",
      ),
    /workspace/,
  );
});

test("the Maven bundle signs payloads only", () => {
  assert.equal(isPayload("jazz-kotlin-1.jar"), true);
  assert.equal(isPayload("jazz-kotlin-1.jar.asc"), false);
  assert.equal(isPayload("jazz-kotlin-1.pom.sha1"), false);
  assert.equal(isPayload("maven-metadata-local.xml"), false);
  assert.deepEqual(requiredFiles("jazz-kotlin-android", "1")[0], "jazz-kotlin-android-1.aar");
});

const run = { id: 1, html_url: "https://example.invalid/run/1", conclusion: "success" };

test("RC holds are listed with their owners, and don't refuse", () => {
  const { lines, annotations, refuse } = summarize(
    {
      verdict: "HOLD",
      holds: [
        { name: "Cloud RC probe: needs-infra", owner: "#infra", detail: "probe" },
        { name: "Adopter gate: NEEDS_REVIEW", owner: "Anselm", detail: "sizes: jazz-wasm +3.2%" },
      ],
      jobs: [{ job: "examples-e2e", result: "success", ok: true }],
    },
    run,
  );
  assert.equal(refuse, false);
  assert.match(lines[0], /HOLD/);
  assert.ok(
    lines.includes("- **Adopter gate: NEEDS_REVIEW** (owner: Anselm): sizes: jazz-wasm +3.2%"),
  );
  assert.equal(annotations.length, 2);
});

test("NO-GO refuses; GO doesn't", () => {
  assert.equal(
    summarize(
      { verdict: "NO-GO", holds: [], jobs: [{ job: "x", result: "failure", ok: false }] },
      run,
    ).refuse,
    true,
  );
  assert.equal(summarize({ verdict: "GO", holds: [], jobs: [] }, run).refuse, false);
});

test("a gate run without rc-verdict is reported, and refused only if it failed", () => {
  assert.match(summarize(null, run).lines.join("\n"), /predates/);
  assert.equal(summarize(null, run).refuse, false);
  assert.equal(summarize(null, { ...run, conclusion: "failure" }).refuse, true);
  assert.equal(summarize(null, null).refuse, false);
});
