import assert from "node:assert/strict";
import { test } from "node:test";
import { checkManifest, npmAnswer, publishLevels } from "./npm-publish.mjs";
import { isPayload, requiredFiles } from "./maven-central.mjs";
import { pickGateRun, summarize } from "./rc-verdict.mjs";

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

const gateRun = (id, display_title, created_at) => ({ id, display_title, created_at });

test("a Cloud RC probe-only run never counts as the gate run", () => {
  const picked = pickGateRun([
    gateRun(1, "Release candidate dry run, changeset-release/release", "2026-11-19T10:00:00Z"),
    gateRun(2, "Cloud RC probe only, v2.0.0-alpha.68-rc.1", "2026-11-19T11:00:00Z"),
  ]);
  assert.equal(picked.id, 1);
  assert.equal(pickGateRun([gateRun(2, "Cloud RC probe only, x", "2026-11-19T11:00:00Z")]), null);
});

test("a real candidate wins over a newer dry run; otherwise the newest", () => {
  assert.equal(
    pickGateRun([
      gateRun(1, "Release candidate v2.0.0-alpha.68-rc.1", "2026-11-19T10:00:00Z"),
      gateRun(2, "Release candidate dry run, changeset-release/release", "2026-11-19T11:00:00Z"),
    ]).id,
    1,
  );
  assert.equal(
    pickGateRun([
      gateRun(1, "Release candidate dry run, a", "2026-11-19T10:00:00Z"),
      gateRun(2, "Release candidate dry run, b", "2026-11-19T11:00:00Z"),
    ]).id,
    2,
  );
});

test("verify-trust checks every package npm-publish publishes, named as npm escapes them", async () => {
  const { NPM_PACKAGES, escapedName } = await import("./verify-trust.mjs");
  const { REQUIRED, NAPI_PLATFORMS } = await import("./npm-publish.mjs");
  assert.equal(NPM_PACKAGES.length, 12);
  for (const name of REQUIRED) assert.ok(NPM_PACKAGES.includes(name), name);
  assert.equal(
    NPM_PACKAGES.filter((n) => n.startsWith("@garden-co/jazz-napi-")).length,
    NAPI_PLATFORMS,
  );
  assert.equal(escapedName("@garden-co/jazz-napi-darwin-x64"), "@garden-co%2fjazz-napi-darwin-x64");
  assert.equal(escapedName("jazz-tools"), "jazz-tools");
});

test("npm's answer is kept, without the tarball listing", () => {
  const out = [
    "npm notice 📦  jazz-rn-android@2.0.0-alpha.68",
    "npm notice Tarball Contents",
    "npm notice 101.2MB android/libjazz.so",
    "npm notice Publishing to https://registry.npmjs.org/ with tag latest and public access",
    "npm notice publish Signed provenance statement",
    "+ jazz-rn-android@2.0.0-alpha.68",
  ].join("\n");
  const answer = npmAnswer(out);
  assert.match(answer, /\+ jazz-rn-android@2\.0\.0-alpha\.68/);
  assert.match(answer, /Publishing to/);
  assert.doesNotMatch(answer, /Tarball|101\.2MB/);
});
