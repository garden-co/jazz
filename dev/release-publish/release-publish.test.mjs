import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";
import { checkManifest, npmAnswer, publishLevels } from "./npm-publish.mjs";
import { isPayload, requiredFiles, servedUrls } from "./maven-central.mjs";
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

test("Maven Central counts as served when repo1 has both POMs", () => {
  assert.deepEqual(servedUrls("2.0.0-alpha.69"), [
    "https://repo1.maven.org/maven2/tools/jazz/jazz-kotlin/2.0.0-alpha.69/jazz-kotlin-2.0.0-alpha.69.pom",
    "https://repo1.maven.org/maven2/tools/jazz/jazz-kotlin-android/2.0.0-alpha.69/jazz-kotlin-android-2.0.0-alpha.69.pom",
  ]);
});

const repoFile = (path) => readFileSync(new URL(`../../${path}`, import.meta.url), "utf8");
const kotlinChannel = (...args) =>
  execFileSync("bash", [fileURLToPath(new URL("./kotlin-channel.sh", import.meta.url)), ...args], {
    encoding: "utf8",
  }).trim();

test("Central is paused from beta.1; the versions before it are smoked from Central", () => {
  assert.equal(kotlinChannel(), "github-release");
  for (const version of ["2.0.0-alpha.66", "2.0.0-alpha.70", "2.0.0-beta.0"])
    assert.equal(kotlinChannel(version), "central", version);
  for (const version of ["2.0.0-beta.1", "2.0.0-beta.10", "2.0.0-beta.0.1-rc.1"])
    assert.equal(kotlinChannel(version), "github-release", version);
});

const workflow = parse(repoFile(".github/workflows/release-publish.yml"));

/** The value of one of release-publish.yml's job expressions. */
function evaluate(expression, { channel, mode, portalDryRun = false, results = {} }) {
  const inputs = {
    mode,
    "maven-publishing-type": "AUTOMATIC",
    "maven-portal-dry-run": portalDryRun,
  };
  const needs = new Proxy(
    {},
    {
      get: (_, job) => ({
        result: results[job] ?? "success",
        outputs: { kotlin_channel: channel },
      }),
    },
  );
  const js = expression
    .trim()
    .replace(/^\$\{\{([\s\S]*)\}\}$/, "$1")
    .replace(/\.([A-Za-z_][\w-]*)/g, '["$1"]')
    .replace(/\s+/g, " ");
  return new Function("inputs", "needs", "cancelled", `return (${js});`)(
    inputs,
    needs,
    () => false,
  );
}

test("while Central is paused the Maven leg gets no secrets, uploads nothing and isn't awaited", () => {
  const { maven, finalize, "maven-published": served } = workflow.jobs;
  for (const mode of ["publish", "dry-run"])
    for (const portalDryRun of [false, true]) {
      const run = { channel: "github-release", mode, portalDryRun };
      assert.equal(evaluate(maven.environment, run), "", JSON.stringify(run));
      assert.equal(evaluate(maven.env.PORTAL, run), false, JSON.stringify(run));
      assert.equal(evaluate(served.if, run), false, JSON.stringify(run));
      // The leg still runs, and finalize follows it with the served job skipped.
      assert.equal(evaluate(maven.if, run), true);
      const results = { "maven-published": "skipped" };
      assert.equal(evaluate(finalize.if, { ...run, results }), true);
    }
  for (const step of maven.steps)
    if (JSON.stringify(step).includes("secrets.")) assert.match(JSON.stringify(step), /PORTAL/);
});

test("a failed or skipped Maven bundle still blocks the GitHub release", () => {
  for (const maven of ["failure", "skipped"])
    assert.equal(
      evaluate(workflow.jobs.finalize.if, {
        channel: "github-release",
        mode: "publish",
        results: { maven },
      }),
      false,
    );
});

test("switched back to central, the Maven leg publishes as before", () => {
  const { maven, "maven-published": served } = workflow.jobs;
  const publish = { channel: "central", mode: "publish" };
  assert.equal(evaluate(maven.environment, publish), "publish");
  assert.equal(evaluate(maven.env.PORTAL, publish), true);
  assert.equal(evaluate(served.if, publish), true);
  const dry = { channel: "central", mode: "dry-run" };
  assert.equal(evaluate(maven.environment, dry), "");
  assert.equal(evaluate(maven.env.PORTAL, { ...dry, portalDryRun: true }), true);
});

test("the jar and AAR installs declare exactly the dependencies of the Kotlin builds", () => {
  const MAVEN = /\b(?:api|implementation)\("([^"]+:[^"]+:[^"]+)"\)/g;
  const coordinates = (text) => [...text.matchAll(MAVEN)].map((match) => match[1]).sort();
  const jvm = coordinates(repoFile("packages/jazz-kotlin/build.gradle.kts"));
  const android = coordinates(repoFile("packages/jazz-kotlin-android/build.gradle.kts"));
  assert.equal(jvm.length, 3);
  assert.equal(android.length, 3);
  assert.deepEqual(
    coordinates(repoFile("dev/adopter-gate/t9.sh")).filter((c) => !c.startsWith("tools.jazz:")),
    jvm,
  );

  const install = (path, heading) => {
    const section = repoFile(path).split(`${heading}\n`)[1].split("\n## ")[0];
    const blocks = [...section.matchAll(/```kotlin[^\n]*\n([\s\S]*?)```/g)].map((match) =>
      coordinates(match[1]).filter((c) => !c.startsWith("tools.jazz:")),
    );
    assert.deepEqual(blocks, [jvm, android], path);
  };
  install("docs/content/docs/setup/kotlin.mdx", "## Install");
  install(
    "docs/content/docs/operating/backups-and-upgrades.mdx",
    "## Upgrading from beta.0 to beta.1",
  );
});

test("the Maven bundle takes jazz-kotlin-jvm only when the tree has it (Kotlin Multiplatform)", async () => {
  const { treeArtifacts } = await import("./maven-central.mjs");
  const { mkdtempSync, mkdirSync, rmSync } = await import("node:fs");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const tree = mkdtempSync(join(tmpdir(), "tree-"));
  try {
    assert.deepEqual(treeArtifacts(tree, "1"), ["jazz-kotlin", "jazz-kotlin-android"]);
    mkdirSync(join(tree, "jazz-kotlin-jvm", "1"), { recursive: true });
    assert.deepEqual(treeArtifacts(tree, "1"), ["jazz-kotlin", "jazz-kotlin-jvm", "jazz-kotlin-android"]);
    assert.equal(requiredFiles("jazz-kotlin-jvm", "1")[0], "jazz-kotlin-jvm-1.jar");
  } finally {
    rmSync(tree, { recursive: true, force: true });
  }
});

test("the KMP root needs only its POM and module metadata", () => {
  assert.deepEqual(requiredFiles("jazz-kotlin", "1", true), ["jazz-kotlin-1.pom", "jazz-kotlin-1.module"]);
  assert.equal(requiredFiles("jazz-kotlin", "1")[0], "jazz-kotlin-1.jar");
});
