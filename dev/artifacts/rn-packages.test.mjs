import assert from "node:assert/strict";
import test from "node:test";
import {
  mkdtempSync,
  mkdirSync,
  writeFileSync,
  readFileSync,
  readdirSync,
  cpSync,
  symlinkSync,
  rmSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
const repository = resolve(import.meta.dirname, "../..");
const write = (path, content) => {
  mkdirSync(resolve(path, ".."), { recursive: true });
  writeFileSync(path, content);
};
const json = (path, content) => write(path, JSON.stringify(content));

test("packed set exports a verified receipt; publish retries skip completed payloads and reject changed bytes", () => {
  const root = mkdtempSync(join(tmpdir(), "rn-packages-test-"));
  try {
    for (const script of ["rn-packages.mjs", "publish-rn-packages.mjs", "verify-rn-pack-size.mjs"])
      write(
        join(root, "dev/artifacts", script),
        readFileSync(join(repository, "dev/artifacts", script)),
      );
    const wrapper = join(root, "crates/jazz-rn");
    const version = "2.0.0-alpha.55";
    json(join(wrapper, "package.json"), {
      name: "jazz-rn",
      version,
      files: ["index.js"],
      dependencies: { "jazz-rn-ios": version, "jazz-rn-android": version },
    });
    write(join(wrapper, "index.js"), "export {};\n");
    // This fixture tests orchestration with a verifier stub. Binary/manifest
    // corruption sensitivity remains covered by the real relay verifier suite.
    write(
      join(wrapper, "scripts/verify-relay-artifacts.mjs"),
      'if (!process.argv.includes("--package-root")) throw new Error("missing packed root");',
    );
    for (const platform of ["android", "ios"]) {
      json(join(wrapper, "npm", platform, "package.json"), {
        name: `jazz-rn-${platform}`,
        version,
        files: ["payload.a"],
      });
      write(join(wrapper, "npm", platform, "payload.a"), `synthetic ${platform}`);
    }
    const githubEnv = join(root, "github-env");
    execFileSync(
      "node",
      [join(root, "dev/artifacts/rn-packages.mjs"), "pack", join(root, "dist")],
      { env: { ...process.env, GITHUB_ENV: githubEnv }, stdio: "pipe" },
    );
    const exported = readFileSync(githubEnv, "utf8").trim().split("=");
    assert.equal(exported[0], "JAZZ_RN_VERIFIED_RECEIPT");
    const receipt = JSON.parse(readFileSync(exported[1], "utf8"));
    assert.deepEqual(
      receipt.map((x) => x.name),
      ["jazz-rn-android", "jazz-rn-ios", "jazz-rn"],
    );
    const bin = join(root, "bin");
    const log = join(root, "npm-log");
    write(
      join(bin, "npm"),
      '#!/usr/bin/env node\nconst fs=require("fs");fs.appendFileSync(process.env.NPM_TEST_LOG,JSON.stringify(process.argv.slice(2))+"\\n");if(process.argv[2]==="view")process.exit(process.argv[3].startsWith("jazz-rn-android@")?0:1);',
    );
    execFileSync("chmod", ["+x", join(bin, "npm")]);
    const env = {
      ...process.env,
      PATH: `${bin}:${process.env.PATH}`,
      NPM_TEST_LOG: log,
      JAZZ_RN_VERIFIED_RECEIPT: exported[1],
    };
    const publish = join(root, "dev/artifacts/publish-rn-packages.mjs");
    execFileSync("node", [publish], { env, stdio: "pipe" });
    const calls = readFileSync(log, "utf8").trim().split("\n").map(JSON.parse);
    assert.deepEqual(
      calls.filter((x) => x[0] === "publish").map((x) => x[1]),
      [receipt[1].tarball, receipt[2].tarball],
    );
    writeFileSync(log, "");
    writeFileSync(receipt[2].tarball, "changed");
    assert.notEqual(spawnSync("node", [publish], { env }).status, 0);
    assert.equal(readFileSync(log, "utf8"), "");
    const missing = { ...env };
    delete missing.JAZZ_RN_VERIFIED_RECEIPT;
    assert.notEqual(spawnSync("node", [publish], { env: missing }).status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("payload resolution follows pnpm links, rejects version mismatch, and preview URLs bind source", () => {
  const root = mkdtempSync(join(tmpdir(), "rn-resolve-test-"));
  try {
    const wrapper = join(root, "store/jazz-rn");
    const payload = join(root, "store/android");
    const version = "2.0.0-alpha.55";
    const source = "a".repeat(40);
    const metadata = { name: "jazz-rn", version, dependencies: { "jazz-rn-android": version } };
    json(join(wrapper, "package.json"), metadata);
    json(join(payload, "package.json"), { name: "jazz-rn-android", version });
    json(join(payload, "android/jazz-native-relay.manifest.json"), { sourceRevision: source });
    mkdirSync(join(wrapper, "node_modules"), { recursive: true });
    symlinkSync(payload, join(wrapper, "node_modules/jazz-rn-android"));
    write(
      join(wrapper, "scripts/resolve-payload.cjs"),
      readFileSync(join(repository, "crates/jazz-rn/scripts/resolve-payload.cjs")),
    );
    const command = [join(wrapper, "scripts/resolve-payload.cjs"), "android"];
    assert.equal(execFileSync("node", command, { encoding: "utf8" }), payload);
    json(join(payload, "package.json"), { name: "jazz-rn-android", version: "wrong" });
    assert.notEqual(spawnSync("node", command).status, 0);
    json(join(payload, "package.json"), { name: "jazz-rn-android", version });
    metadata.dependencies["jazz-rn-android"] =
      `https://pkg.pr.new/garden-co/jazz/jazz-rn-android@${source}`;
    json(join(wrapper, "package.json"), metadata);
    assert.equal(execFileSync("node", command, { encoding: "utf8" }), payload);
    json(join(payload, "android/jazz-native-relay.manifest.json"), {
      sourceRevision: "b".repeat(40),
    });
    assert.notEqual(spawnSync("node", command).status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("preview rewrites both ordinary dependencies to the exact source URLs", () => {
  const root = mkdtempSync(join(tmpdir(), "rn-preview-test-"));
  try {
    const version = "2.0.0-alpha.55";
    const commit = "a".repeat(40);
    json(join(root, "package.json"), {
      name: "jazz-rn",
      version,
      dependencies: { "jazz-rn-ios": "workspace:*", "jazz-rn-android": "workspace:*" },
    });
    for (const platform of ["android", "ios"])
      json(join(root, "npm", platform, "package.json"), { name: `jazz-rn-${platform}`, version });
    execFileSync("node", [
      join(repository, "dev/artifacts/rn-preview-dependencies.mjs"),
      commit,
      root,
    ]);
    const metadata = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
    assert.deepEqual(
      metadata.dependencies,
      Object.fromEntries(
        ["ios", "android"].map((platform) => [
          `jazz-rn-${platform}`,
          `https://pkg.pr.new/garden-co/jazz/jazz-rn-${platform}@${commit}`,
        ]),
      ),
    );
    assert.equal(metadata.optionalDependencies, undefined);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("download-only native link jobs stage their payload before generating consumers", async () => {
  const { parse } = await import("yaml");
  const workflow = parse(
    readFileSync(join(repository, ".github/workflows/rn-native-artifacts.yml"), "utf8"),
  );
  for (const platform of ["android", "ios"]) {
    const steps = workflow.jobs[`${platform}-link`].steps;
    const download = steps.findIndex((step) => step.with?.name === `jazz-rn-relay-${platform}`);
    const stage = steps.findIndex((step) =>
      step.run?.includes(`rn-packages.mjs stage ${platform}`),
    );
    const consumer = steps.findIndex((step) => step.run?.includes("verify:expo:"));
    assert.ok(download >= 0 && stage > download && consumer > stage);
    assert.match(steps[stage].run, /JAZZ_NATIVE_RELAY_SOURCE_REVISION=/);
  }
});

test("Android SDK setup explicitly selects supported packages instead of deprecated defaults", async () => {
  const { parse } = await import("yaml");
  const directory = join(repository, ".github/workflows");
  const callsites = [];
  for (const file of readdirSync(directory).filter((name) => /\.ya?ml$/.test(name))) {
    const workflow = parse(readFileSync(join(directory, file), "utf8"));
    for (const [jobName, job] of Object.entries(workflow.jobs ?? {})) {
      for (const step of job.steps ?? []) {
        if (!step.uses?.startsWith("android-actions/setup-android@")) continue;
        const location = `${file}:${jobName}`;
        callsites.push(location);
        assert.equal(
          typeof step.with?.packages,
          "string",
          `${location} must override setup-android's legacy default`,
        );
        const packages = step.with.packages.trim().split(/\s+/);
        assert.ok(
          packages.includes("platform-tools"),
          `${location} must install adb/platform-tools`,
        );
        assert.ok(
          !packages.includes("tools"),
          `${location} must not request the retired SDK tools package`,
        );
      }
    }
  }
  for (const required of [
    "build-jazz-packages.yml:build-jazz-rn-android",
    "rn-native-artifacts.yml:android",
    "rn-native-artifacts.yml:android-link",
  ])
    assert.ok(callsites.includes(required), `missing Android SDK setup at ${required}`);
});
