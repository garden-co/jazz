import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile, rm, symlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { artifactPaths, seal, verify, verifyCodspeedVersion } from "./codspeed-artifact.mjs";

const root = fileURLToPath(new URL("../..", import.meta.url));
const identity = {
  source: "a".repeat(40),
  run: "12345",
  compiler: "rustc 1.93.1 (fixture)\nhost: aarch64-unknown-linux-gnu",
};

test("benchmark artifact handoff fails closed on stale or corrupted executables", async (t) => {
  const previous = process.cwd();
  const dir = await mkdtemp(path.join(os.tmpdir(), "jazz-codspeed-artifact-"));
  process.chdir(dir);
  try {
    const { binary, bundle } = artifactPaths("todo");
    await mkdir(path.dirname(binary), { recursive: true });
    await writeFile(binary, "benchmark fixture");
    await writeFile("cli", "CLI fixture");
    const reset = () => seal("todo", identity, "cli");
    const manifestFile = path.join(bundle, "manifest.json");
    await t.test("matching bundle and retry in the same run are accepted", async () => {
      const manifest = await reset();
      assert.deepEqual(await verify("todo", identity), manifest);
      assert.equal(manifest.format, "jazz-codspeed-benchmark-artifact-v1");
      assert.equal(manifest.contract.profile, "bench");
      assert.equal(manifest.files.walltime.length, 64);
      assert.deepEqual(await verify("todo", identity), manifest);
    });
    for (const [key, value] of [
      ["source", "b".repeat(40)],
      ["run", "67890"],
      ["compiler", `${identity.compiler}\nchanged compiler`],
    ]) {
      await t.test(`reject ${key} mismatch`, async () => {
        await reset();
        await assert.rejects(verify("todo", { ...identity, [key]: value }), /mismatch/);
      });
    }
    for (const [key, value] of [
      ["format", "unknown-v2"],
      ["workload", "permissioned-resources"],
      ["contract", { profile: "dev" }],
    ]) {
      await t.test(`reject wrong ${key}`, async () => {
        const manifest = await reset();
        await writeFile(manifestFile, JSON.stringify({ ...manifest, [key]: value }));
        await assert.rejects(verify("todo", identity));
      });
    }
    for (const name of ["walltime", "cargo-codspeed"]) {
      await t.test(`reject modified ${name}`, async () => {
        await reset();
        await writeFile(path.join(bundle, name), "corruption");
        await assert.rejects(verify("todo", identity), /hash mismatch/);
      });
    }
    await t.test("reject extra, absent and symlinked files", async () => {
      await reset();
      await writeFile(path.join(bundle, "unexpected"), "extra");
      await assert.rejects(verify("todo", identity));
      await rm(path.join(bundle, "unexpected"));
      await rm(path.join(bundle, "walltime"));
      await assert.rejects(verify("todo", identity));
      await symlink(path.resolve(binary), path.join(bundle, "walltime"));
      await assert.rejects(verify("todo", identity), /regular file/);
    });
    await t.test("reject arbitrary workload paths", () => {
      assert.throws(() => artifactPaths("../../escape"), /unknown workload/);
    });
  } finally {
    process.chdir(previous);
    await rm(dir, { recursive: true, force: true });
  }
});

test("timing shim adds only --timings to build, preserving all arguments", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "jazz-timed-cargo-"));
  try {
    const fake = path.join(dir, "cargo");
    await writeFile(
      fake,
      "#!/usr/bin/env node\nconsole.log(JSON.stringify(process.argv.slice(2)))\n",
      { mode: 0o755 },
    );
    for (const args of [
      ["build", "--config", "target.'cfg(all())'.rustflags=['-Cdebuginfo=2']"],
      ["metadata", "--format-version", "1"],
      ["codspeed", "--version"],
    ]) {
      const actual = JSON.parse(
        execFileSync("bash", [path.join(root, "dev/benchmarks/timed-cargo/cargo"), ...args], {
          encoding: "utf8",
          env: { ...process.env, JAZZ_REAL_CARGO: fake },
        }),
      );
      assert.deepEqual(actual, args[0] === "build" ? [...args, "--timings"] : args);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("version probe accepts the pinned CLI's exit-1 response, rejects real failures", async () => {
  const dir = await mkdtemp(path.join(os.tmpdir(), "jazz-codspeed-version-"));
  try {
    const cli = path.join(dir, "cargo-codspeed");
    for (const [status, output, accepted] of [
      [1, "cargo-codspeed 5.0.1\n\n", true],
      [0, "cargo-codspeed 5.0.1\n", true],
      [1, "cargo-codspeed 5.0.2\n", false],
      [1, "cargo-codspeed 5.0.1\nerror: broken\n", false],
      [2, "cargo-codspeed 5.0.1\n", false],
    ]) {
      await writeFile(
        cli,
        `#!/usr/bin/env node\nprocess.stderr.write(${JSON.stringify(output)});process.exit(${status});\n`,
        { mode: 0o755 },
      );
      if (accepted) verifyCodspeedVersion(cli);
      else assert.throws(() => verifyCodspeedVersion(cli));
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("workflow separates native builds from unchanged CodSpeed measurement", async () => {
  const workflow = await readFile(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
  const build = workflow
    .split("\n  native-workloads-build:\n")[1]
    .split("\n  native-workloads-walltime:\n")[0];
  const run = workflow.split("\n  native-workloads-walltime:\n")[1];
  assert.match(build, /runs-on: blacksmith-16vcpu-ubuntu-2204-arm/);
  assert.match(build, /CARGO_BUILD_JOBS: 16/);
  assert.match(build, /cache-workspace-crates: true/);
  assert.match(
    build,
    /cargo codspeed build -m walltime --package jazz-example-\$\{\{ matrix.workload \}\}-benchmark --bench walltime --features jazz-benchmark-guard\/mimalloc --locked/,
  );
  assert.match(run, /runs-on: codspeed-macro\n/);
  assert.doesNotMatch(run, /cargo (install|build|codspeed build)/);
  assert.match(run, /codspeed-artifact.mjs install/);
  assert.match(
    run,
    /cargo codspeed run -m walltime --package jazz-example-\$\{\{ matrix.workload \}\}-benchmark --bench walltime/,
  );
  assert.match(run, /RUST_MIN_STACK: 4194304/);
});
