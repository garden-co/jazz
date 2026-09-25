import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile, rm, symlink } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  artifactPaths,
  buildArgs,
  measureSettings,
  measurementWorkspace,
  runArgs,
  seal,
  sourcePathFlags,
  verify,
  workloads,
  verifyCodspeedVersion,
  verifyMeasurementWorkspace,
} from "./codspeed-artifact.mjs";

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
    const { binaries, bundle } = artifactPaths("todo");
    const binary = binaries.walltime;
    await mkdir(path.dirname(binary), { recursive: true });
    await writeFile(binary, "benchmark fixture");
    await writeFile("cli", "CLI fixture");
    const reset = () => seal("todo", identity, "cli");
    const manifestFile = path.join(bundle, "manifest.json");
    await t.test("matching bundle and retry in the same run are accepted", async () => {
      const manifest = await reset();
      assert.deepEqual(await verify("todo", identity), manifest);
      assert.equal(manifest.format, "jazz-codspeed-benchmark-artifact-v2");
      assert.equal(manifest.contract.profile, "bench");
      assert.deepEqual(manifest.contract.sourcePaths, {
        kind: "measurement-workspace-absolute",
        root: measurementWorkspace,
      });
      assert.equal(manifest.contract.features, "jazz-benchmark-guard/mimalloc");
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
      ["format", "jazz-codspeed-benchmark-artifact-v1"],
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
    await t.test("reject the previous relative-path artifact contract", async () => {
      const manifest = await reset();
      manifest.contract = { ...manifest.contract, sourcePaths: "workspace-relative" };
      await writeFile(manifestFile, JSON.stringify(manifest));
      await assert.rejects(verify("todo", identity), /build contract mismatch/);
    });
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
      for (const name of ["../../escape", "constructor", "__proto__"]) {
        assert.throws(() => artifactPaths(name), /unknown workload/);
        assert.throws(() => buildArgs(name), /unknown workload/);
      }
    });
  } finally {
    process.chdir(previous);
    await rm(dir, { recursive: true, force: true });
  }
});

test("multi-bench workloads seal and install every bench executable", async () => {
  const previous = process.cwd();
  const dir = await mkdtemp(path.join(os.tmpdir(), "jazz-codspeed-artifact-multi-"));
  process.chdir(dir);
  try {
    const { binaries, bundle } = artifactPaths("w1");
    assert.deepEqual(binaries, {
      ahead_current: "target/codspeed/walltime/jazz-example-benchmark-w1/ahead_current",
      reads_memory_walltime:
        "target/codspeed/walltime/jazz-example-benchmark-w1/reads_memory_walltime",
      reads_rocksdb_walltime:
        "target/codspeed/walltime/jazz-example-benchmark-w1/reads_rocksdb_walltime",
    });
    for (const binary of Object.values(binaries)) {
      await mkdir(path.dirname(binary), { recursive: true });
      await writeFile(binary, binary);
    }
    await writeFile("cli", "CLI fixture");
    const manifest = await seal("w1", identity, "cli");
    assert.deepEqual(Object.keys(manifest.files).sort(), [
      "ahead_current",
      "cargo-codspeed",
      "reads_memory_walltime",
      "reads_rocksdb_walltime",
    ]);
    assert.equal(manifest.contract.features, null);
    assert.deepEqual(await verify("w1", identity), manifest);
    // A bundle sealed for one workload never verifies as another.
    await assert.rejects(verify("groove-ivm", identity));
    await rm(path.join(bundle, "reads_rocksdb_walltime"));
    await assert.rejects(verify("w1", identity));
  } finally {
    process.chdir(previous);
    await rm(dir, { recursive: true, force: true });
  }
});

test("each workload builds and runs exactly what it measured on the macro runner", () => {
  // The commands these workloads used when they compiled on codspeed-macro
  // (and, for the native examples, on the ARM builder). Moving the build must
  // not change a package, bench or feature.
  const native = (name) =>
    `--package jazz-example-${name}-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc`;
  const previous = {
    todo: native("todo"),
    "permissioned-resources": native("permissioned-resources"),
    "policy-scoped-documents": native("policy-scoped-documents"),
    "band-chat": native("band-chat"),
    "world-tour": native("world-tour"),
    "big-label": "--package jazz-example-big-label-benchmark --bench ingest_walltime --bench loads",
    w1: "--package jazz-example-benchmark-w1 --bench reads_memory_walltime --bench reads_rocksdb_walltime --bench ahead_current",
    "route-subscription": "--package jazz --bench route_subscription_curve --features testing",
    "groove-ivm": "--package groove --bench pull_vs_snapshot --bench steady_state",
    "selective-hydration": "--package jazz --bench selective_global_hydration --features testing",
  };
  assert.deepEqual(workloads, Object.keys(previous));
  for (const [workload, args] of Object.entries(previous)) {
    assert.equal(buildArgs(workload).join(" "), args);
    assert.equal(runArgs(workload).join(" "), args.replace(/ --features \S+$/, ""));
    const cli = (action) =>
      execFileSync(
        "node",
        [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), action, workload],
        {
          encoding: "utf8",
        },
      ).trim();
    assert.equal(cli("build-args"), buildArgs(workload).join(" "));
    assert.equal(cli("run-args"), runArgs(workload).join(" "));
  }
  assert.throws(() =>
    execFileSync(
      "node",
      [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), "build-args", "nope"],
      {
        stdio: "pipe",
      },
    ),
  );
});

test("source remapping uses the exact measurement root and rejects checkout drift", () => {
  assert.equal(
    sourcePathFlags("/home/runner/_work/jazz/jazz"),
    "--remap-path-prefix=/home/runner/_work/jazz/jazz=/actions-runner/_work/jazz/jazz",
  );
  verifyMeasurementWorkspace("/actions-runner/_work/jazz/jazz");
  for (const wrong of [".", "/home/runner/_work/jazz/jazz", "/actions-runner/_work/other/other"]) {
    assert.throws(() => verifyMeasurementWorkspace(wrong), /checkout path changed/);
  }
  for (const wrong of [".", "/workspace with spaces", "/workspace=other"]) {
    assert.throws(() => sourcePathFlags(wrong));
  }
  assert.equal(
    execFileSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), "rustflags"], {
      cwd: root,
      encoding: "utf8",
    }).trim(),
    sourcePathFlags(root),
  );
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

test("measurement keeps each workload's former thread stack and timeout", () => {
  // Former macro jobs: native examples raised RUST_MIN_STACK and had 20 minutes;
  // the others ran with the default stack under their own job limits.
  const stack = "4194304";
  assert.deepEqual(measureSettings(), {
    todo: { min_stack: stack, timeout: 20 },
    "permissioned-resources": { min_stack: stack, timeout: 20 },
    "policy-scoped-documents": { min_stack: stack, timeout: 20 },
    "band-chat": { min_stack: stack, timeout: 20 },
    "world-tour": { min_stack: stack, timeout: 20 },
    "big-label": { min_stack: "", timeout: 25 },
    w1: { min_stack: "", timeout: 40 },
    "route-subscription": { min_stack: "", timeout: 25 },
    "groove-ivm": { min_stack: "", timeout: 40 },
    "selective-hydration": { min_stack: "", timeout: 35 },
  });
  const cli = (action) =>
    execFileSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), action], {
      encoding: "utf8",
    });
  assert.deepEqual(JSON.parse(cli("matrix")), workloads);
  assert.deepEqual(JSON.parse(cli("measure")), measureSettings());
});

test("workflow separates native builds from unchanged CodSpeed measurement", async () => {
  const workflow = await readFile(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
  const build = workflow
    .split("\n  native-workloads-build:\n")[1]
    .split("\n  native-workloads-walltime:\n")[0];
  const run = workflow.split("\n  native-workloads-walltime:\n")[1];
  assert.match(build, /runs-on: blacksmith-16vcpu-ubuntu-2204-arm/);
  assert.match(build, /CARGO_BUILD_JOBS: 16/);
  assert.match(build, /cache-targets: false/);
  assert.ok(
    build.includes(
      'RUSTFLAGS="$(node dev/benchmarks/codspeed-artifact.mjs rustflags)"\n          export RUSTFLAGS',
    ),
  );
  assert.ok(
    build.includes(
      'args="$(node dev/benchmarks/codspeed-artifact.mjs build-args ${{ matrix.workload }})"\n' +
        '          read -ra args <<<"$args"\n' +
        '          /usr/bin/time -v cargo codspeed build -m walltime "${args[@]}" --locked',
    ),
  );
  assert.match(run, /runs-on: codspeed-macro\n/);
  // One failed build must not skip the other workloads' measurements.
  assert.ok(
    run.includes("if: ${{ !cancelled() && needs.native-workloads-build.result != 'skipped' }}\n"),
  );
  assert.doesNotMatch(run, /cargo (install|build|codspeed build)/);
  assert.match(run, /codspeed-artifact.mjs install \$\{\{ matrix.workload \}\}/);
  assert.ok(
    run.includes(
      'args="$(node dev/benchmarks/codspeed-artifact.mjs run-args ${{ matrix.workload }})"\n' +
        '          echo "run-args=$args" >> "$GITHUB_OUTPUT"',
    ),
  );
  assert.match(
    run,
    /run: cargo codspeed run -m walltime \$\{\{ steps.install.outputs.run-args \}\}\n/,
  );
  // Both matrices and the measurement settings come from the artifact script.
  const fromPlan = "${{ fromJSON(needs.native-workloads-plan.outputs.workloads) }}";
  assert.ok(build.includes(`workload: ${fromPlan}\n`));
  assert.ok(run.includes(`workload: ${fromPlan}\n`));
  const measure = "fromJSON(needs.native-workloads-plan.outputs.measure)[matrix.workload]";
  assert.ok(run.includes(`timeout-minutes: \${{ ${measure}.timeout }}\n`));
  assert.ok(run.includes(`RUST_MIN_STACK: \${{ ${measure}.min_stack }}\n`));
  assert.doesNotMatch(run, /RUST_MIN_STACK: 4194304/);
  assert.ok(workflow.includes('measure="$(node dev/benchmarks/codspeed-artifact.mjs measure)"'));
  // No other job compiles on the measurement runner.
  for (const job of workflow.split(/\n  (?=[a-z-]+:\n)/)) {
    if (/runs-on: codspeed-macro/.test(job)) {
      assert.doesNotMatch(job, /cargo (install|build)|cargo codspeed build/, job.split("\n")[0]);
    }
  }
});

test("compiler cache restores across revisions while isolating compatible workloads", async () => {
  const workflow = await readFile(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
  const cache = workflow
    .split("      - name: Restore native compiler outputs\n")[1]
    .split("      - name:")[0];
  const key = cache.match(/          key: (.+)/)[1];
  const prefix = cache.match(/          restore-keys: \|\n            (.+)/)[1];
  const render = (template, overrides = {}) => {
    const values = {
      "matrix.workload": "todo",
      "runner.os": "Linux",
      "runner.arch": "ARM64",
      "github.sha": "source-a",
      ...overrides,
    };
    return template.replace(/\$\{\{ (.*?) \}\}/g, (_, expression) =>
      expression.startsWith("hashFiles(") ? (overrides.lockHash ?? "lock-a") : values[expression],
    );
  };
  const oldKey = render(key);
  const nextKey = render(key, { "github.sha": "source-b" });
  const nextPrefix = render(prefix, { "github.sha": "source-b" });
  assert.notEqual(oldKey, nextKey, "each source can save new workspace outputs");
  assert.ok(oldKey.startsWith(nextPrefix), "next source must restore previous source outputs");
  for (const change of [
    { "matrix.workload": "policy-scoped-documents" },
    { "runner.os": "macOS" },
    { "runner.arch": "X64" },
    { lockHash: "lock-b" },
  ]) {
    assert.ok(!oldKey.startsWith(render(prefix, change)), "incompatible cache must not restore");
  }
  assert.match(prefix, /ubuntu2204-rust1\.93\.1-codspeed5\.0\.1-per-workload-absolute-remap/);
  assert.match(cache, /path: target\/release/);
  assert.match(workflow, /key: \$\{\{ steps.native-build-cache.outputs.cache-primary-key \}\}/);
  assert.doesNotMatch(workflow, /JAZZ_BENCHMARK_SOURCE/);
});
