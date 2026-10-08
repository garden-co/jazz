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
  contractFor,
  currentSuite,
  groupMeasureSettings,
  groupWorkloads,
  groups,
  measureSettings,
  measurementWorkspace,
  runArgs,
  runCommand,
  seal,
  sourcePathFlags,
  suiteGroups,
  suiteWorkloads,
  verify,
  workloads,
  verifyCodspeedVersion,
  verifyMeasurementWorkspace,
} from "./codspeed-artifact.mjs";

// The workflow sets the suite for every job, this test step included; each
// test below names the suite it checks.
delete process.env.JAZZ_CODSPEED_SUITE;

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
    const { binaries, bundle } = artifactPaths("stage-plan");
    const binary = binaries.walltime;
    await mkdir(path.dirname(binary), { recursive: true });
    await writeFile(binary, "benchmark fixture");
    await writeFile("cli", "CLI fixture");
    const reset = () => seal("stage-plan", identity, "cli");
    const manifestFile = path.join(bundle, "manifest.json");
    await t.test("matching bundle and retry in the same run are accepted", async () => {
      const manifest = await reset();
      assert.deepEqual(await verify("stage-plan", identity), manifest);
      assert.equal(manifest.format, "jazz-codspeed-benchmark-artifact-v2");
      assert.equal(manifest.contract.profile, "bench");
      assert.deepEqual(manifest.contract.sourcePaths, {
        kind: "measurement-workspace-absolute",
        root: measurementWorkspace,
      });
      assert.equal(manifest.contract.features, "jazz-benchmark-guard/mimalloc");
      assert.equal(manifest.files.walltime.length, 64);
      assert.deepEqual(await verify("stage-plan", identity), manifest);
    });
    for (const [key, value] of [
      ["source", "b".repeat(40)],
      ["run", "67890"],
      ["compiler", `${identity.compiler}\nchanged compiler`],
    ]) {
      await t.test(`reject ${key} mismatch`, async () => {
        await reset();
        await assert.rejects(verify("stage-plan", { ...identity, [key]: value }), /mismatch/);
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
        await assert.rejects(verify("stage-plan", identity));
      });
    }
    for (const name of ["walltime", "cargo-codspeed"]) {
      await t.test(`reject modified ${name}`, async () => {
        await reset();
        await writeFile(path.join(bundle, name), "corruption");
        await assert.rejects(verify("stage-plan", identity), /hash mismatch/);
      });
    }
    await t.test("reject the previous relative-path artifact contract", async () => {
      const manifest = await reset();
      manifest.contract = { ...manifest.contract, sourcePaths: "workspace-relative" };
      await writeFile(manifestFile, JSON.stringify(manifest));
      await assert.rejects(verify("stage-plan", identity), /build contract mismatch/);
    });
    await t.test("reject extra, absent and symlinked files", async () => {
      await reset();
      await writeFile(path.join(bundle, "unexpected"), "extra");
      await assert.rejects(verify("stage-plan", identity));
      await rm(path.join(bundle, "unexpected"));
      await rm(path.join(bundle, "walltime"));
      await assert.rejects(verify("stage-plan", identity));
      await symlink(path.resolve(binary), path.join(bundle, "walltime"));
      await assert.rejects(verify("stage-plan", identity), /regular file/);
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
    const { binaries, bundle } = artifactPaths("big-label");
    assert.deepEqual(binaries, {
      ingest_walltime: "target/codspeed/walltime/jazz-example-big-label-benchmark/ingest_walltime",
      loads: "target/codspeed/walltime/jazz-example-big-label-benchmark/loads",
    });
    for (const binary of Object.values(binaries)) {
      await mkdir(path.dirname(binary), { recursive: true });
      await writeFile(binary, binary);
    }
    await writeFile("cli", "CLI fixture");
    const manifest = await seal("big-label", identity, "cli");
    assert.deepEqual(Object.keys(manifest.files).sort(), [
      "cargo-codspeed",
      "ingest_walltime",
      "loads",
    ]);
    assert.equal(manifest.contract.features, null);
    assert.deepEqual(await verify("big-label", identity), manifest);
    // A bundle sealed for one workload never verifies as another.
    await assert.rejects(verify("groove-ivm", identity));
    await rm(path.join(bundle, "loads"));
    await assert.rejects(verify("big-label", identity));
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
    "stage-plan": native("stage-plan"),
    "band-chat": native("band-chat"),
    "band-book": native("band-book"),
    "world-tour": native("world-tour"),
    wequencer: native("wequencer"),
    "poster-shop": native("poster-shop"),
    "record-player": native("record-player"),
    "epic-drop": native("epic-drop"),
    "jamazon-warehouse": native("jamazon-warehouse"),
    "music-agent": native("music-agent"),
    "big-label": "--package jazz-example-big-label-benchmark --bench ingest_walltime --bench loads",
    "permissioned-resources": native("permissioned-resources"),
    "groove-ivm": "--package groove --bench pull_vs_snapshot --bench steady_state",
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
    "stage-plan": { min_stack: stack, timeout: 40 },
    "band-chat": { min_stack: stack, timeout: 20 },
    "band-book": { min_stack: stack, timeout: 20 },
    "world-tour": { min_stack: stack, timeout: 20 },
    wequencer: { min_stack: stack, timeout: 20 },
    "poster-shop": { min_stack: stack, timeout: 20 },
    "record-player": { min_stack: stack, timeout: 20 },
    "epic-drop": { min_stack: stack, timeout: 20 },
    "jamazon-warehouse": { min_stack: stack, timeout: 20 },
    "music-agent": { min_stack: stack, timeout: 20 },
    "big-label": { min_stack: "", timeout: 25 },
    "permissioned-resources": { min_stack: stack, timeout: 20 },
    "groove-ivm": { min_stack: "", timeout: 40 },
  });

  const cli = (action) =>
    execFileSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), action], {
      encoding: "utf8",
    });
  assert.deepEqual(JSON.parse(cli("matrix")), workloads);
  assert.deepEqual(JSON.parse(cli("measure")), measureSettings());
});

test("workload groups share jobs without changing what each workload measures", () => {
  const grouped = groups.flatMap((group) => groupWorkloads(group));
  assert.deepEqual([...grouped].sort(), [...workloads].sort(), "every workload once");
  assert.equal(new Set(grouped).size, workloads.length);
  assert.ok(groups.length < workloads.length, "grouping reduces CodSpeed jobs");
  const settings = measureSettings();
  for (const group of groups) {
    // A group's measurement job gets the sum of its workloads' limits.
    assert.equal(
      groupMeasureSettings()[group].timeout,
      groupWorkloads(group).reduce((sum, w) => sum + settings[w].timeout, 0),
    );
    // One command per workload, in order, with that workload's own run
    // arguments and thread stack; the first failure stops the group.
    const commands = runCommand(group).split(" && ");
    assert.equal(commands.length, groupWorkloads(group).length);
    groupWorkloads(group).forEach((workload, index) => {
      const env = settings[workload].min_stack
        ? `RUST_MIN_STACK=${settings[workload].min_stack}`
        : "env -u RUST_MIN_STACK";
      assert.equal(
        commands[index],
        `${env} cargo codspeed run -m walltime ${runArgs(workload).join(" ")}`,
      );
      assert.doesNotMatch(commands[index], /--features/);
    });
  }
  assert.throws(() => groupWorkloads("../everything"), /unknown group/);
  const cli = (...args) =>
    execFileSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), ...args], {
      encoding: "utf8",
    }).trim();
  assert.deepEqual(JSON.parse(cli("groups")), groups);
  assert.deepEqual(JSON.parse(cli("group-measure")), groupMeasureSettings());
  assert.equal(cli("group-workloads", "engine"), "groove-ivm");
  assert.equal(cli("run-command", "engine"), runCommand("engine"));
});

test("workflow separates grouped native builds from unchanged CodSpeed measurement", async () => {
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
  // One Cargo invocation per workload of the group, each with its own
  // arguments (and so its own features), in one shared target directory.
  assert.ok(
    build.includes(
      'workloads="$(node dev/benchmarks/codspeed-artifact.mjs group-workloads ${{ matrix.group }})"\n' +
        "          for workload in $workloads; do\n" +
        '            args="$(node dev/benchmarks/codspeed-artifact.mjs build-args "$workload")"\n' +
        '            read -ra args <<<"$args"\n',
    ),
  );
  assert.ok(
    build.includes(
      '            /usr/bin/time -v cargo codspeed build -m walltime "${args[@]}" --locked\n',
    ),
  );
  assert.match(build, /codspeed-artifact\.mjs seal "\$workload"/);
  // The marker keeps target/ the artifact root for a one-workload group.
  assert.ok(build.includes("target/codspeed-group.txt\n            target/codspeed-artifact-*/"));
  assert.match(run, /runs-on: codspeed-macro\n/);
  // One failed build must not skip the other workloads' measurements.
  assert.ok(
    run.includes("if: ${{ !cancelled() && needs.native-workloads-build.result != 'skipped' }}\n"),
  );
  assert.doesNotMatch(run, /cargo (install|build|codspeed build)/);
  assert.match(run, /codspeed-artifact\.mjs install "\$workload"/);
  assert.match(
    run,
    /name: codspeed-native-\$\{\{ matrix.group \}\}-\$\{\{ github.sha \}\}\n\s+path: target\/\n/,
  );
  assert.ok(
    run.includes(
      'command="$(node dev/benchmarks/codspeed-artifact.mjs run-command ${{ matrix.group }})"\n' +
        '          echo "run-command=$command" >> "$GITHUB_OUTPUT"',
    ),
  );
  assert.match(run, /run: \$\{\{ steps.install.outputs.run-command \}\}\n/);
  // Both matrices and the measurement settings come from the artifact script.
  const fromPlan = "${{ fromJSON(needs.native-workloads-plan.outputs.groups) }}";
  assert.ok(build.includes(`group: ${fromPlan}\n`));
  assert.ok(run.includes(`group: ${fromPlan}\n`));
  const measure = "fromJSON(needs.native-workloads-plan.outputs.measure)[matrix.group]";
  assert.ok(run.includes(`timeout-minutes: \${{ ${measure}.timeout }}\n`));
  // Thread stacks are per workload, in the run command, not per job.
  assert.doesNotMatch(run, /RUST_MIN_STACK:/);
  assert.ok(
    workflow.includes('measure="$(node dev/benchmarks/codspeed-artifact.mjs group-measure)"'),
  );
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
      "matrix.group": "stage-plan",
      "env.JAZZ_CODSPEED_SUITE": "merge",
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
  // The nightly suite restores merge outputs but saves under its own key.
  const nightlyKey = render(key, { "env.JAZZ_CODSPEED_SUITE": "nightly" });
  assert.notEqual(nightlyKey, oldKey);
  assert.ok(nightlyKey.startsWith(render(prefix)), "nightly restores merge outputs");
  assert.ok(oldKey.startsWith(render(prefix, { "env.JAZZ_CODSPEED_SUITE": "nightly" })));
  for (const change of [
    { "matrix.group": "live-apps" },
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

test("the nightly suite measures only the nightly extras, never a merge case", async () => {
  assert.equal(currentSuite({}), "merge");
  assert.equal(currentSuite({ JAZZ_CODSPEED_SUITE: "nightly" }), "nightly");
  assert.throws(() => currentSuite({ JAZZ_CODSPEED_SUITE: "weekly" }), /unknown suite/);
  const native = (name, benches) =>
    `--package jazz-example-${name}-benchmark ${benches.map((b) => `--bench ${b}`).join(" ")} --features jazz-benchmark-guard/mimalloc`;
  assert.equal(buildArgs("stage-plan", "nightly").join(" "), native("stage-plan", ["nightly"]));
  assert.equal(buildArgs("band-chat", "nightly").join(" "), native("band-chat", ["nightly"]));
  assert.equal(buildArgs("band-book", "nightly").join(" "), native("band-book", ["nightly"]));
  // Only workloads with extras run at night, in only the groups holding them.
  // The perf timeline admits scheduled main runs, so a nightly run that
  // re-measured a merge case would add a second point at the same commit.
  assert.deepEqual(suiteWorkloads("merge"), workloads);
  assert.deepEqual(suiteWorkloads("nightly"), [
    "stage-plan",
    "band-chat",
    "band-book",
    "groove-ivm",
  ]);
  assert.deepEqual(suiteGroups("merge"), groups);
  assert.deepEqual(suiteGroups("nightly"), [
    "stage-plan",
    "docs-and-access",
    "live-apps",
    "engine",
  ]);
  assert.deepEqual(groupWorkloads("docs-and-access", "nightly"), ["band-book"]);
  assert.deepEqual(groupWorkloads("live-apps", "nightly"), ["band-chat"]);
  for (const workload of workloads.filter((w) => !suiteWorkloads("nightly").includes(w))) {
    assert.throws(() => buildArgs(workload, "nightly"), /no nightly extras/, workload);
  }
  // Example extras are other bench targets than the merge ones.
  for (const workload of ["stage-plan", "band-chat", "band-book"]) {
    const merge = artifactPaths(workload, "merge").binaries;
    const nightly = artifactPaths(workload, "nightly").binaries;
    assert.deepEqual(
      Object.keys(nightly).filter((bench) => bench in merge),
      [],
      `${workload} nightly re-measures a merge target`,
    );
  }
  // Groove's sweep runs the same executables with GROOVE_BENCH_SWEEP=1, which
  // makes them skip the merge cases (see crates/groove/benches).
  assert.deepEqual(buildArgs("groove-ivm", "nightly"), buildArgs("groove-ivm", "merge"));
  assert.equal(
    runCommand("engine", "nightly"),
    "env -u RUST_MIN_STACK GROOVE_BENCH_SWEEP=1 cargo codspeed run -m walltime --package groove --bench pull_vs_snapshot --bench steady_state",
  );
  assert.doesNotMatch(runCommand("engine", "merge"), /GROOVE_BENCH_SWEEP/);
  const groove = await Promise.all(
    ["pull_vs_snapshot", "steady_state"].map((bench) =>
      readFile(path.join(root, `crates/groove/benches/${bench}.rs`), "utf8"),
    ),
  );
  for (const source of groove) {
    // Swept IVM sizes exclude the largest, the size merges measure.
    assert.match(source, /if sweep\(\) \{\n\s+[A-Z_]+\[\.\.[A-Z_]+\.len\(\) - 1\]\.to_vec\(\)/);
  }
  assert.deepEqual(Object.keys(groupMeasureSettings("nightly")), suiteGroups("nightly"));
  for (const group of suiteGroups("nightly")) {
    assert.equal(
      runCommand(group, "nightly").split(" && ").length,
      groupWorkloads(group, "nightly").length,
    );
  }
  // A merge bundle never verifies as a nightly one, or the reverse.
  assert.notDeepEqual(contractFor("groove-ivm", "merge"), contractFor("groove-ivm", "nightly"));
  const previous = process.cwd();
  const dir = await mkdtemp(path.join(os.tmpdir(), "jazz-codspeed-artifact-nightly-"));
  process.chdir(dir);
  try {
    const { binaries } = artifactPaths("stage-plan", "nightly");
    assert.deepEqual(Object.keys(binaries), ["nightly"]);
    for (const binary of Object.values(binaries)) {
      await mkdir(path.dirname(binary), { recursive: true });
      await writeFile(binary, binary);
    }
    await writeFile("cli", "CLI fixture");
    const manifest = await seal("stage-plan", identity, "cli", "nightly");
    assert.deepEqual(Object.keys(manifest.files).sort(), ["cargo-codspeed", "nightly"]);
    assert.deepEqual(await verify("stage-plan", identity, "nightly"), manifest);
    await assert.rejects(verify("stage-plan", identity, "merge"));
  } finally {
    process.chdir(previous);
    await rm(dir, { recursive: true, force: true });
  }
  // The CLI follows the environment the workflow sets.
  const cli = (...args) =>
    execFileSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), ...args], {
      encoding: "utf8",
      env: { ...process.env, JAZZ_CODSPEED_SUITE: "nightly" },
    }).trim();
  assert.equal(cli("suite"), "nightly");
  assert.equal(cli("build-args", "band-book"), buildArgs("band-book", "nightly").join(" "));
  assert.equal(cli("run-command", "engine"), runCommand("engine", "nightly"));
  assert.deepEqual(JSON.parse(cli("group-measure")), groupMeasureSettings("nightly"));
  assert.deepEqual(JSON.parse(cli("groups")), suiteGroups("nightly"));
  assert.deepEqual(JSON.parse(cli("matrix")), suiteWorkloads("nightly"));
  assert.equal(cli("group-workloads", "docs-and-access"), "band-book");
});

test("the workflow runs the nightly suite on its schedule and on request", async () => {
  const workflow = await readFile(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
  assert.match(workflow, /\n  schedule:\n    - cron: "[^"]+"\n/);
  assert.match(workflow, /options: \[merge, nightly\]/);
  assert.ok(
    workflow.includes(
      "JAZZ_CODSPEED_SUITE: ${{ (github.event_name == 'schedule' || inputs.suite == 'nightly') && 'nightly' || 'merge' }}",
    ),
  );
});
