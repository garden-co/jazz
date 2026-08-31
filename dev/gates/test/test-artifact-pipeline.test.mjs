import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import test from "node:test";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  acquireArtifactBuildLock,
  buildTestArtifacts,
  command,
  unlockArtifactBuildLock,
  withArtifactBuildLock,
} from "../build-test-artifacts.mjs";

const pipelineUrl = new URL("../build-test-artifacts.mjs", import.meta.url).href;

function childWithLock(lockPath, body) {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import fs from 'node:fs'; import { buildTestArtifacts, command, withArtifactBuildLock } from ${JSON.stringify(pipelineUrl)}; ${body}`,
    ],
    {
      env: { ...process.env, JAZZ_TEST_ARTIFACT_LOCK_PATH: lockPath },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  let output = "";
  child.stdout.on("data", (chunk) => (output += chunk));
  child.stderr.on("data", (chunk) => (output += chunk));
  return {
    child,
    output: () => output,
    closed: new Promise((resolve) =>
      child.once("close", (code, signal) => resolve({ code, signal })),
    ),
  };
}

async function waitFor(condition, message) {
  for (let attempts = 0; attempts < 100; attempts++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(message);
}

test("artifact lock rejects a concurrent real subprocess with actionable ownership", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-lock-"));
  const lockPath = join(fixture, "lock");
  const holder = childWithLock(
    lockPath,
    "await withArtifactBuildLock(() => new Promise(() => setInterval(() => {}, 1000)));",
  );
  try {
    await waitFor(() => existsSync(lockPath), "holder did not acquire lock");
    const contender = childWithLock(lockPath, "await withArtifactBuildLock(async () => {});");
    const result = await contender.closed;
    assert.equal(result.code, 1);
    assert.match(contender.output(), /active artifact lock \(pid \d+, cwd .+, started .+\)/);
    const message = contender.output().match(/Error: ([^\n]+)/)?.[1] ?? "";
    assert.doesNotMatch(message, /Lock:|\/tmp\/|\/work\//);
  } finally {
    holder.child.kill("SIGTERM");
    await holder.closed;
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("artifact lock refuses stale owners until explicit verified unlock", () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-stale-"));
  const lockPath = join(fixture, "lock");
  writeFileSync(
    lockPath,
    JSON.stringify({
      pid: 999_999_999,
      cwd: "stale-checkout",
      startedAt: "2000-01-01T00:00:00.000Z",
      token: "dead",
    }),
  );
  assert.throws(() => acquireArtifactBuildLock(lockPath), /stale artifact lock.*artifacts:unlock/);
  assert.equal(existsSync(lockPath), true);
  unlockArtifactBuildLock(lockPath);
  const lock = acquireArtifactBuildLock(lockPath);
  lock.release();
  assert.equal(existsSync(lockPath), false);
  rmSync(fixture, { recursive: true, force: true });
});

test("80 simultaneous stale contenders never auto-steal; unlock permits exactly one next owner", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-stale-race-"));
  const lockPath = join(fixture, "lock");
  writeFileSync(
    lockPath,
    JSON.stringify({
      pid: 999_999_999,
      cwd: "stale-checkout",
      startedAt: "2000-01-01T00:00:00.000Z",
      token: "dead",
    }),
  );
  const contenders = Array.from({ length: 80 }, () =>
    childWithLock(
      lockPath,
      "await withArtifactBuildLock(() => new Promise(() => setInterval(() => {}, 1000)));",
    ),
  );
  try {
    for (const contender of contenders) {
      const result = await contender.closed;
      assert.equal(result.code, 1);
      assert.match(contender.output(), /stale artifact lock.*artifacts:unlock/);
    }
    assert.equal(existsSync(lockPath), true);
    unlockArtifactBuildLock(lockPath);
    const winner = childWithLock(
      lockPath,
      "await withArtifactBuildLock(() => new Promise(() => setInterval(() => {}, 1000)));",
    );
    await waitFor(() => existsSync(lockPath), "next contender did not acquire lock");
    assert.equal(winner.child.exitCode, null);
    winner.child.kill("SIGTERM");
    await winner.closed;
  } finally {
    for (const contender of contenders) contender.child.kill("SIGTERM");
    await Promise.all(contenders.map((contender) => contender.closed));
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("artifact lock refuses an unowned directory instead of deleting a possibly live lock", () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-unowned-"));
  const lockPath = join(fixture, "lock");
  writeFileSync(lockPath, "not a receipt");
  assert.throws(() => acquireArtifactBuildLock(lockPath), /read lock receipt failed/);
  assert.equal(existsSync(lockPath), true);
  rmSync(fixture, { recursive: true, force: true });
});

test("lock filesystem failures redact missing absolute paths", () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-path-redaction-"));
  const missingParent = join(fixture, "missing-parent");
  const lockPath = join(missingParent, "lock");
  // This planted raw filesystem call proves the underlying diagnostic would
  // disclose the absolute path if the lock layer ever rethrows it directly.
  assert.throws(() => writeFileSync(lockPath, "raw"), new RegExp(missingParent));
  assert.throws(
    () => acquireArtifactBuildLock(lockPath),
    (error) =>
      /create lock receipt failed \(ENOENT\)/.test(error.message) &&
      !error.message.includes(fixture) &&
      !error.message.includes(lockPath),
  );
  rmSync(fixture, { recursive: true, force: true });
});

test("artifact lock cleans up after a real failure and termination signal", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-cleanup-"));
  const failingLock = join(fixture, "failure-lock");
  const failure = childWithLock(
    failingLock,
    "await withArtifactBuildLock(async () => { throw new Error('deliberate failure'); });",
  );
  assert.equal((await failure.closed).code, 1);
  assert.equal(existsSync(failingLock), false);

  for (const signal of ["SIGINT", "SIGTERM"]) {
    const signalLock = join(fixture, `signal-lock-${signal}`);
    const running = childWithLock(
      signalLock,
      "await withArtifactBuildLock(() => new Promise(() => setInterval(() => {}, 1000)));",
    );
    await waitFor(() => existsSync(signalLock), "signal child did not acquire lock");
    // The receipt is written just before the signal listeners; give the
    // subprocess one event-loop turn to install them before injecting a signal.
    await new Promise((resolve) => setTimeout(resolve, 20));
    running.child.kill(signal);
    const terminated = await running.closed;
    assert.equal(terminated.signal, signal);
    assert.equal(existsSync(signalLock), false);
  }
  rmSync(fixture, { recursive: true, force: true });
});

test("signal keeps the lock until its delayed child has exited", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-signal-drain-"));
  const lockPath = join(fixture, "lock");
  const received = join(fixture, "received");
  const ready = join(fixture, "ready");
  const delayedChild = `
    const fs = require('node:fs');
    fs.writeFileSync(${JSON.stringify(ready)}, 'ready');
    process.on('SIGTERM', () => {
      fs.writeFileSync(${JSON.stringify(received)}, 'received');
      setTimeout(() => process.exit(0), 180);
    });
    setInterval(() => {}, 1000);
  `;
  const holder = childWithLock(
    lockPath,
    `await withArtifactBuildLock(async (scope) => scope.track(command(process.execPath, ['-e', ${JSON.stringify(delayedChild)}], 'delayed child', { signal: scope.signal })));`,
  );
  try {
    await waitFor(() => existsSync(lockPath), "holder did not acquire lock");
    await waitFor(() => existsSync(ready), "delayed child did not start");
    holder.child.kill("SIGTERM");
    await waitFor(() => existsSync(received), "child did not receive SIGTERM");
    assert.equal(existsSync(lockPath), true, "lock released before child exit");
    const contender = childWithLock(lockPath, "await withArtifactBuildLock(async () => {});");
    assert.equal((await contender.closed).code, 1);
    assert.match(contender.output(), /active artifact lock/);
    assert.equal((await holder.closed).signal, "SIGTERM");
    assert.equal(existsSync(lockPath), false);
  } finally {
    holder.child.kill("SIGKILL");
    await holder.closed;
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("signal during NAPI repair drains the parent scope before unlock", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-repair-signal-"));
  const lockPath = join(fixture, "lock");
  const repairing = join(fixture, "repairing");
  const received = join(fixture, "received");
  const stopped = join(fixture, "stopped");
  const holder = childWithLock(
    lockPath,
    `await withArtifactBuildLock((scope) => buildTestArtifacts((command, args, label, { signal }) => {
      if (label === 'preflight release NAPI') return Promise.reject(new Error('load failed'));
      if (label === 'repair release NAPI') return new Promise((resolve) => {
        fs.writeFileSync(${JSON.stringify(repairing)}, 'repairing');
        const timer = setInterval(() => {}, 1000);
        signal.addEventListener('abort', () => { fs.writeFileSync(${JSON.stringify(received)}, 'received'); setTimeout(() => { clearInterval(timer); fs.writeFileSync(${JSON.stringify(stopped)}, 'stopped'); resolve(); }, 150); }, { once: true });
      });
      return Promise.resolve();
    }, scope));`,
  );
  try {
    await waitFor(() => existsSync(repairing), "repair did not start");
    await new Promise((resolve) => setTimeout(resolve, 20));
    holder.child.kill("SIGTERM");
    await waitFor(() => existsSync(received), "repair did not receive shutdown");
    assert.equal(existsSync(lockPath), true, "lock released during repair shutdown");
    assert.equal((await holder.closed).signal, "SIGTERM");
    assert.equal(existsSync(stopped), true);
    assert.equal(existsSync(lockPath), false);
  } finally {
    holder.child.kill("SIGKILL");
    await holder.closed;
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("test artifact pipeline builds only runtime consumers and repairs NAPI after a failed load", async () => {
  const calls = [];
  const snapshots = [];
  let releaseLoadAttempts = 0;
  await buildTestArtifacts(
    async (command, args, label, options) => {
      calls.push({ command, args, label, options });
      if (label === "preflight release NAPI" && ++releaseLoadAttempts === 1)
        throw new Error("simulated corrupt binding");
    },
    undefined,
    undefined,
    () => {
      snapshots.push(calls.at(-1)?.label);
    },
  );

  const labels = calls.map((call) => call.label);
  assert.deepEqual(labels.slice(0, 4), [
    "release NAPI",
    "fast WASM",
    "derive local artifact expectations",
    "preflight release NAPI",
  ]);
  assert.equal(labels.filter((label) => label === "release NAPI").length, 1);
  assert.equal(labels.filter((label) => label === "repair release NAPI").length, 1);
  assert.ok(labels.indexOf("verify fast WASM provenance") < labels.indexOf("load release NAPI"));
  assert.ok(
    labels.indexOf("refresh repaired artifact expectations") >
      labels.indexOf("repair release NAPI"),
  );
  assert.ok(
    labels.lastIndexOf("preflight release NAPI") >
      labels.indexOf("refresh repaired artifact expectations"),
  );
  assert.deepEqual(snapshots, ["preflight release NAPI"]);
  assert.ok(labels.indexOf("verify release NAPI provenance") > labels.indexOf("load release NAPI"));
  for (const call of calls.filter(({ label }) =>
    ["fast WASM", "release NAPI", "repair release NAPI"].includes(label),
  ))
    assert.equal(call.options.env?.CARGO_TARGET_DIR, undefined, call.label);
});

test("aggregate CI lock selection reaches every Turbo artifact producer", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-ci-artifact-lock-"));
  const lockPath = join(fixture, "runner-temp-selected.lock");
  const calls = [];
  try {
    await withArtifactBuildLock(
      (scope, lease) =>
        buildTestArtifacts(
          async (command, args, label, options) => {
            calls.push({ command, args, label, options });
          },
          scope,
          lease,
        ),
      lockPath,
    );
    for (const call of calls.filter(({ label }) =>
      ["release NAPI", "fast WASM", "repair release NAPI"].includes(label),
    )) {
      assert.equal(
        call.options.env.JAZZ_TEST_ARTIFACT_LOCK_PATH,
        lockPath,
        `${call.label} lost the parent-selected CI lock path`,
      );
      assert.equal(
        call.options.env.JAZZ_ARTIFACT_BUILD_LOCK_PATH,
        lockPath,
        `${call.label} received a child-selected lease path`,
      );
      assert.ok(call.options.env.JAZZ_ARTIFACT_BUILD_LEASE, `${call.label} lost the lease token`);
    }
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("a strict Turbo-like child verifies the CI parent's runner-temp lease", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-ci-strict-artifact-lock-"));
  const lockPath = join(fixture, "runner-temp-selected.lock");
  try {
    await withArtifactBuildLock(async (unusedScope, lease) => {
      const child = spawnSync(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `import { verifyArtifactBuildLease } from ${JSON.stringify(new URL("../build-test-artifacts.mjs", import.meta.url).href)}; verifyArtifactBuildLease({ token: process.env.JAZZ_ARTIFACT_BUILD_LEASE, lockPath: process.env.JAZZ_ARTIFACT_BUILD_LOCK_PATH });`,
        ],
        {
          // Turbo's strict environment is intentionally modelled as exactly
          // the declared inputs, not the ambient parent process.
          env: { PATH: process.env.PATH, ...lease },
        },
      );
      assert.equal(child.status, 0, child.stderr.toString());
    }, lockPath);
  } finally {
    rmSync(fixture, { recursive: true, force: true });
  }
});

test("a failed runtime artifact build releases its scope", async () => {
  const aborted = [];
  await assert.rejects(
    buildTestArtifacts((unusedCommand, unusedArgs, label, { signal } = {}) => {
      if (label === "release NAPI") return Promise.reject(new Error("simulated NAPI failure"));
      return new Promise((resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            aborted.push(label);
            reject(new Error(`${label} aborted`));
          },
          { once: true },
        );
      });
    }),
    /simulated NAPI failure/,
  );
  assert.deepEqual(aborted, []);
});

test("real subprocess inherits the caller's cache-compatible Cargo target", async () => {
  await command(
    process.execPath,
    [
      "-e",
      `if(process.env.CARGO_TARGET_DIR!==${JSON.stringify(process.env.CARGO_TARGET_DIR)}) process.exit(42)`,
    ],
    "default target smoke",
  );
});

test("aborting a real subprocess terminates its spawned child", async () => {
  const fixture = mkdtempSync(join(tmpdir(), "jazz-test-artifact-cancel-"));
  const marker = join(fixture, "orphaned-child");
  const controller = new AbortController();
  const parentScript = [
    "const {spawn}=require('node:child_process')",
    `spawn(process.execPath,['-e',${JSON.stringify(`setTimeout(()=>require('node:fs').writeFileSync(${JSON.stringify(marker)},'orphan'),400)`)}],{stdio:'ignore'})`,
    "setInterval(()=>{},1000)",
  ].join(";");
  const running = command(process.execPath, ["-e", parentScript], "cancellation smoke", {
    signal: controller.signal,
  });
  setTimeout(() => controller.abort(), 100);
  await assert.rejects(running, /cancellation smoke failed/);
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert.equal(existsSync(marker), false);
  rmSync(fixture, { recursive: true, force: true });
});

test("CI uses the correctness artifact path while package builds keep release WASM", () => {
  const workflow = readFileSync(
    new URL("../../../.github/workflows/ci-suite.yml", import.meta.url),
    "utf8",
  );
  const packageJson = readFileSync(new URL("../../../package.json", import.meta.url), "utf8");
  const pipeline = readFileSync(new URL("../build-test-artifacts.mjs", import.meta.url), "utf8");
  const localCi = readFileSync(new URL("../local-ci-equivalent.mjs", import.meta.url), "utf8");
  const consumers = readFileSync(new URL("../run-ts-consumers.mjs", import.meta.url), "utf8");
  assert.match(workflow, /local-ci-equivalent\.mjs --ci-partition typescript/);
  assert.match(
    localCi,
    /native correctness-artifact producer[\s\S]*pnpm[\s\S]*build:correctness-artifacts/,
  );
  assert.match(
    packageJson,
    /"build:correctness-artifacts": "node dev\/gates\/build-test-artifacts\.mjs"/,
  );
  assert.match(
    packageJson,
    /"test:typescript-consumers": "node dev\/gates\/run-ts-consumers\.mjs"/,
  );
  assert.match(
    packageJson,
    /"artifacts:unlock": "node dev\/gates\/build-test-artifacts\.mjs unlock"/,
  );
  assert.match(
    packageJson,
    /"build:ci": "turbo run build:crates.*jazz-wasm.*jazz-napi.*jazz-tools/,
  );
  for (const script of ["build", "build:core", "build:ci"])
    assert.match(
      packageJson,
      new RegExp(
        `"${script.replace(":", "\\:")}": "turbo run build:crates.*jazz-wasm.*turbo run build`,
      ),
      `${script} must leave atomic WASM publication as the sole provenance writer`,
    );
  assert.doesNotMatch(workflow, /CARGO_TARGET_DIR/);
  assert.doesNotMatch(pipeline, /target\/test-artifacts-(?:wasm|napi)/);
  for (const task of ["build", "build:fast"])
    assert.match(
      pipeline,
      new RegExp(`"exec", "turbo", "run", "${task.replace(":", "\\:")}"`),
      `${task} correctness artifact must run through Turbo`,
    );
  assert.doesNotMatch(pipeline, /--filter=jazz-tools/);
  assert.ok(
    consumers.indexOf("runCorrectnessConsumer(") < consumers.indexOf('"--filter=jazz-tools"'),
    "TS consumers must reject a stale producer receipt before Jazz Tools can build",
  );

  const turbo = JSON.parse(readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8"));
  const expectedLease = [
    "JAZZ_TEST_ARTIFACT_LOCK_PATH",
    "JAZZ_ARTIFACT_BUILD_LEASE",
    "JAZZ_ARTIFACT_BUILD_LOCK_PATH",
    "JAZZ_TEST_SEALED_TOOLS_DIST",
  ];
  for (const task of ["jazz-napi#build", "jazz-wasm#build", "jazz-wasm#build:fast"])
    assert.deepEqual(
      turbo.tasks[task].passThroughEnv,
      expectedLease,
      `${task} must preserve the aggregate parent's selected artifact lock`,
    );
  for (const task of ["build"])
    assert.deepEqual(
      turbo.tasks[task].passThroughEnv,
      ["JAZZ_TEST_SEALED_TOOLS_DIST"],
      `${task} must preserve the sealed shared test surface for child package scripts`,
    );
  assert.deepEqual(turbo.tasks["jazz-tools#build"].passThroughEnv, [
    "JAZZ_TEST_SEALED_TOOLS_DIST",
    "JAZZ_CORRECTNESS_ARTIFACT_RUN",
    "JAZZ_CORRECTNESS_WASM_PACKAGE",
    "JAZZ_CORRECTNESS_NAPI_BINDING",
    "JAZZ_CORRECTNESS_NAPI_FINGERPRINT",
  ]);
  // Correctness consumers inject a content-addressed WASM package through a
  // pass-through environment variable. Turbo does not include pass-through
  // values in its task hash, so restoring Jazz Tools' bundled output could
  // otherwise pair a verified current snapshot with stale embedded WASM.
  // The native producer's `.native-artifacts/**` output includes Cargo
  // generations measured in tens of GiB. These four tasks must use the
  // explicit producer/consumer hand-off, never Turbo archives.
  for (const task of [
    "jazz-napi#build",
    "jazz-wasm#build",
    "jazz-wasm#build:fast",
    "jazz-tools#build",
  ])
    assert.equal(
      turbo.tasks[task].cache,
      false,
      `${task} must remain uncached: correctness artifacts are not Turbo outputs`,
    );
  assert.deepEqual(turbo.tasks.test.passThroughEnv, [
    ...turbo.tasks["jazz-tools#build"].passThroughEnv,
    "JAZZ_CORRECTNESS_CONSUMER_CAPABILITY",
    "JAZZ_CORRECTNESS_CONSUMER_TOKEN",
  ]);
});

test("Turbo invalidates each native artifact only for its Cargo closure", () => {
  const turbo = JSON.parse(readFileSync(new URL("../../../turbo.json", import.meta.url), "utf8"));
  const napi = turbo.tasks["jazz-napi#build"];
  const wasm = turbo.tasks["jazz-wasm#build"];
  const fastWasm = turbo.tasks["jazz-wasm#build:fast"];
  const jazzTools = turbo.tasks["jazz-tools#build"];
  const cli = turbo.tasks["build:crates"];
  assert.equal(napi.dependsOn, undefined);
  assert.equal(wasm.dependsOn, undefined);
  for (const [task, inputs] of [
    [
      napi,
      [
        "$TURBO_ROOT$/crates/jazz-napi/package.json",
        "$TURBO_ROOT$/crates/jazz-napi/Cargo.toml",
        "$TURBO_ROOT$/crates/jazz-napi/build.rs",
        "$TURBO_ROOT$/crates/jazz-napi/scripts/**",
        "$TURBO_ROOT$/crates/jazz-napi/src/**/*.rs",
      ].concat(["jazz-otel", "jazz", "groove"].map((crate) => `$TURBO_ROOT$/crates/${crate}/**`)),
    ],
    [
      wasm,
      ["package.json", "Cargo.toml", "src/**/*.rs"].concat(
        ["jazz", "groove"].map((crate) => `$TURBO_ROOT$/crates/${crate}/**`),
      ),
    ],
    [
      fastWasm,
      ["package.json", "Cargo.toml", "src/**/*.rs"].concat(
        ["jazz", "groove"].map((crate) => `$TURBO_ROOT$/crates/${crate}/**`),
      ),
    ],
    [
      cli,
      ["jazz-cli", "jazz", "groove"].flatMap((crate) => [
        `$TURBO_ROOT$/crates/${crate}/Cargo.toml`,
        `$TURBO_ROOT$/crates/${crate}/src/**/*.rs`,
      ]),
    ],
  ]) {
    for (const input of inputs) assert.ok(task.inputs.includes(input), input);
    assert.equal(task.inputs.includes("$TURBO_ROOT$/crates/**/*.rs"), false);
  }
  for (const generatedInput of [
    "$TURBO_ROOT$/crates/jazz-napi/*.node",
    "$TURBO_ROOT$/crates/jazz-napi/.jazz-artifact-manifest.json",
    "$TURBO_ROOT$/crates/jazz-wasm/pkg/**",
  ])
    assert.ok(
      jazzTools.inputs.includes(generatedInput),
      `jazz-tools build hash omits ${generatedInput}`,
    );
});
