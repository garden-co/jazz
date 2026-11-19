// The TypeScript suites run as shards of dev/gates/run-ts-tests.sh, one
// test-ts matrix job each. Splitting them must not drop a suite: these
// contracts prove the shards' lanes together run exactly what the unsharded
// `all` runs, and that every shard keeps the producer/consumer boundary.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";
import {
  TYPESCRIPT_SHARDS,
  assertArtifactBoundary,
  ciPartitionGroups,
  ciPartitionJobs,
  ciPartitions,
  planFor,
  runPlan,
} from "../local-ci-equivalent.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const runner = path.join(root, "dev/gates/run-ts-tests.sh");
const runnerSource = fs.readFileSync(runner, "utf8");
const jazzTools = JSON.parse(
  fs.readFileSync(path.join(root, "packages/jazz-tools/package.json"), "utf8"),
).scripts;
const workflow = parse(fs.readFileSync(path.join(root, ".github/workflows/ci-suite.yml"), "utf8"));

function lanes(shard) {
  const result = spawnSync("bash", [runner, "--describe-shard", shard], {
    cwd: root,
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  const [node, browser, ...rest] = result.stdout.split("\n");
  assert.deepEqual(rest, [""], `${shard} must describe exactly two lanes`);
  return [node, browser];
}

const runnerShards = /^ts_shards="([^"]+)"$/m.exec(runnerSource)[1].split(" ");
const shardsOf = () => runnerShards.filter((shard) => shard !== "all");
const typescriptPartitionNames = Object.keys(ciPartitions).filter((name) =>
  name.startsWith("typescript-"),
);

// `a || status=1; b || status=1; exit ${status:-0}` → [a, b]
function chain(script) {
  const match = /^(.*); exit \$\{status:-0\}$/.exec(script);
  assert.ok(match, `not a status chain: ${script}`);
  return match[1].split("; ").map((part) => {
    assert.match(part, / \|\| status=1$/);
    return part.replace(/ \|\| status=1$/, "");
  });
}

// Every command a lane runs, split on `&&`; the empty lane ":" runs nothing.
const laneCommands = (lane) => (lane === ":" ? [] : lane.split(" && "));

test("every shard is a CI partition, and every TypeScript partition is a test-ts matrix job", () => {
  assert.deepEqual(
    typescriptPartitionNames.map((name) => name.slice("typescript-".length)).sort(),
    shardsOf().sort(),
  );
  assert.deepEqual([...TYPESCRIPT_SHARDS, "examples"].sort(), shardsOf().sort());
  for (const name of typescriptPartitionNames) assert.equal(ciPartitionJobs[name], "test-ts");
  assert.deepEqual(workflow.jobs["test-ts"].strategy.matrix.partition, typescriptPartitionNames);
  assert.equal(workflow.jobs["test-ts"].strategy["fail-fast"], false);
  assert.deepEqual(ciPartitionGroups.typescript, typescriptPartitionNames);
  assert.deepEqual(
    planFor({ partition: "typescript" }),
    typescriptPartitionNames.flatMap((name) => ciPartitions[name]),
  );
});

test("each TypeScript partition admits its own sealed artifacts before its suites", async () => {
  for (const name of typescriptPartitionNames) {
    const plan = planFor({ partition: name });
    assert.doesNotThrow(() => assertArtifactBoundary(plan), name);
    const shard = name.slice("typescript-".length);
    const consumers = plan.filter(({ label }) => label === "TypeScript consumers");
    assert.deepEqual(consumers, [
      {
        label: "TypeScript consumers",
        executable: "pnpm",
        args: ["test:typescript-consumers", "--shard", shard],
        env: { JAZZ_REQUIRE_CI_TEST_COMMANDS: "1" },
      },
    ]);
    assert.match(plan[0].label, /^native correctness-artifact producer/, name);
    const seen = [];
    await assert.rejects(
      runPlan(plan, async (item) => {
        seen.push(item.label);
        if (item === plan[0]) throw new Error("planted producer failure");
      }),
      /planted producer failure/,
    );
    assert.deepEqual(seen, [plan[0].label], name);
  }
});

test("the shards' lanes together run exactly the unsharded suites", () => {
  const [allNode, allBrowser] = lanes("all");
  const commands = shardsOf().flatMap((shard) => lanes(shard).flatMap(laneCommands));
  const take = (predicate, label) => {
    const matches = commands.filter(predicate);
    assert.equal(matches.length, 1, `${label} must run in exactly one shard`);
    commands.splice(commands.indexOf(matches[0]), 1);
    return matches[0];
  };

  // Node: every workspace package's `test` except jazz-tools, through the same
  // Turbo selection, plus jazz-tools' `test`, which is its two halves.
  take((command) => command === `${allNode} --filter=!jazz-tools`, "the other packages' tests");
  assert.deepEqual(chain(jazzTools.test), ["pnpm test:without-node", "pnpm test:node"]);
  take(
    (command) => command === "pnpm --filter jazz-tools run test:without-node",
    "jazz-tools test:without-node",
  );
  const shardsOfScript = (script) => {
    const prefix = `pnpm --filter jazz-tools run ${script} --shard=`;
    const found = commands.filter((command) => command.startsWith(prefix));
    for (const command of found) commands.splice(commands.indexOf(command), 1);
    const parts = found.map((command) => command.slice(prefix.length)).sort();
    assert.ok(parts.length > 0, `${script} runs in no shard`);
    assert.deepEqual(
      parts,
      parts.map((_, index) => `${index + 1}/${parts.length}`),
      `${script} must run every one of its vitest shards once`,
    );
  };
  shardsOfScript("test:node");

  // Browser: jazz-tools' `test:browser` is its Vitest run (sharded) and two
  // receipts; the other packages keep their `test:browser` in one lane.
  assert.equal(
    jazzTools["test:browser"],
    `${jazzTools["test:browser:vitest"]} && pnpm test:browser:storage-quota && pnpm test:browser:transport`,
  );
  const browserPrefix = "pnpm --parallel --filter jazz-tools ";
  assert.ok(allBrowser.startsWith(browserPrefix) && allBrowser.endsWith(" test:browser"));
  take(
    (command) => command === `pnpm --parallel ${allBrowser.slice(browserPrefix.length)}`,
    "the other packages' browser suites",
  );
  shardsOfScript("test:browser:vitest");
  take(
    (command) => command === "pnpm --filter jazz-tools run test:browser:storage-quota",
    "jazz-tools storage-quota receipt",
  );
  take(
    (command) => command === "pnpm --filter jazz-tools run test:browser:transport",
    "jazz-tools transport receipt",
  );

  assert.deepEqual(commands, [], "a shard runs a command `all` does not");
});

test("the runner rejects an unknown shard and malformed arguments", () => {
  for (const args of [
    ["--shard", "made-up"],
    ["--describe-shard", "made-up"],
    ["--shard"],
    ["x"],
  ]) {
    const result = spawnSync("bash", [runner, ...args], { cwd: root, encoding: "utf8" });
    assert.equal(result.status, 2, args.join(" "));
  }
});

test("tools-other starts the producer and an -O1 Durable Object host together", async () => {
  const [first] = planFor({ partition: "typescript-tools-other" });
  assert.deepEqual(first, {
    label: "native correctness-artifact producer and Durable Object host",
    executable: "node",
    args: ["dev/gates/ensure-artifacts-and-durable-object-host.mjs"],
  });
  const step = await import("../ensure-artifacts-and-durable-object-host.mjs");
  assert.equal(step.DURABLE_OBJECT_MERGE_GROUP_WASM_OPT_LEVEL, "O1");
  const calls = [];
  await assert.rejects(
    step.ensureArtifactsAndDurableObjectHost(async (label, args, env) => {
      calls.push({ label, args, env });
      return label === "durable-object-host" ? "planted host failure" : undefined;
    }),
    /planted host failure/,
  );
  assert.deepEqual(
    calls.map(({ label, args }) => [label, args]),
    [
      ["producer", ["dev/gates/ensure-correctness-artifacts.mjs"]],
      [
        "durable-object-host",
        ["packages/jazz-tools/scripts/ensure-durable-object-host.mjs", "--wasm-opt-level", "O1"],
      ],
    ],
  );
  assert.equal(
    calls[1].env.CARGO_TARGET_DIR,
    path.join(root, step.DURABLE_OBJECT_TARGET_DIR),
    "the host builds in its own target directory, so the two do not share Cargo's lock",
  );
  // The default (what ships, and what local `pnpm test` builds) stays -Oz.
  const build = fs.readFileSync(
    path.join(root, "crates/jazz-wasm/durable-object/build.mjs"),
    "utf8",
  );
  assert.match(build, /const wasmOptLevel = levelFlag === -1 \? "Oz" :/);
});

test("the -Oz host that ships runs the full Durable Object suite before every release and nightly", () => {
  assert.deepEqual(lanes("durable-objects"), [
    "pnpm --filter jazz-tools run test:durable-objects",
    ":",
  ]);
  assert.ok(chain(jazzTools["test:without-node"]).includes("pnpm test:durable-objects"));
  const suite = [
    "node dev/gates/ensure-correctness-artifacts.mjs",
    "node dev/gates/run-ts-consumers.mjs --shard durable-objects",
  ];
  const runs = (job) => job.steps.filter(({ run }) => run).map(({ run }) => run.trim());

  // Release: in the job that builds the -Oz host, after both packages are
  // uploaded (the producer republishes crates/jazz-wasm/pkg as its fast build).
  const release = parse(
    fs.readFileSync(path.join(root, ".github/workflows/build-jazz-packages.yml"), "utf8"),
  ).jobs["build-wasm"];
  const releaseRuns = runs(release);
  const built = releaseRuns.indexOf("pnpm --filter jazz-wasm build:durable-object");
  assert.notEqual(built, -1);
  assert.deepEqual(releaseRuns.slice(-2), suite);
  const lastUpload = release.steps.findLastIndex(({ uses }) =>
    uses?.startsWith("actions/upload-artifact"),
  );
  const firstSuite = release.steps.findIndex(({ run }) => run?.trim() === suite[0]);
  assert.ok(lastUpload < firstSuite, "the suite runs after the release packages are uploaded");

  // Nightly: the same, on main.
  const nightly = parse(
    fs.readFileSync(path.join(root, ".github/workflows/nightly-tests.yml"), "utf8"),
  );
  const job = nightly.jobs["durable-object-release-host"];
  assert.ok(job, "nightly runs the Durable Object suite on the -Oz host");
  assert.deepEqual(runs(job), [
    "node dev/gates/ensure-correctness-artifacts.mjs",
    "pnpm --filter jazz-wasm build:durable-object",
    "node dev/gates/run-ts-consumers.mjs --shard durable-objects",
  ]);
});
