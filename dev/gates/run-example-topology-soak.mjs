#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { spawn, spawnSync } from "node:child_process";

const root = realpathSync(resolve(import.meta.dirname, "../.."));
const registryPath = process.env.JAZZ_EXAMPLE_TOPOLOGY_REGISTRY
  ? confinedExistingPath(process.env.JAZZ_EXAMPLE_TOPOLOGY_REGISTRY, "scenario registry")
  : confinedExistingPath("dev/example-topology-scenarios.json", "scenario registry");
const registry = JSON.parse(readFileSync(registryPath, "utf8"));
validateRegistry(registry);
let output = "target/example-topology-soak";
let seedCount = 3;
let watchdogSeconds = 90;
let selected = [];
for (let index = 2; index < process.argv.length; index++) {
  const option = process.argv[index];
  if (option === "--output") output = process.argv[++index];
  else if (option === "--seed-count") seedCount = positiveInteger(process.argv[++index], option);
  else if (option === "--watchdog-seconds")
    watchdogSeconds = positiveInteger(process.argv[++index], option);
  else if (option === "--scenario") selected.push(process.argv[++index]);
  else if (option === "--list") {
    for (const scenario of registry.scenarios) console.log(scenario.id);
    process.exit(0);
  } else usage(`unknown option: ${option}`);
}
if (seedCount > 100 || watchdogSeconds > 900) usage("seed/watchdog cap exceeded");
const scenarios = registry.scenarios.filter(
  ({ id }) => selected.length === 0 || selected.includes(id),
);
if (scenarios.length === 0 || selected.some((id) => !scenarios.some((item) => item.id === id))) {
  usage("unknown or empty scenario selection");
}

const outputDir = confinedCreatablePath(output, "output");
mkdirSync(resolve(outputDir, "logs"), { recursive: true });
const fixedSeeds = [11, 29, 47, 83, 32676, 40595, 2234158, 3715011, 4372288];
const cases = [];
for (const scenario of scenarios) {
  for (let index = 0; index < seedCount; index++) {
    const seed = fixedSeeds[index] ?? 1000 + (index - fixedSeeds.length) * 7919;
    const logName = `${cases.length}-${scenario.id.replaceAll(/[^a-zA-Z0-9.-]/g, "-")}-seed-${seed}.log`;
    const command = [`JAZZ_EXAMPLE_TOPOLOGY_SEED=${seed}`, ...scenario.argv.map(shellQuote)].join(
      " ",
    );
    const replay = scenario.cwd === "." ? command : `cd ${shellQuote(scenario.cwd)} && ${command}`;
    const started = Date.now();
    const result = await runProcess(scenario.argv[0], scenario.argv.slice(1), {
      cwd: confinedExistingPath(scenario.cwd, `scenario ${scenario.id} cwd`),
      env: { ...process.env, JAZZ_EXAMPLE_TOPOLOGY_SEED: String(seed) },
      timeoutMs: watchdogSeconds * 1000,
    });
    const status = result.timedOut ? "timeout" : result.status === 0 ? "passed" : "failed";
    writeFileSync(
      resolve(outputDir, "logs", logName),
      `${result.stdout ?? ""}${result.stderr ?? ""}`,
    );
    cases.push({
      scenario: scenario.id,
      topology: scenario.topology,
      seed,
      status,
      exitCode: result.status,
      elapsedMs: Date.now() - started,
      log: `logs/${logName}`,
      replay,
    });
    console.log(`${scenario.id} seed=${seed} status=${status}; replay: ${replay}`);
  }
}
const summary = {
  schemaVersion: 1,
  sha: spawnSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).stdout.trim(),
  cases,
  failures: cases.filter(({ status }) => status !== "passed"),
};
writeFileSync(resolve(outputDir, "summary.json"), `${JSON.stringify(summary, null, 2)}\n`);
process.exitCode = summary.failures.length === 0 ? 0 : 1;

function positiveInteger(value, option) {
  if (!/^[1-9][0-9]*$/.test(value ?? "")) usage(`${option} requires a positive integer`);
  return Number(value);
}

function shellQuote(value) {
  return /^[a-zA-Z0-9_./:@=-]+$/.test(value) ? value : `'${value.replaceAll("'", `'\\''`)}'`;
}

function lexicalConfinedPath(value, label) {
  const path = resolve(root, value);
  assertConfined(path, label);
  return path;
}

function confinedExistingPath(value, label) {
  const path = realpathSync(lexicalConfinedPath(value, label));
  assertConfined(path, label);
  return path;
}

function confinedCreatablePath(value, label) {
  const path = lexicalConfinedPath(value, label);
  let existing = path;
  while (!existsSync(existing)) existing = dirname(existing);
  assertConfined(realpathSync(existing), label);
  mkdirSync(path, { recursive: true });
  const canonical = realpathSync(path);
  assertConfined(canonical, label);
  return canonical;
}

function assertConfined(path, label) {
  const fromRoot = relative(root, path);
  if (fromRoot === ".." || fromRoot.startsWith(`..${sep}`) || isAbsolute(fromRoot)) {
    usage(`${label} must remain inside the repository`);
  }
}

function runProcess(command, args, { cwd, env, timeoutMs }) {
  return new Promise((resolveResult) => {
    const child = spawn(command, args, {
      cwd,
      env,
      detached: process.platform !== "win32",
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
    child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
    const timer = setTimeout(() => {
      timedOut = true;
      terminateProcessTree(child.pid);
    }, timeoutMs);
    child.on("error", (error) => {
      clearTimeout(timer);
      resolveResult({ status: null, stdout, stderr: `${stderr}${error.message}\n`, timedOut });
    });
    child.on("close", (status) => {
      clearTimeout(timer);
      resolveResult({ status, stdout, stderr, timedOut });
    });
  });
}

function terminateProcessTree(pid) {
  if (!pid) return;
  if (process.platform === "win32") {
    spawnSync("taskkill", ["/pid", String(pid), "/T", "/F"], { windowsHide: true });
    return;
  }
  const descendants = descendantPids(pid);
  for (const descendant of descendants.reverse()) killPid(descendant);
  try {
    process.kill(-pid, "SIGKILL");
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
  killPid(pid);
}

function descendantPids(rootPid) {
  const processList = spawnSync("ps", ["-eo", "pid=,ppid="], { encoding: "utf8" });
  if (processList.status !== 0) return [];
  const children = new Map();
  for (const line of processList.stdout.trim().split("\n")) {
    const [pid, parentPid] = line.trim().split(/\s+/).map(Number);
    if (!children.has(parentPid)) children.set(parentPid, []);
    children.get(parentPid).push(pid);
  }
  const found = [];
  const pending = [...(children.get(rootPid) ?? [])];
  while (pending.length > 0) {
    const pid = pending.pop();
    found.push(pid);
    pending.push(...(children.get(pid) ?? []));
  }
  return found;
}

function killPid(pid) {
  try {
    process.kill(pid, "SIGKILL");
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

function validateRegistry(value) {
  if (value?.schemaVersion !== 1 || !Array.isArray(value.scenarios)) {
    usage("scenario registry must have schemaVersion 1 and a scenarios array");
  }
  const ids = new Set();
  for (const scenario of value.scenarios) {
    if (typeof scenario?.id !== "string" || scenario.id.length === 0) {
      usage("each scenario requires a non-empty id");
    }
    if (ids.has(scenario.id)) usage(`duplicate scenario id: ${scenario.id}`);
    ids.add(scenario.id);
    if (!Array.isArray(scenario.topology) || scenario.topology.length === 0) {
      usage(`scenario ${scenario.id} requires at least one topology`);
    }
    const topologyKinds = new Set(["core", "edge", "browser", "native", "fixture"]);
    if (scenario.topology.some((kind) => !topologyKinds.has(kind))) {
      usage(`scenario ${scenario.id} has an unknown topology`);
    }
    if (typeof scenario.cwd !== "string" || scenario.cwd.length === 0) {
      usage(`scenario ${scenario.id} requires cwd`);
    }
    if (
      !Array.isArray(scenario.argv) ||
      scenario.argv.length === 0 ||
      scenario.argv.some((argument) => typeof argument !== "string")
    ) {
      usage(`scenario ${scenario.id} requires a non-empty string argv`);
    }
  }
}

function usage(error) {
  if (error) console.error(error);
  console.error(
    "Usage: run-example-topology-soak.mjs [--list] [--scenario ID] [--seed-count N] [--watchdog-seconds N] [--output DIR]",
  );
  process.exit(2);
}
