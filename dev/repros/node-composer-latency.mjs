// Run after `pnpm --filter jazz-napi build` from this checkout.
// Usage: node dev/repros/node-composer-latency.mjs [receipt-name]
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const cwd = resolve(root, "packages/jazz-tools");
const name = process.argv[2] ?? "receipt";
if (!/^[a-zA-Z0-9_-]+$/.test(name)) throw new Error("receipt name must be a simple filename");
const output = resolve(cwd, "target/composer-repro");
mkdirSync(output, { recursive: true });
const bundle = resolve(output, "harness.mjs");
execFileSync(
  "pnpm",
  [
    "exec",
    "esbuild",
    "../../dev/repros/node-composer-latency.ts",
    "--bundle",
    "--platform=node",
    "--format=esm",
    "--packages=external",
    `--outfile=${bundle}`,
  ],
  { cwd, stdio: "inherit" },
);
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const require = createRequire(resolve(root, "crates/jazz-napi/native-binding.cjs"));
const { nativeBinding, expectedNativeArtifactFingerprint } = require("./native-binding.cjs");
const binaryPath = Object.keys(require.cache).find(
  (path) => path.startsWith(resolve(root, "crates/jazz-napi")) && path.endsWith(".node"),
);
if (!binaryPath) throw new Error("loaded native binary path is missing");
const provenance = {
  binaryPath,
  binaryHash: hash(readFileSync(binaryPath)),
  head: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  diffHash: hash(execFileSync("git", ["diff", "HEAD", "--binary"], { cwd: root })),
  harnessHash: hash(readFileSync(bundle)),
  fingerprint: nativeBinding.nativeArtifactFingerprint(),
  expectedNativeArtifactFingerprint,
  pointer: readFileSync(resolve(root, "crates/jazz-napi/native-binding.pointer.cjs"), "utf8"),
};
const result = spawnSync(process.execPath, [bundle], {
  cwd,
  encoding: "utf8",
  timeout: 30000,
  maxBuffer: 16 * 1024 * 1024,
});
if (result.status !== 0)
  throw new Error(`harness failed (${result.status}): ${result.error ?? result.stderr}`);
const receipt = { provenance, ...JSON.parse(result.stdout) };
const path = resolve(output, `${name}.json`);
writeFileSync(path, JSON.stringify(receipt));
const summary = {};
for (const [phase, start, end] of [
  ["startup", receipt.phases.startup, receipt.phases.initialCallbacks],
  ["idle", receipt.phases.initialCallbacks, receipt.phases.settled],
  ["writes", receipt.phases.settled, receipt.phases.finished],
]) {
  const ticks = receipt.ticks.filter((tick) => tick.start >= start && tick.start < end);
  const beats = [start, ...receipt.beats.filter((beat) => beat >= start && beat < end), end];
  const reasons = {};
  for (const wake of receipt.wakes.filter((wake) => wake.time >= start && wake.time < end))
    reasons[wake.urgency] = (reasons[wake.urgency] ?? 0) + 1;
  summary[phase] = {
    durationMs: end - start,
    ticks: ticks.length,
    nativeMs: ticks.reduce((sum, tick) => sum + tick.duration, 0),
    maxHeartbeatGapMs: Math.max(...beats.slice(1).map((beat, i) => beat - beats[i])),
    reasons,
  };
}
console.log(
  JSON.stringify(
    {
      path,
      node: receipt.node,
      summary,
      synchronousEchoes: receipt.writes.filter((write) => write.synchronous).length,
    },
    null,
    2,
  ),
);
