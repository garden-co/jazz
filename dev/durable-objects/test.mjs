// Maintained DO integration suites. Cloudflare is never part of a local run.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../../", import.meta.url));
const harness = "dev/durable-objects/native-sqlite/harness/";
const args = process.argv.slice(2);
if (args.includes("--help")) {
  console.log(`Usage: node dev/durable-objects/test.mjs [host|storage|all|cloudflare] [--no-build]

host (default)  Provider unit tests; production admission/accounts,
                createJazz, reconnect/restart and POSIX SIGKILL coverage.
storage         SQL scopes/values, catalogue rollback, replica sync and shared core.
all             Both local suites. Never deploys to Cloudflare.
cloudflare      Disposable real deployment: reconnect, upgrade and persistence.
                Requires CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN.

Builds selected WASM hosts (FAST=1 locally) and the storage suite's native client.
The host suite also builds backend JavaScript. Host/cloudflare require built NAPI artifacts.
--no-build uses existing artifacts; use only when they match current source.
Focused test scripts remain runnable directly; see README.md for their contracts.`);
  process.exit(0);
}
const suite = args.find((arg) => !arg.startsWith("--")) ?? "host";
if (
  !["host", "storage", "all", "cloudflare"].includes(suite) ||
  args.filter((arg) => !arg.startsWith("--")).length > 1 ||
  args.some((arg) => arg.startsWith("--") && arg !== "--no-build")
) {
  throw new Error("Unknown suite or option; run with --help");
}
if (
  suite === "cloudflare" &&
  (!process.env.CLOUDFLARE_ACCOUNT_ID || !process.env.CLOUDFLARE_API_TOKEN)
)
  throw new Error("Cloudflare suite requires CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN");
if (suite !== "storage" && suite !== "cloudflare" && process.platform === "win32")
  throw new Error("The host crash suite requires POSIX process groups");
const env = { ...process.env, JAZZ_DO_PACKAGED: "1" };
if (suite !== "cloudflare") env.FAST ??= "1";
async function run(label, command, argv, overrides = {}) {
  console.log(`\n[DO ${suite}] ${label}`);
  await new Promise((resolve, reject) => {
    const child = spawn(command, argv, {
      cwd: root,
      env: { ...env, ...overrides },
      stdio: "inherit",
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${label} failed (${signal ?? code})`));
    });
  });
}
const host = suite === "host" || suite === "all";
const storage = suite === "storage" || suite === "all";
if (!args.includes("--no-build")) {
  if (host)
    await run("build backend JavaScript", "pnpm", ["--dir", "packages/jazz-tools", "exec", "tsc"]);
  if (host || suite === "cloudflare")
    await run("build production host", process.execPath, [
      "packages/jazz-tools/scripts/bundle-durable-object-worker.mjs",
    ]);
  if (storage) {
    await run("build storage probes", "sh", [
      "dev/durable-objects/native-sqlite/build.sh",
      "--features",
      "scope-tests",
    ]);
    await run("build native sync client", "cargo", [
      "build",
      "-p",
      "jazz-example-doc-shelf-benchmark",
      "--bin",
      "doc-shelf-do-ceiling-ws",
    ]);
  }
}
if (host) {
  await run("provider unit tests", "pnpm", [
    "--dir",
    "packages/jazz-tools",
    "exec",
    "vitest",
    "run",
    "src/backend/core-providers/durable-objects.test.ts",
  ]);
  for (const name of ["create-jazz", "worker-backend", "accounts", "lifecycle", "crash"])
    await run(name, process.execPath, [`${harness}test-${name}.mjs`]);
}
if (storage) {
  await run(
    "catalogue validation and rollback",
    process.execPath,
    [`${harness}test-catalogue.mjs`],
    { JAZZ_DO_PACKAGED: "0" },
  );
  await run("SQL scopes and catalogue recovery", process.execPath, [
    `${harness}test-scopes.mjs`,
    "--catalogue",
  ]);
  await run("SQL values and native replica sync", process.execPath, [
    `${harness}test-values.mjs`,
    "--sync",
  ]);
  await run("shared server core", process.execPath, [`${harness}test-server-core.mjs`]);
}
if (suite === "cloudflare")
  await run("deployed lifecycle", process.execPath, [
    `${harness}test-lifecycle.mjs`,
    "--cloudflare",
  ]);
console.log(`\nDO ${suite} suite passed.`);
