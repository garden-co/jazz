// Build local workerd test inputs. No Cloudflare API calls or deployments.
import { spawn } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../../../", import.meta.url));
const env = { ...process.env, FAST: process.env.FAST ?? "1" };
async function run(command, args, cwd = root) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) =>
      code === 0 ? resolve() : reject(new Error(`${command} failed (${signal ?? code})`)),
    );
  });
}
await run(process.execPath, ["packages/jazz-tools/scripts/bundle-durable-object-worker.mjs"]);
const args = [
  "build",
  "jazz-do",
  "--target",
  "web",
  "--release",
  "--out-dir",
  join(root, "packages/jazz-tools/tests/durable-objects/pkg"),
];
if (env.FAST === "1") args.push("--no-opt");
args.push("--", "--locked", "--features", "scope-tests");
if (env.FAST === "1")
  args.push(
    "--config",
    'profile.release.lto="off"',
    "--config",
    "profile.release.codegen-units=16",
  );
await run("wasm-pack", args, join(root, "crates/jazz-wasm/durable-object-tests"));
await run("cargo", [
  "build",
  "--locked",
  "--target-dir",
  join(root, "target"),
  "-p",
  "jazz-example-doc-shelf-benchmark",
  "--bin",
  "doc-shelf-do-ceiling-ws",
]);
await run("pnpm", ["exec", "tsc"], join(root, "packages/jazz-tools"));
