// Builds a Rust example's binary with cargo, for a test to run.
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const examples = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

/** Builds `bin` of cargo package `pkg` and returns the executable's path. */
export function cargoBin(pkg: string, bin: string): string {
  const build = spawnSync(
    "cargo",
    ["build", "-p", pkg, "--bin", bin, "--message-format=json-render-diagnostics"],
    { cwd: examples, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 256 << 20 },
  );
  if (build.status !== 0) throw new Error(`cargo build -p ${pkg} failed (exit ${build.status})`);
  for (const line of build.stdout.split("\n")) {
    if (!line.startsWith("{")) continue;
    const message = JSON.parse(line);
    if (
      message.reason === "compiler-artifact" &&
      message.target?.name === bin &&
      message.executable
    ) {
      return message.executable as string;
    }
  }
  throw new Error(`cargo build -p ${pkg} produced no ${bin} executable`);
}
