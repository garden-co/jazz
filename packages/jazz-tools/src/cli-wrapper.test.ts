import { execFileSync, spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const wrapper = join(packageRoot, "bin", "jazz-tools.js");

async function runWrapper(
  args: string[],
  options: { cwd?: string; env?: NodeJS.ProcessEnv } = {},
): Promise<{ status: number | null; stdout: string; stderr: string }> {
  let resolve!: (value: { status: number | null; stdout: string; stderr: string }) => void;
  const promise = new Promise<{ status: number | null; stdout: string; stderr: string }>(
    (resolvePromise) => {
      resolve = resolvePromise;
    },
  );
  const child = spawn(process.execPath, ["--no-warnings", wrapper, ...args], {
    cwd: options.cwd,
    env: options.env ?? process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk: Buffer) => {
    stdout += chunk.toString();
  });
  child.stderr.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  child.on("close", (status) => resolve({ status, stdout, stderr }));
  return promise;
}

describe("jazz-tools wrapper", () => {
  it.each(["--help", "-h"])("shows deploy help before required arguments (%s)", (flag) => {
    const help = execFileSync(process.execPath, [wrapper, "deploy", flag], { encoding: "utf8" });
    expect(help).toContain("deploy <appId>");
    expect(help).toContain("--server-url");
    expect(help).toContain("--admin-secret");
  });

  it("does not advertise the removed documentation MCP server", () => {
    const help = execFileSync(process.execPath, [wrapper, "--help"], { encoding: "utf8" });

    expect(help).toContain("validate");
    expect(help).not.toMatch(/\bmcp\b/i);
  });

  it.each([
    ["--rust-bin followed by --help", ["--rust-bin", "--help"]],
    ["--rust-bin with no following value", ["--rust-bin"]],
    ["--rust-bin with an empty value", ["--rust-bin", ""]],
  ])("rejects %s before routing to native Rust", async (_description, args) => {
    const result = await runWrapper(args);

    expect(result.status).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Missing value for --rust-bin.");
  });
});
