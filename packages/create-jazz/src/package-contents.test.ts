import { spawn } from "node:child_process";
import * as path from "node:path";
import { describe, expect, it } from "vitest";

const packageRoot = path.resolve(import.meta.dirname, "..");
// `npm pack` performs filesystem traversal and can contend with the parallel
// browser suite in the TypeScript CI partition. Keep that one child operation
// bounded without making the whole test runner's timeout permissive.
const NPM_PACK_TIMEOUT_MS = 30_000;
const bundledSkillFiles = [
  "skills/jazz/SKILL.md",
  "skills/jazz/references/application-data.md",
  "skills/jazz/references/authentication.md",
  "skills/jazz/references/schemas-and-permissions.md",
  "skills/jazz/references/testing.md",
];

function runBoundedChild(
  command: string,
  args: string[],
  cwd: string,
  timeoutMs: number,
  description: string,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    const stdout: string[] = [];
    const stderr: string[] = [];
    child.stdout.on("data", (chunk) => stdout.push(String(chunk)));
    child.stderr.on("data", (chunk) => stderr.push(String(chunk)));

    let settled = false;
    const finish = (result: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      result();
    };
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(() => reject(new Error(`${description} timed out after ${timeoutMs}ms`)));
    }, timeoutMs);

    child.on("error", (error) => finish(() => reject(error)));
    child.on("close", (code, signal) => {
      if (code === 0) {
        finish(() => resolve(stdout.join("")));
        return;
      }
      finish(() =>
        reject(
          new Error(
            `${description} exited with code=${code} signal=${signal ?? "none"}\n${stdout.join("")}${stderr.join("")}`,
          ),
        ),
      );
    });
  });
}

describe("create-jazz package contents", () => {
  it("terminates a hung package command with the phase watchdog", { timeout: 1_000 }, async () => {
    await expect(
      runBoundedChild(
        process.execPath,
        ["-e", "setTimeout(() => {}, 10_000)"],
        packageRoot,
        50,
        "package watchdog probe",
      ),
    ).rejects.toThrow("package watchdog probe timed out after 50ms");
  });

  it(
    "ships the bundled agent skills in npm pack output",
    { timeout: NPM_PACK_TIMEOUT_MS + 5_000 },
    async () => {
      const output = await runBoundedChild(
        "npm",
        ["pack", "--json", "--dry-run", "--ignore-scripts"],
        packageRoot,
        NPM_PACK_TIMEOUT_MS,
        "npm pack --dry-run",
      );
      const packed = JSON.parse(output) as Array<{ files?: Array<{ path: string }> }>;
      const paths = new Set(packed[0]?.files?.map((file) => file.path));

      for (const file of bundledSkillFiles) {
        expect(paths).toContain(file);
      }
    },
  );
});
