import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, expect, it } from "vitest";

const packageRoot = path.resolve(import.meta.dirname, "..");
// `npm pack` performs filesystem traversal and can contend with the parallel
// browser suite in the TypeScript CI partition. A cold complete partition took
// 11.7s and concurrent load exceeded the former 15s whole-test budget, so 30s
// gives that phase bounded contention headroom without making Vitest global.
const NPM_PACK_TIMEOUT_MS = 30_000;
const TEST_CLEANUP_MARGIN_MS = 5_000;
const TERMINATION_GRACE_MS = 1_000;
const npmPackArgs = ["pack", "--json", "--dry-run", "--ignore-scripts"];
const windowsNpmPackCommand = "npm.cmd pack --json --dry-run --ignore-scripts";
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

    let spawnError: Error | undefined;
    let timedOut = false;
    let forceKillTimer: NodeJS.Timeout | undefined;
    const timer = setTimeout(() => {
      timedOut = true;
      // Request graceful termination first so package-manager cleanup can run.
      // The close handler below is the only settlement point: it guarantees
      // both output streams have drained and the child has been reaped.
      child.kill("SIGTERM");
      forceKillTimer = setTimeout(() => child.kill("SIGKILL"), TERMINATION_GRACE_MS);
    }, timeoutMs);

    child.on("error", (error) => {
      spawnError = error;
    });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      if (forceKillTimer) clearTimeout(forceKillTimer);
      const output = `${stdout.join("")}${stderr.join("")}`;
      if (timedOut) {
        reject(new Error(`${description} timed out after ${timeoutMs}ms\n${output}`));
        return;
      }
      if (spawnError) {
        reject(new Error(`${spawnError.message}\n${output}`));
        return;
      }
      if (code === 0) {
        resolve(stdout.join(""));
        return;
      }
      reject(
        new Error(`${description} exited with code=${code} signal=${signal ?? "none"}\n${output}`),
      );
    });
  });
}

function npmPackInvocation(
  platform = process.platform,
  comSpec = process.env.ComSpec,
): { command: string; args: string[] } {
  if (platform === "win32") {
    // Node requires .cmd files to run through cmd.exe. The command string is
    // deliberately fixed (rather than formed from values) before it reaches
    // cmd.exe, while ComSpec stays a direct executable argument to spawn.
    return {
      command: comSpec || "cmd.exe",
      args: ["/d", "/s", "/c", windowsNpmPackCommand],
    };
  }
  return { command: "npm", args: npmPackArgs };
}

describe("create-jazz package contents", () => {
  it("constructs the Windows npm invocation through cmd.exe without interpolation", () => {
    expect(npmPackInvocation("win32", "C:\\Windows\\System32\\cmd.exe")).toEqual({
      command: "C:\\Windows\\System32\\cmd.exe",
      args: ["/d", "/s", "/c", "npm.cmd pack --json --dry-run --ignore-scripts"],
    });
  });

  it(
    "prevents a timed-out child from performing a delayed side effect",
    { timeout: 2_000 },
    async () => {
      const probeDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-pack-watchdog-"));
      const marker = path.join(probeDir, "late-side-effect");
      try {
        await expect(
          runBoundedChild(
            process.execPath,
            [
              "-e",
              "const fs = require('node:fs'); setTimeout(() => { fs.writeFileSync(process.argv[1], process.pid.toString()); }, 250);",
              marker,
            ],
            packageRoot,
            50,
            "package watchdog probe",
          ),
        ).rejects.toThrow("package watchdog probe timed out after 50ms");

        await new Promise((resolve) => setTimeout(resolve, 300));
        expect(fs.existsSync(marker)).toBe(false);
      } finally {
        fs.rmSync(probeDir, { recursive: true, force: true });
      }
    },
  );

  it(
    "ships the bundled agent skills in npm pack output",
    { timeout: NPM_PACK_TIMEOUT_MS + TEST_CLEANUP_MARGIN_MS },
    async () => {
      const npm = npmPackInvocation();
      const output = await runBoundedChild(
        npm.command,
        npm.args,
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
