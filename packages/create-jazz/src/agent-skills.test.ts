import { afterEach, describe, expect, it, vi } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { intentInstallCommand, setupAgentSkills } from "./agent-skills.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function createProject(packageJson: Record<string, unknown> = {}): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-agent-skills-"));
  tempDirs.push(dir);
  fs.writeFileSync(path.join(dir, "package.json"), `${JSON.stringify(packageJson, null, 2)}\n`);
  return dir;
}

describe("intentInstallCommand", () => {
  it.each([
    ["npm", "npx", ["--yes", "@tanstack/intent@latest", "install", "--no-notices"]],
    ["pnpm", "pnpm", ["dlx", "@tanstack/intent@latest", "install", "--no-notices"]],
    ["yarn", "yarn", ["dlx", "@tanstack/intent@latest", "install", "--no-notices"]],
    ["bun", "bunx", ["@tanstack/intent@latest", "install", "--no-notices"]],
  ])("uses the official Intent runner for %s", (packageManager, executable, args) => {
    expect(intentInstallCommand(packageManager)).toEqual({ executable, args });
  });

  it("rejects unsupported package managers", () => {
    expect(() => intentInstallCommand("deno")).toThrow(/Cannot set up Jazz coding skills/);
  });
});

describe("setupAgentSkills", () => {
  it("runs Intent from the scaffolded project", () => {
    const run = vi.fn();
    const dir = createProject();

    setupAgentSkills(dir, "pnpm", run);

    expect(run).toHaveBeenCalledWith(
      "pnpm",
      ["dlx", "@tanstack/intent@latest", "install", "--no-notices"],
      { cwd: dir, stdio: "pipe" },
    );
    expect(JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"))).toMatchObject({
      intent: { skills: ["jazz-tools"] },
    });
  });

  it("preserves existing Intent configuration", () => {
    const dir = createProject({ intent: { skills: ["other-skills"], exclude: ["legacy"] } });

    setupAgentSkills(dir, "npm", vi.fn());

    expect(JSON.parse(fs.readFileSync(path.join(dir, "package.json"), "utf8"))).toMatchObject({
      intent: { skills: ["other-skills", "jazz-tools"], exclude: ["legacy"] },
    });
  });

  it("surfaces command failures with their stderr", () => {
    const dir = createProject();
    const run = vi.fn(() => {
      throw Object.assign(new Error("command failed"), { stderr: "network unavailable" });
    });

    expect(() => setupAgentSkills(dir, "npm", run)).toThrow(
      /Jazz coding skill setup failed: network unavailable/,
    );
  });
});
