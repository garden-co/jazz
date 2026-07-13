import { describe, expect, it, vi } from "vitest";
import { intentInstallCommand, setupAgentSkills } from "./agent-skills.js";

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

    setupAgentSkills("/tmp/my-jazz-app", "pnpm", run);

    expect(run).toHaveBeenCalledWith(
      "pnpm",
      ["dlx", "@tanstack/intent@latest", "install", "--no-notices"],
      { cwd: "/tmp/my-jazz-app", stdio: "pipe" },
    );
  });

  it("surfaces command failures with their stderr", () => {
    const run = vi.fn(() => {
      throw Object.assign(new Error("command failed"), { stderr: "network unavailable" });
    });

    expect(() => setupAgentSkills("/tmp/my-jazz-app", "npm", run)).toThrow(
      /Jazz coding skill setup failed: network unavailable/,
    );
  });
});
