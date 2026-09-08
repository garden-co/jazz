import { execFileSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const wrapper = join(packageRoot, "bin", "jazz-tools.js");

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
});
