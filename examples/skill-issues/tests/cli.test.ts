import { chmod, mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { configPath } from "../src/config.js";
import { runCli } from "../src/cli.js";

describe("skill issues CLI", () => {
  it("initializes local-first auth config", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));

    const result = await runCli(["auth", "init"], {
      cwd: home,
      env: {
        SKILL_ISSUES_APP_ID: "app-id",
        SKILL_ISSUES_SERVER_URL: "https://cloud.example",
      },
    });

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain("Initialized skill issues auth");

    const config = JSON.parse(
      await readFile(join(home, ".skill-issues", "config.json"), "utf8"),
    ) as unknown;

    expect(config).toMatchObject({
      appId: "app-id",
      serverUrl: "https://cloud.example",
    });
    expect(config).toHaveProperty("localFirstSecret");
    expect(typeof (config as { localFirstSecret?: unknown }).localFirstSecret).toBe("string");
  });

  it("rejects unknown commands", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));

    const result = await runCli(["wat"], { cwd: home, env: {} });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain("Unknown command: wat");
  });

  it("restricts an existing config file to owner-only permissions", async () => {
    const home = await mkdtemp(join(tmpdir(), "skill-issues-cli-"));
    const path = configPath(home);
    await mkdir(join(home, ".skill-issues"), { recursive: true });
    await writeFile(path, "{}\n");
    await chmod(path, 0o644);

    const result = await runCli(["auth", "init"], {
      cwd: home,
      env: {
        SKILL_ISSUES_APP_ID: "app-id",
        SKILL_ISSUES_SERVER_URL: "https://cloud.example",
      },
    });

    expect(result.exitCode).toBe(0);
    expect((await stat(path)).mode & 0o777).toBe(0o600);
  });
});
