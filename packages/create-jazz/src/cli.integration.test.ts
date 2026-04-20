import { describe, it, expect, afterEach } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const srcDir = path.dirname(fileURLToPath(import.meta.url));
const packageRoot = path.resolve(srcDir, "..");
const repoRoot = path.resolve(packageRoot, "../..");
const cliEntry = path.join(srcDir, "index.ts");
const tsxBin = path.join(packageRoot, "node_modules/.bin/tsx");

describe("create-jazz CLI end-to-end", () => {
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it(
    "scaffolds a project via `tsx src/index.ts <name> --starter next-betterauth`",
    { timeout: 60_000 },
    () => {
      tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-cli-e2e-"));

      const starterPath = path.join(repoRoot, "starters/next-betterauth");
      const env = {
        ...process.env,
        JAZZ_STARTER_PATH: starterPath,
        // CI runners have no global git identity; inject fallbacks so the
        // real git-init path gets exercised end-to-end.
        GIT_AUTHOR_NAME: process.env.GIT_AUTHOR_NAME ?? "create-jazz tests",
        GIT_AUTHOR_EMAIL: process.env.GIT_AUTHOR_EMAIL ?? "tests@create-jazz.invalid",
        GIT_COMMITTER_NAME: process.env.GIT_COMMITTER_NAME ?? "create-jazz tests",
        GIT_COMMITTER_EMAIL: process.env.GIT_COMMITTER_EMAIL ?? "tests@create-jazz.invalid",
      };
      // Drop npm_config_user_agent so detectPackageManager returns null and
      // the scaffold pipeline skips the install step — we want to verify
      // the CLI wiring, not a real pm install.
      delete env.npm_config_user_agent;

      const result = spawnSync(tsxBin, [cliEntry, "my-e2e-app", "--starter", "next-betterauth"], {
        cwd: tmpDir,
        env,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "pipe"],
      });

      expect(
        result.status,
        `CLI exited non-zero. stdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      ).toBe(0);

      const scaffoldedDir = path.join(tmpDir, "my-e2e-app");
      expect(fs.existsSync(scaffoldedDir), "scaffolded directory should exist").toBe(true);
      expect(
        fs.existsSync(path.join(scaffoldedDir, "package.json")),
        "package.json should exist",
      ).toBe(true);
      expect(fs.existsSync(path.join(scaffoldedDir, ".git")), ".git should exist").toBe(true);

      const pkg = JSON.parse(
        fs.readFileSync(path.join(scaffoldedDir, "package.json"), "utf-8"),
      ) as { name?: string; dependencies?: Record<string, string> };

      expect(pkg.name).toBe("my-e2e-app");
      for (const value of Object.values(pkg.dependencies ?? {})) {
        expect(value).not.toMatch(/^workspace:/);
        expect(value).not.toMatch(/^catalog:/);
      }
    },
  );

  it("fails with a clear error when an unknown starter is passed", { timeout: 15_000 }, () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-cli-e2e-unknown-"));

    const env = { ...process.env };
    delete env.npm_config_user_agent;

    const result = spawnSync(
      tsxBin,
      [cliEntry, "will-not-be-scaffolded", "--starter", "does-not-exist"],
      {
        cwd: tmpDir,
        env,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    expect(result.status).not.toBe(0);
    expect(result.stdout + result.stderr).toMatch(/Unknown starter/);
    expect(fs.existsSync(path.join(tmpDir, "will-not-be-scaffolded"))).toBe(false);
  });
});
