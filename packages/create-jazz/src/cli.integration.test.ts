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
      const env: NodeJS.ProcessEnv = {
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

  it("prints the deferred setup command when --skills is used without installing dependencies", () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-cli-e2e-skills-"));

    const env: NodeJS.ProcessEnv = {
      ...process.env,
      JAZZ_STARTER_PATH: path.join(repoRoot, "starters/next-localfirst"),
    };
    delete env.npm_config_user_agent;

    const result = spawnSync(
      tsxBin,
      [cliEntry, "skills-app", "--starter", "next-localfirst", "--skills", "--no-git"],
      {
        cwd: tmpDir,
        env,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    expect(result.status, result.stdout + result.stderr).toBe(0);
    expect(result.stdout + result.stderr).toContain("npx @tanstack/intent@latest install");
    expect(fs.existsSync(path.join(tmpDir, "skills-app", "package.json"))).toBe(true);
  });

  it("keeps a scaffolded app when optional Intent setup fails", { timeout: 60_000 }, () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-cli-e2e-skills-failure-"));
    const binDir = path.join(tmpDir, "bin");
    fs.mkdirSync(binDir);
    const fakePnpm = path.join(binDir, "pnpm");
    fs.writeFileSync(
      fakePnpm,
      `#!/usr/bin/env node
if (process.argv[2] === "install") process.exit(0);
console.error("network unavailable");
process.exit(1);
`,
    );
    fs.chmodSync(fakePnpm, 0o755);

    const env: NodeJS.ProcessEnv = {
      ...process.env,
      JAZZ_STARTER_PATH: path.join(repoRoot, "starters/next-localfirst"),
      npm_config_user_agent: "pnpm/10.0.0",
      PATH: `${binDir}${path.delimiter}${process.env.PATH ?? ""}`,
    };

    const result = spawnSync(
      tsxBin,
      [cliEntry, "skills-failure-app", "--starter", "next-localfirst", "--skills", "--no-git"],
      {
        cwd: tmpDir,
        env,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    const output = result.stdout + result.stderr;
    expect(result.status, output).toBe(0);
    expect(output).toContain("Jazz coding skill setup failed: network unavailable");
    expect(output).toContain(
      "Your app is ready. Retry skill setup with: pnpm dlx @tanstack/intent@latest install --no-notices",
    );
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(tmpDir, "skills-failure-app", "package.json"), "utf8"),
    ) as { intent?: { skills?: string[] } };
    expect(packageJson.intent?.skills).toEqual(["jazz-tools"]);
  });

  it("rejects contradictory skill setup flags before scaffolding", () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-cli-e2e-skills-flags-"));

    const env = { ...process.env };
    delete env.npm_config_user_agent;

    const result = spawnSync(
      tsxBin,
      [
        cliEntry,
        "conflicting-skills-app",
        "--starter",
        "next-localfirst",
        "--skills",
        "--no-skills",
      ],
      {
        cwd: tmpDir,
        env,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "pipe"],
      },
    );

    expect(result.status).not.toBe(0);
    expect(result.stdout + result.stderr).toContain("Choose either --skills or --no-skills");
    expect(fs.existsSync(path.join(tmpDir, "conflicting-skills-app"))).toBe(false);
  });
});
