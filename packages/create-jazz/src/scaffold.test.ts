import { describe, it, expect, afterEach, beforeAll, beforeEach } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { execSync } from "node:child_process";
import { scaffold, validateAppName, type ScaffoldOptions } from "./scaffold.js";

const repoRoot = path.resolve(import.meta.dirname, "../../../");
const betterauthStarterPath = path.join(repoRoot, "starters/next-betterauth");
const localfirstStarterPath = path.join(repoRoot, "starters/next-localfirst");

// CI runners have no global git identity configured, so inject fallbacks
// via the env vars git honours. Production code still fails loudly when a
// real user has neither set — these only kick in for the test process.
beforeAll(() => {
  process.env.GIT_AUTHOR_NAME ??= "create-jazz tests";
  process.env.GIT_AUTHOR_EMAIL ??= "tests@create-jazz.invalid";
  process.env.GIT_COMMITTER_NAME ??= "create-jazz tests";
  process.env.GIT_COMMITTER_EMAIL ??= "tests@create-jazz.invalid";
});

/**
 * Swap JAZZ_STARTER_PATH for the duration of each test so scaffold() runs
 * its real local-fixture path (fs.cp + resolveLocalDeps) against the
 * in-repo starters. This covers strictly more real code than the old
 * dep-injection tests did.
 */
function withLocalStarter(starterPath: string) {
  let previous: string | undefined;
  beforeEach(() => {
    previous = process.env.JAZZ_STARTER_PATH;
    process.env.JAZZ_STARTER_PATH = starterPath;
  });
  afterEach(() => {
    if (previous === undefined) delete process.env.JAZZ_STARTER_PATH;
    else process.env.JAZZ_STARTER_PATH = previous;
  });
}

describe("scaffold() — existing-dir rejection", () => {
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("refuses to touch a directory that already exists, even when empty", async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "scaffold-exists-"));

    const options: ScaffoldOptions = {
      appName: "alice-app",
      targetDir: tmpDir,
      pm: null,
      git: false,
    };

    await expect(scaffold(options)).rejects.toThrow(/already exists/);
    // The dir must still be there — scaffold must never remove something it didn't create.
    expect(fs.existsSync(tmpDir)).toBe(true);
  });
});

describe("scaffold() — next-betterauth e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(betterauthStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete next-betterauth project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-betterauth-${Date.now()}`);

    await scaffold({
      appName: "alice-app",
      targetDir: tmpDir,
      pm: null,
      starter: "next-betterauth",
    });

    const pkgJsonPath = path.join(tmpDir, "package.json");
    expect(fs.existsSync(pkgJsonPath)).toBe(true);

    const pkgJson = JSON.parse(fs.readFileSync(pkgJsonPath, "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-app");

    // No workspace: or catalog: specifiers should survive dep resolution.
    const allDepValues = [
      ...Object.values(pkgJson.dependencies ?? {}),
      ...Object.values(pkgJson.devDependencies ?? {}),
    ];
    for (const value of allDepValues) {
      expect(value).not.toMatch(/^workspace:/);
      expect(value).not.toMatch(/^catalog:/);
    }

    // `.env.local` must not be copied from the starter source tree.
    expect(fs.existsSync(path.join(tmpDir, ".env.local"))).toBe(false);

    // The initial commit was made (default behaviour).
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);
    const log = execSync("git log --oneline", { cwd: tmpDir, stdio: "pipe" }).toString().trim();
    expect(log).not.toBe("");
  });

  it("skips git init when git: false", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-nogit-${Date.now()}`);

    await scaffold({
      appName: "bob-app",
      targetDir: tmpDir,
      pm: null,
      starter: "next-betterauth",
      git: false,
    });

    expect(fs.existsSync(path.join(tmpDir, "package.json"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(false);
  });
});

describe("scaffold() — next-localfirst e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(localfirstStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete next-localfirst project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-localfirst-${Date.now()}`);

    await scaffold({
      appName: "alice-localfirst",
      targetDir: tmpDir,
      pm: null,
      starter: "next-localfirst",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-localfirst");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);

    const allDepValues = [
      ...Object.values(pkgJson.dependencies ?? {}),
      ...Object.values(pkgJson.devDependencies ?? {}),
    ];
    for (const value of allDepValues) {
      expect(value).not.toMatch(/^workspace:/);
      expect(value).not.toMatch(/^catalog:/);
    }
  });
});

describe("scaffold() — unknown starter", () => {
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("rejects with a clear error and does not touch the filesystem", async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-unknown-${Date.now()}`);

    const err = await scaffold({
      appName: "bob-app",
      targetDir: tmpDir,
      pm: null,
      starter: "does-not-exist",
    }).catch((e: unknown) => e);

    expect(err).toBeInstanceOf(Error);
    expect((err as Error).message).toMatch(/Unknown starter/);
    expect((err as Error).message).toContain("does-not-exist");
    expect(fs.existsSync(tmpDir)).toBe(false);
  });
});

describe("validateAppName", () => {
  it("accepts a plain lowercase name", () => {
    expect(() => validateAppName("my-app")).not.toThrow();
  });

  it("accepts scoped names", () => {
    expect(() => validateAppName("@acme/my-app")).not.toThrow();
  });

  it("accepts names with dots and underscores", () => {
    expect(() => validateAppName("my.app_v2")).not.toThrow();
  });

  it("rejects names with whitespace", () => {
    expect(() => validateAppName("my app")).toThrow(/Invalid app name/);
  });

  it("rejects names with slashes outside a scope", () => {
    expect(() => validateAppName("my/app")).toThrow(/Invalid app name/);
  });

  it("rejects leading dot", () => {
    expect(() => validateAppName(".secret")).toThrow(/Invalid app name/);
  });

  it("rejects uppercase", () => {
    expect(() => validateAppName("MyApp")).toThrow(/Invalid app name/);
  });

  it("rejects empty strings", () => {
    expect(() => validateAppName("")).toThrow(/Invalid app name/);
  });
});
