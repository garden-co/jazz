import { describe, it, expect, afterEach, beforeAll, beforeEach } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { execSync } from "node:child_process";
import { scaffold, validateAppName, type ScaffoldOptions } from "./scaffold.js";

const repoRoot = path.resolve(import.meta.dirname, "../../../");
const betterauthStarterPath = path.join(repoRoot, "starters/next-betterauth");
const localfirstStarterPath = path.join(repoRoot, "starters/next-localfirst");
const hybridStarterPath = path.join(repoRoot, "starters/next-hybrid");
const sveltekitBetterauthStarterPath = path.join(repoRoot, "starters/sveltekit-betterauth");
const sveltekitLocalfirstStarterPath = path.join(repoRoot, "starters/sveltekit-localfirst");
const sveltekitHybridStarterPath = path.join(repoRoot, "starters/sveltekit-hybrid");
const tsBetterauthStarterPath = path.join(repoRoot, "starters/ts-betterauth");
const tsLocalfirstStarterPath = path.join(repoRoot, "starters/ts-localfirst");
const tsHybridStarterPath = path.join(repoRoot, "starters/ts-hybrid");
const reactBetterauthStarterPath = path.join(repoRoot, "starters/react-betterauth");
const reactLocalfirstStarterPath = path.join(repoRoot, "starters/react-localfirst");
const reactHybridStarterPath = path.join(repoRoot, "starters/react-hybrid");

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

    // `.env` must not be copied from the starter source tree.
    expect(fs.existsSync(path.join(tmpDir, ".env"))).toBe(false);

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

  it(
    "passes onStep to preInstall so the spinner advances past git init",
    { timeout: 30_000 },
    async () => {
      tmpDir = path.join(os.tmpdir(), `scaffold-preinstall-${Date.now()}`);
      const steps: string[] = [];

      await scaffold({
        appName: "dave-preinstall",
        targetDir: tmpDir,
        pm: null,
        starter: "next-betterauth",
        git: false,
        onStep: (label) => steps.push(label),
        preInstall: async (ctx) => {
          expect(typeof ctx).toBe("object");
          expect(typeof ctx.dir).toBe("string");
          expect(typeof ctx.onStep).toBe("function");
          ctx.onStep("Provisioning Jazz Cloud app");
        },
      });

      expect(steps).toContain("Provisioning Jazz Cloud app");
    },
  );

  it("reports per-package progress during dep resolution", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-progress-${Date.now()}`);
    const steps: string[] = [];

    await scaffold({
      appName: "carol-progress",
      targetDir: tmpDir,
      pm: null,
      starter: "next-betterauth",
      git: false,
      onStep: (label) => steps.push(label),
    });

    const progressSteps = steps.filter((s) => s.startsWith("Resolving dependencies ("));
    expect(progressSteps.length).toBeGreaterThan(0);
    for (const step of progressSteps) {
      expect(step).toMatch(/^Resolving dependencies \(\d+\/\d+\)$/);
    }
    const last = progressSteps.at(-1)!;
    const [, resolved, total] = last.match(/\((\d+)\/(\d+)\)/)!;
    expect(Number(resolved)).toBe(Number(total));
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

describe("scaffold() — sveltekit-betterauth e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(sveltekitBetterauthStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete sveltekit-betterauth project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-sveltekit-ba-${Date.now()}`);

    await scaffold({
      appName: "alice-sveltekit",
      targetDir: tmpDir,
      pm: null,
      starter: "sveltekit-betterauth",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-sveltekit");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);

    // SvelteKit starters have src/lib/ structure
    expect(fs.existsSync(path.join(tmpDir, "src/lib/schema.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "svelte.config.js"))).toBe(true);

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

describe("scaffold() — next-hybrid e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(hybridStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete next-hybrid project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-next-hybrid-${Date.now()}`);

    await scaffold({
      appName: "alice-hybrid",
      targetDir: tmpDir,
      pm: null,
      starter: "next-hybrid",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-hybrid");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);

    // Hybrid starters include server-side auth wiring.
    expect(fs.existsSync(path.join(tmpDir, "lib/auth.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "lib/auth-client.ts"))).toBe(true);

    // Hybrid starters ship an env initialisation script.
    expect(fs.existsSync(path.join(tmpDir, "scripts/ensure-env.js"))).toBe(true);

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

describe("scaffold() — sveltekit-localfirst e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(sveltekitLocalfirstStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete sveltekit-localfirst project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-sveltekit-lf-${Date.now()}`);

    await scaffold({
      appName: "alice-sveltekit-lf",
      targetDir: tmpDir,
      pm: null,
      starter: "sveltekit-localfirst",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-sveltekit-lf");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);

    // SvelteKit starters have src/lib/ structure.
    expect(fs.existsSync(path.join(tmpDir, "src/lib/schema.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "svelte.config.js"))).toBe(true);

    // Local-first starters have no server-side auth module.
    expect(fs.existsSync(path.join(tmpDir, "src/lib/auth.ts"))).toBe(false);

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

describe("scaffold() — sveltekit-hybrid e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(sveltekitHybridStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete sveltekit-hybrid project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-sveltekit-hybrid-${Date.now()}`);

    await scaffold({
      appName: "alice-sveltekit-hybrid",
      targetDir: tmpDir,
      pm: null,
      starter: "sveltekit-hybrid",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-sveltekit-hybrid");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);

    // SvelteKit starters have src/lib/ structure.
    expect(fs.existsSync(path.join(tmpDir, "src/lib/schema.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "svelte.config.js"))).toBe(true);

    // Hybrid starters include server-side auth wiring and sign-up/sign-in routes.
    expect(fs.existsSync(path.join(tmpDir, "src/lib/auth.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/lib/auth-client.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/routes/signup/+page.svelte"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/routes/signin/+page.svelte"))).toBe(true);

    // Hybrid starters ship an env initialisation script.
    expect(fs.existsSync(path.join(tmpDir, "scripts/ensure-env.js"))).toBe(true);

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

describe("scaffold() — ts-localfirst e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(tsLocalfirstStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete ts-localfirst project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-ts-localfirst-${Date.now()}`);

    await scaffold({
      appName: "alice-ts-localfirst",
      targetDir: tmpDir,
      pm: null,
      starter: "ts-localfirst",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-ts-localfirst");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/main.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/todo-widget.ts"))).toBe(true);
    // No React in the dep tree.
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).not.toHaveProperty("react");
    expect(allDeps).not.toHaveProperty("@vitejs/plugin-react");

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

describe("scaffold() — ts-hybrid e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(tsHybridStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete ts-hybrid project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-ts-hybrid-${Date.now()}`);

    await scaffold({
      appName: "alice-ts-hybrid",
      targetDir: tmpDir,
      pm: null,
      starter: "ts-hybrid",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-ts-hybrid");
    expect(fs.existsSync(path.join(tmpDir, "server/auth.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/sign-up-form.ts"))).toBe(true);
    expect(pkgJson.dependencies).toHaveProperty("better-auth");
    expect(pkgJson.dependencies).toHaveProperty("hono");
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).not.toHaveProperty("react");
  });
});

describe("scaffold() — ts-betterauth e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(tsBetterauthStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete ts-betterauth project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-ts-betterauth-${Date.now()}`);

    await scaffold({
      appName: "alice-ts-betterauth",
      targetDir: tmpDir,
      pm: null,
      starter: "ts-betterauth",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-ts-betterauth");
    expect(fs.existsSync(path.join(tmpDir, "server/auth.ts"))).toBe(true);
    // No sign-up-form.ts — the combined form lives in sign-in-form.ts.
    expect(fs.existsSync(path.join(tmpDir, "src/sign-up-form.ts"))).toBe(false);
    expect(fs.existsSync(path.join(tmpDir, "src/sign-in-form.ts"))).toBe(true);
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).not.toHaveProperty("react");
  });
});

describe("scaffold() — react-localfirst e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(reactLocalfirstStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete react-localfirst project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-react-localfirst-${Date.now()}`);

    await scaffold({
      appName: "alice-react-localfirst",
      targetDir: tmpDir,
      pm: null,
      starter: "react-localfirst",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-react-localfirst");
    expect(fs.existsSync(path.join(tmpDir, ".git"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/main.tsx"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/App.tsx"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/todo-widget.tsx"))).toBe(true);
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).toHaveProperty("react");
    expect(allDeps).toHaveProperty("@vitejs/plugin-react");
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

describe("scaffold() — react-hybrid e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(reactHybridStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete react-hybrid project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-react-hybrid-${Date.now()}`);

    await scaffold({
      appName: "alice-react-hybrid",
      targetDir: tmpDir,
      pm: null,
      starter: "react-hybrid",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-react-hybrid");
    expect(fs.existsSync(path.join(tmpDir, "server/auth.ts"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/sign-up-form.tsx"))).toBe(true);
    expect(fs.existsSync(path.join(tmpDir, "src/sign-in-form.tsx"))).toBe(true);
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).toHaveProperty("react");
    expect(allDeps).toHaveProperty("better-auth");
  });
});

describe("scaffold() — react-betterauth e2e via JAZZ_STARTER_PATH", () => {
  withLocalStarter(reactBetterauthStarterPath);
  let tmpDir: string;

  afterEach(() => {
    if (tmpDir && fs.existsSync(tmpDir)) {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("scaffolds a complete react-betterauth project", { timeout: 30_000 }, async () => {
    tmpDir = path.join(os.tmpdir(), `scaffold-react-betterauth-${Date.now()}`);

    await scaffold({
      appName: "alice-react-betterauth",
      targetDir: tmpDir,
      pm: null,
      starter: "react-betterauth",
    });

    const pkgJson = JSON.parse(fs.readFileSync(path.join(tmpDir, "package.json"), "utf-8")) as {
      name?: string;
      dependencies?: Record<string, string>;
      devDependencies?: Record<string, string>;
    };

    expect(pkgJson.name).toBe("alice-react-betterauth");
    expect(fs.existsSync(path.join(tmpDir, "server/auth.ts"))).toBe(true);
    // No sign-up-form.tsx — the combined form lives in sign-in-form.tsx.
    expect(fs.existsSync(path.join(tmpDir, "src/sign-up-form.tsx"))).toBe(false);
    expect(fs.existsSync(path.join(tmpDir, "src/sign-in-form.tsx"))).toBe(true);
    const allDeps = { ...pkgJson.dependencies, ...pkgJson.devDependencies };
    expect(allDeps).toHaveProperty("react");
    expect(allDeps).toHaveProperty("better-auth");
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
