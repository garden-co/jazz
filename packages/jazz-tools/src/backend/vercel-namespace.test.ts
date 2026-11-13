import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { perBranch, vercel } from "./index.js";
import { logStoreOpened } from "./store-log.js";
import { coreName, DRIVE_NAME_MAX, driveName, SANDBOX_NAME_MAX } from "./vercel-store.js";

/**
 * The core name jazz-tools gave a deployment by default before `namespace`
 * (alpha.61), copied verbatim: the cores it created must keep their names.
 */
function legacyDefaultCoreName(project: string, env: Record<string, string | undefined>): string {
  const target = env.VERCEL_ENV ?? "development";
  const safe = (part: string) => part.toLowerCase().replace(/[^a-z0-9]+/g, "-");
  if (target !== "preview") return `jazz-${safe(project)}-${safe(target)}`;
  const branch = env.VERCEL_GIT_COMMIT_REF;
  if (!branch) throw new Error("no Git branch");
  const slug = branch
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 24);
  const hash = createHash("sha256").update(branch).digest("hex").slice(0, 8);
  return `jazz-${safe(project)}-preview-${slug}-${hash}`;
}

/** The documented recipe for those cores. */
const legacy = ({ environment, branch }: { environment: string; branch?: string }) =>
  environment === "production" ? "production" : perBranch(branch);

const PROJECTS = ["prj_1", "prj_test", "prj_AbC123dEf456GhI789jKl012MnO3"];
const BRANCHES = [
  "main",
  "dev",
  "feat/A",
  "feat-a",
  "Feature/UPPER_case",
  "renovate/@types-node-22.x",
  // Its slug is cut to 24 characters right after a "-": the name has "--".
  "abcdefghijklmnopqrstuvw-xyz",
  "a".repeat(80),
  "-leading-and-trailing-",
  "ünïcödé/branch",
  "日本語",
  "dependabot/npm_and_yarn/next-16.0.1",
];

describe("vercel() namespace", () => {
  it("names the cores earlier versions created by default identically with the perBranch recipe", () => {
    for (const project of PROJECTS) {
      const production = { VERCEL_ENV: "production", VERCEL_GIT_COMMIT_REF: "main" };
      expect(coreName(project, legacy, production).name).toBe(
        legacyDefaultCoreName(project, production),
      );
      for (const branch of BRANCHES) {
        const preview = { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: branch };
        expect(coreName(project, legacy, preview).name).toBe(
          legacyDefaultCoreName(project, preview),
        );
        // A custom environment ran with VERCEL_ENV=preview, so it had
        // per-branch cores too; its namespace function sees its slug.
        const custom = { ...preview, VERCEL_TARGET_ENV: "staging" };
        expect(coreName(project, legacy, custom).name).toBe(legacyDefaultCoreName(project, custom));
      }
    }
    // Pinned literally, not only against the copy above.
    expect(coreName("prj_1", legacy, { VERCEL_ENV: "production" }).name).toBe(
      "jazz-prj-1-production",
    );
    expect(
      coreName("prj_1", legacy, { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: "feat/A" }).name,
    ).toBe(`jazz-prj-1-preview-feat-a-${sha8("feat/A")}`);
    expect(perBranch("abcdefghijklmnopqrstuvw-xyz")).toBe(
      `preview-abcdefghijklmnopqrstuvw--${sha8("abcdefghijklmnopqrstuvw-xyz")}`,
    );
  });

  it("gives each preview branch its own core", () => {
    const env = (branch: string) => ({ VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: branch });
    const names = ["feat/A", "feat-a", "feat_a"].map((b) => coreName("prj_1", legacy, env(b)).name);
    expect(new Set(names).size).toBe(3);
  });

  it("passes the deployment's environment and branch to a namespace function", () => {
    const seen: unknown[] = [];
    const namespace = (context: { environment: string; branch?: string }) => {
      seen.push(context);
      return context.environment;
    };
    expect(coreName("prj_1", namespace, { VERCEL_ENV: "production" }).name).toBe(
      "jazz-prj-1-production",
    );
    coreName("prj_1", namespace, {
      VERCEL_ENV: "preview",
      VERCEL_TARGET_ENV: "staging",
      VERCEL_GIT_COMMIT_REF: "",
    });
    expect(seen).toEqual([
      { environment: "production", branch: undefined },
      { environment: "staging", branch: undefined },
    ]);
    expect(coreName("prj_1", "My_Core.v2", {}).name).toBe("jazz-prj-1-my-core-v2");
  });

  it("refuses a preview without a Git branch whose namespace function returns none", () => {
    // Deploys through the API, or from the CLI outside a Git checkout, run
    // with an empty branch; they must not all share one core.
    for (const ref of ["", undefined]) {
      expect(() =>
        coreName("prj_1", legacy, { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: ref }),
      ).toThrow(/preview deployment has no Git branch.*return a namespace for it/);
    }
    expect(() => coreName("prj_1", () => "", { VERCEL_ENV: "production" })).toThrow(
      /namespace function returned no namespace for this production deployment/,
    );
  });

  it("is required, with the usual choices to copy", () => {
    expect(() => vercel({} as never)).toThrow(/pass `namespace`/);
    expect(() => vercel(undefined as never)).toThrow(/pass `namespace`/);
    let message = "";
    try {
      vercel({} as never);
    } catch (error) {
      message = String(error);
    }
    expect(message).toContain("vercel({ namespace: ({ environment }) => environment })");
    expect(message).toContain(
      'vercel({ namespace: ({ environment, branch }) => environment === "production" ? "production" : perBranch(branch) })',
    );
  });

  it("refuses `name`, saying how to migrate", () => {
    expect(() => vercel({ name: "jazz-prj-1-production" } as never)).toThrow(
      /`name` is now `namespace`.*namespace: "<rest>".*perBranch/,
    );
    expect(() => vercel({ namespace: "production", name: "x" } as never)).toThrow(
      /`name` is now `namespace`/,
    );
    // With the project known, the namespace that keeps the core, exactly.
    const credentials = { token: "t", teamId: "team_1", projectId: "prj_AbC1" };
    expect(() => vercel({ name: "jazz-prj-abc1-staging-eu", credentials } as never)).toThrow(
      'To keep the core jazz-prj-abc1-staging-eu, pass `namespace: "staging-eu"`.',
    );
    // Any other name can't be reached, and the error says so.
    for (const name of ["myapp-core", "jazz-prj_AbC1-prod", "jazz-prj-abc1-Prod"]) {
      expect(() => vercel({ name, credentials } as never)).toThrow(
        /is not of that form \(for this project it would start with jazz-prj-abc1-\), so no namespace reaches it/,
      );
    }
    expect(() => vercel({ name: "myapp-core" } as never)).toThrow(
      /lowercased with each run of characters other than a-z and 0-9 as one `-` \(prj_AbC1 is prj-abc1\).*a name of any other form can't be reached/,
    );
  });

  it("refuses an empty or oversized namespace", () => {
    expect(() => vercel({ namespace: "" })).toThrow(/namespace is empty/);
    expect(() => vercel({ namespace: "--" })).toThrow(/no letters or digits/);
    expect(() => vercel({ namespace: 3 as never })).toThrow(/must be a string/);
    // With a typical project id (32 characters) the prefix takes 38.
    expect(() => vercel({ namespace: "a".repeat(SANDBOX_NAME_MAX - 38) })).not.toThrow();
    expect(() => vercel({ namespace: "a".repeat(SANDBOX_NAME_MAX - 37) })).toThrow(
      /is 91 characters; with this project's prefix jazz-prj-x+- it may have at most 90/,
    );
    expect(() => coreName("prj_1", () => "a".repeat(200), {})).toThrow(/at most 117/);
  });

  it("keeps a core's Drive name, and shortens one Vercel would refuse", () => {
    const fits = `jazz-prj-1-${"a".repeat(DRIVE_NAME_MAX - 16)}`;
    expect(driveName(fits)).toBe(`${fits}-data`);
    const project = "prj_AbC123dEf456GhI789jKl012MnO3";
    const branchCores = BRANCHES.map(
      (branch) =>
        coreName(project, legacy, { VERCEL_ENV: "preview", VERCEL_GIT_COMMIT_REF: branch }).name,
    );
    const drives = branchCores.map(driveName);
    for (const drive of drives) {
      expect(drive.length).toBeLessThanOrEqual(DRIVE_NAME_MAX);
      expect(drive).toMatch(/^jazz-prj-abc123def456ghi789jkl012mno3-.*-data$/);
    }
    expect(new Set(drives).size).toBe(new Set(branchCores).size);
    // A pure function of the core's name: a core always finds its Drive
    // again. Pinned literally, for a name that fits and one that doesn't.
    expect(driveName("jazz-prj-abc123def456ghi789jkl012mno3-production")).toBe(
      "jazz-prj-abc123def456ghi789jkl012mno3-production-data",
    );
    const long = coreName(project, legacy, {
      VERCEL_ENV: "preview",
      VERCEL_GIT_COMMIT_REF: "feature/login-page",
    }).name;
    expect(long).toBe("jazz-prj-abc123def456ghi789jkl012mno3-preview-feature-login-page-a0e5be53");
    expect(driveName(long)).toBe(
      "jazz-prj-abc123def456ghi789jkl012mno3-preview-feat-d8996cfa-data",
    );
    expect(driveName(long)).toBe(driveName(long));
  });

  it("logs each store it opens once per process", () => {
    const log = vi.fn();
    const line = `jazz: store vercel namespace "test-${Math.random()}" (core jazz-prj-1-x)`;
    logStoreOpened(line, log);
    logStoreOpened(line, log);
    logStoreOpened(`${line} other`, log);
    expect(log.mock.calls).toEqual([[line], [`${line} other`]]);
  });
});

function sha8(text: string): string {
  return createHash("sha256").update(text).digest("hex").slice(0, 8);
}
