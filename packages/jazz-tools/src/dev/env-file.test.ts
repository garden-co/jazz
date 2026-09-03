import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { loadEnvFileIntoProcessEnv, resolveViteEnvDir } from "./env-file.js";
import { createTempRootTracker } from "./test-helpers.js";

const tempRoots = createTempRootTracker();
const testKeys = [
  "JAZZ_BUG_121_FILE_PRECEDENCE",
  "JAZZ_BUG_121_PROCESS_PRECEDENCE",
  "JAZZ_BUG_121_BASE_ONLY",
  "JAZZ_BUG_121_LOCAL_ONLY",
  "JAZZ_BUG_121_MODE_ONLY",
  "JAZZ_BUG_121_MODE_LOCAL_ONLY",
  "JAZZ_BUG_121_EXPANDED",
] as const;
const originalValues = Object.fromEntries(testKeys.map((key) => [key, process.env[key]]));

afterEach(async () => {
  await tempRoots.cleanup();
  for (const key of testKeys) {
    const original = originalValues[key];
    if (original === undefined) delete process.env[key];
    else process.env[key] = original;
  }
});

describe("loadEnvFileIntoProcessEnv", () => {
  it("loads development env files in standard precedence and expands values", async () => {
    const root = await tempRoots.create("jazz-env-development-");
    await writeFile(
      join(root, ".env"),
      [
        "JAZZ_BUG_121_FILE_PRECEDENCE=base",
        "JAZZ_BUG_121_PROCESS_PRECEDENCE=base",
        "JAZZ_BUG_121_BASE_ONLY=base",
        "JAZZ_BUG_121_EXPANDED=${JAZZ_BUG_121_BASE_ONLY}/expanded",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(root, ".env.local"),
      [
        "JAZZ_BUG_121_FILE_PRECEDENCE=local",
        "JAZZ_BUG_121_PROCESS_PRECEDENCE=local",
        "JAZZ_BUG_121_LOCAL_ONLY=local",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(root, ".env.development"),
      [
        "JAZZ_BUG_121_FILE_PRECEDENCE=development",
        "JAZZ_BUG_121_PROCESS_PRECEDENCE=development",
        "JAZZ_BUG_121_MODE_ONLY=development",
        "",
      ].join("\n"),
    );
    await writeFile(
      join(root, ".env.development.local"),
      [
        "JAZZ_BUG_121_FILE_PRECEDENCE=development-local",
        "JAZZ_BUG_121_PROCESS_PRECEDENCE=development-local",
        "JAZZ_BUG_121_MODE_LOCAL_ONLY=development-local",
        "",
      ].join("\n"),
    );
    process.env.JAZZ_BUG_121_PROCESS_PRECEDENCE = "process";

    await loadEnvFileIntoProcessEnv(root, "development");

    expect(process.env.JAZZ_BUG_121_FILE_PRECEDENCE).toBe("development-local");
    expect(process.env.JAZZ_BUG_121_PROCESS_PRECEDENCE).toBe("process");
    expect(process.env.JAZZ_BUG_121_BASE_ONLY).toBe("base");
    expect(process.env.JAZZ_BUG_121_LOCAL_ONLY).toBe("local");
    expect(process.env.JAZZ_BUG_121_MODE_ONLY).toBe("development");
    expect(process.env.JAZZ_BUG_121_MODE_LOCAL_ONLY).toBe("development-local");
    expect(process.env.JAZZ_BUG_121_EXPANDED).toBe("base/expanded");
  });

  it("loads production files without leaking development values", async () => {
    const root = await tempRoots.create("jazz-env-production-");
    await writeFile(join(root, ".env"), "JAZZ_BUG_121_FILE_PRECEDENCE=base\n");
    await writeFile(join(root, ".env.development.local"), "JAZZ_BUG_121_MODE_ONLY=development\n");
    await writeFile(
      join(root, ".env.production"),
      "JAZZ_BUG_121_FILE_PRECEDENCE=production\nJAZZ_BUG_121_MODE_ONLY=production\n",
    );
    await writeFile(
      join(root, ".env.production.local"),
      "JAZZ_BUG_121_FILE_PRECEDENCE=production-local\n",
    );

    await loadEnvFileIntoProcessEnv(root, "production");

    expect(process.env.JAZZ_BUG_121_FILE_PRECEDENCE).toBe("production-local");
    expect(process.env.JAZZ_BUG_121_MODE_ONLY).toBe("production");
  });

  it("resolves Vite relative and absolute envDir values, and honors envFile:false", async () => {
    const root = await tempRoots.create("jazz-env-dir-root-");
    const absolute = await tempRoots.create("jazz-env-dir-absolute-");

    expect(resolveViteEnvDir(root, { envDir: "../shared-env" })).toBe(
      join(root, "..", "shared-env"),
    );
    expect(resolveViteEnvDir(root, { envDir: absolute })).toBe(absolute);
    expect(resolveViteEnvDir(root, { envDir: absolute, envFile: false })).toBe(false);
  });
});
