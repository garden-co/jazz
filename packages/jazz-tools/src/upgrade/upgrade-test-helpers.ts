/**
 * Fixture apps for `jazz-tools upgrade`'s tests. Each is a .64-era app
 * (schema.ts, permissions.ts, migrations/, and the jazz.app.json /
 * jazz.permissions.json the old validate wrote) read with the published
 * jazz-tools@2.0.0-alpha.63, which has the same DSL surface as alpha.64.
 * The tests never touch the workspace DSL: each fixture runs in a scratch
 * copy whose node_modules is an npm install of tests/upgrade/legacy-package.json.
 */
import { execFileSync, execSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const UPGRADE_TESTS = join(here, "..", "..", "tests", "upgrade");
export const FIXTURES = join(UPGRADE_TESTS, "fixtures");
const LEGACY = join(UPGRADE_TESTS, ".legacy-modules");

/** node_modules with jazz-tools@2.0.0-alpha.63 (aliased), its jazz-napi and typescript, installed once. */
export function legacyModules(): string {
  const modules = join(LEGACY, "node_modules");
  if (
    !existsSync(join(modules, "jazz-tools", "package.json")) ||
    !existsSync(join(modules, "jazz-napi", "package.json"))
  ) {
    mkdirSync(LEGACY, { recursive: true });
    cpSync(join(UPGRADE_TESTS, "legacy-package.json"), join(LEGACY, "package.json"));
    execSync(
      "npm install --no-package-lock --ignore-scripts --no-audit --no-fund --loglevel=error",
      {
        cwd: LEGACY,
        stdio: "pipe",
        timeout: 240_000,
      },
    );
  }
  return modules;
}

/** A scratch copy of a fixture app, with the legacy node_modules linked in. */
export function copyFixture(name: string): string {
  const root = mkdtempSync(join(tmpdir(), `jazz-upgrade-${name}-`));
  const dir = join(root, name);
  cpSync(join(FIXTURES, name), dir, {
    recursive: true,
    filter: (source) => {
      const rel = relative(join(FIXTURES, name), source);
      return rel !== "node_modules" && rel !== "expected" && !rel.startsWith(`expected/`);
    },
  });
  symlinkSync(legacyModules(), join(dir, "node_modules"), "dir");
  return dir;
}

export function removeFixture(dir: string): void {
  rmSync(dirname(dir), { recursive: true, force: true });
}

/** Every file of a converted app, relative, without node_modules. */
export function projectFiles(dir: string): Record<string, string> {
  const out: Record<string, string> = {};
  const walk = (current: string) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      if (entry.name === "node_modules") continue;
      const path = join(current, entry.name);
      if (entry.isDirectory()) walk(path);
      else out[relative(dir, path)] = readFileSync(path, "utf8");
    }
  };
  walk(dir);
  return out;
}

/**
 * jazz.generated.ts from jazz.app.json: jazz-napi's `generate`, or the
 * binary JAZZ_UPGRADE_TEST_GENERATOR names (a jazz-codegen build) where the
 * installed jazz-napi predates it. Undefined when neither exists.
 */
export async function testGenerator(): Promise<((appJson: string) => string) | undefined> {
  const binary = process.env.JAZZ_UPGRADE_TEST_GENERATOR;
  if (binary)
    return (appJson) => execFileSync(binary, ["ts"], { input: appJson, encoding: "utf8" });
  try {
    const napi = (await import("jazz-napi")) as unknown as {
      generate?: (lang: string, app: string) => string;
    };
    if (typeof napi.generate === "function") return (appJson) => napi.generate!("ts", appJson);
  } catch {
    // No native binding in this environment.
  }
  return undefined;
}
