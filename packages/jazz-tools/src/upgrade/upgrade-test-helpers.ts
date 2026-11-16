/**
 * Fixture apps for `jazz-tools upgrade`'s tests. Each is an alpha.65 app
 * (schema.ts with `defineApp`, permissions.ts, migrations/, and the
 * jazz.app.json / jazz.permissions.json its validate wrote), checked with the
 * published jazz-tools@2.0.0-alpha.65. Each runs in a scratch copy whose
 * node_modules is an npm install of tests/upgrade/legacy-package.json.
 */
import { execSync } from "node:child_process";
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

/** node_modules with jazz-tools@2.0.0-alpha.65, its jazz-napi and typescript, installed once. */
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

/** Step 6 for the tests: this workspace's `generate`, on the converted app. */
export async function testGenerate(schemaDir: string): Promise<void> {
  const { generateApp } = await import("../dev/project.js");
  await generateApp({ cwd: schemaDir });
}
