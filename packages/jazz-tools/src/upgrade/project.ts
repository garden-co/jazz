/**
 * The project `jazz-tools upgrade` converts: where its app files and TS
 * sources are, its tsconfig, and the TypeScript it compiles with.
 */
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, relative, resolve } from "node:path";
import type * as TS from "typescript";
import { UpgradeRefusal } from "./errors.js";

export type Ts = typeof TS;

/** Where a project keeps schema.ts, as validate looks for it. */
export const SCHEMA_ROOTS = [".", "src", join("src", "lib")];

export const APP_FILE = "jazz.app.json";
export const PERMISSIONS_FILE = "jazz.permissions.json";
export const GENERATED_FILE = "jazz.generated.ts";
export const COMPUTED_FILE = "jazz.computed.ts";
/** What schema.ts exported besides the schema, once it is gone. */
export const EXTRAS_FILE = "app-extras.ts";

/** Sources upgrade reads and rewrites. */
const SOURCE_EXTENSIONS = new Set([
  ".ts",
  ".tsx",
  ".mts",
  ".cts",
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
  ".svelte",
  ".vue",
]);
/** Directories that hold build output, dependencies or tooling, never sources. */
const SKIPPED_DIRS = new Set([
  "node_modules",
  "dist",
  "build",
  "out",
  ".next",
  ".svelte-kit",
  ".turbo",
  ".vercel",
  ".output",
  ".expo",
  ".wrangler",
  "target",
  "coverage",
  "android",
  "ios",
  "playwright-report",
  "test-results",
]);

export interface Project {
  /** The project root (`--dir`). */
  root: string;
  /** The directory holding schema.ts and the app files. */
  schemaDir: string;
  schemaFile: string;
  permissionsFile: string | undefined;
  migrationsDir: string | undefined;
  appFile: string;
  permissionsJsonFile: string;
  ts: Ts;
  tsconfig: string | undefined;
  compilerOptions: TS.CompilerOptions;
  /** The tsconfig's own files, for the typecheck. */
  configFileNames: string[];
  /** Every source file of the project, absolute. */
  sources: string[];
}

export function locateProject(root: string): Project {
  root = resolve(root);
  if (!existsSync(root) || !statSync(root).isDirectory()) {
    throw new UpgradeRefusal(`${root} is not a directory`);
  }
  const schemaDir = SCHEMA_ROOTS.map((r) => join(root, r)).find((dir) =>
    existsSync(join(dir, "schema.ts")),
  );
  if (!schemaDir) {
    if (SCHEMA_ROOTS.some((r) => existsSync(join(root, r, APP_FILE)))) {
      throw new UpgradeRefusal(
        `${root} has jazz.app.json but no schema.ts: it is already on files as source, nothing to upgrade`,
      );
    }
    throw new UpgradeRefusal(
      `no schema.ts in ${root}, ${join(root, "src")} or ${join(root, "src", "lib")}: run upgrade in the app's directory (or pass --dir)`,
    );
  }
  const file = (name: string) =>
    existsSync(join(schemaDir, name)) ? join(schemaDir, name) : undefined;
  const ts = loadTypeScript(root);
  const { tsconfig, options, fileNames } = readTsconfig(ts, root);
  return {
    root,
    schemaDir,
    schemaFile: join(schemaDir, "schema.ts"),
    permissionsFile: file("permissions.ts"),
    migrationsDir: existsSync(join(schemaDir, "migrations"))
      ? join(schemaDir, "migrations")
      : undefined,
    appFile: join(schemaDir, APP_FILE),
    permissionsJsonFile: join(schemaDir, PERMISSIONS_FILE),
    ts,
    tsconfig,
    compilerOptions: options,
    configFileNames: fileNames,
    sources: listSources(root),
  };
}

/** The project's TypeScript: upgrade reads schema.ts with the compiler the app builds with. */
export function loadTypeScript(root: string): Ts {
  const require = createRequire(join(root, "package.json"));
  try {
    return require("typescript") as Ts;
  } catch {
    throw new UpgradeRefusal(
      `upgrade reads schema.ts with the TypeScript compiler, but typescript isn't installed in ${root}: add it as a dev dependency and run upgrade again`,
    );
  }
}

function readTsconfig(
  ts: Ts,
  root: string,
): { tsconfig: string | undefined; options: TS.CompilerOptions; fileNames: string[] } {
  const tsconfig = ts.findConfigFile(root, (path) => ts.sys.fileExists(path), "tsconfig.json");
  // Only the project's own tsconfig, not a parent's in a monorepo.
  if (!tsconfig || relative(root, tsconfig).startsWith("..")) {
    return {
      tsconfig: undefined,
      options: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        strict: true,
        skipLibCheck: true,
        jsx: ts.JsxEmit.ReactJSX,
        allowImportingTsExtensions: true,
        noEmit: true,
      },
      fileNames: [],
    };
  }
  const parsed = ts.getParsedCommandLineOfConfigFile(
    tsconfig,
    {},
    {
      ...ts.sys,
      // A missing extended config (SvelteKit's before `svelte-kit sync`) still leaves usable options.
      onUnRecoverableConfigFileDiagnostic: () => {},
    },
  );
  return {
    tsconfig,
    options: { ...parsed?.options, noEmit: true },
    fileNames: parsed?.fileNames ?? [],
  };
}

function listSources(root: string): string[] {
  const out: string[] = [];
  const walk = (dir: string, top: boolean): void => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    // A nested app with its own package.json is its own project.
    if (!top && entries.some((e) => e.name === "package.json")) return;
    for (const entry of entries) {
      if (entry.name.startsWith(".")) continue;
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        if (!SKIPPED_DIRS.has(entry.name)) walk(path, false);
      } else if (
        entry.isFile() &&
        SOURCE_EXTENSIONS.has(extname(entry.name)) &&
        !entry.name.endsWith(".d.ts")
      ) {
        out.push(path);
      }
    }
  };
  walk(root, true);
  return out.sort();
}

/** The version of the jazz-tools package at `dir`. */
export function packageVersion(dir: string): string {
  return (JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as { version: string })
    .version;
}

/** The root of the jazz-tools package running this command. */
export function runningPackageRoot(): string {
  let dir = dirname(new URL(import.meta.url).pathname);
  while (!existsSync(join(dir, "package.json"))) {
    const parent = dirname(dir);
    if (parent === dir) throw new Error("jazz-tools: can't find its own package.json");
    dir = parent;
  }
  return dir;
}
