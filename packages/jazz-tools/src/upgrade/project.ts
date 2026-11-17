/**
 * The project `jazz-tools upgrade` converts: where its app files and TS
 * sources are, its tsconfig, and the TypeScript it compiles with.
 */
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, extname, join, relative, resolve, sep } from "node:path";
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
  // Inline `<script type="module">` blocks.
  ".html",
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
  /**
   * permissions.ts and the modules whose default export it re-exports
   * (`export { default } from "./src/db/access"`): importing any of them
   * imports the permissions.
   */
  permissionsSources: string[];
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

export function locateProject(root: string, schemaDirOption?: string): Project {
  root = resolve(root);
  if (!existsSync(root) || !statSync(root).isDirectory()) {
    throw new UpgradeRefusal(`${root} is not a directory`);
  }
  const schemaDir = findSchemaDir(root, schemaDirOption);
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
    sources: listSources(root, schemaDir),
    permissionsSources: permissionsSources(ts, file("permissions.ts")),
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

/**
 * Where schema.ts is: `--schema-dir`, else where validate looks (., src,
 * src/lib), else the one directory under the project holding both schema.ts
 * and jazz.app.json (an Expo app's `jazz/`, say).
 */
/**
 * An app whose source is its JSON files (jazz.app.json, no schema.ts), as an
 * alpha.65 Rust or Go backend's: `upgrade` writes its TS from them.
 */
export class JsonOnlyApp extends UpgradeRefusal {
  constructor(readonly dir: string) {
    super(`${dir} has jazz.app.json but no schema.ts`);
  }
}

function findSchemaDir(root: string, option: string | undefined): string {
  if (option) {
    const dir = resolve(root, option);
    if (!existsSync(join(dir, "schema.ts"))) {
      if (existsSync(join(dir, APP_FILE))) throw new JsonOnlyApp(dir);
      throw new UpgradeRefusal(`--schema-dir ${option}: no schema.ts in ${dir}`);
    }
    return dir;
  }
  const known = SCHEMA_ROOTS.map((r) => join(root, r)).find((dir) =>
    existsSync(join(dir, "schema.ts")),
  );
  if (known) return known;
  const found: string[] = [];
  const walk = (dir: string, depth: number): void => {
    if (depth > 3) return;
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    if (entries.some((e) => e.name === "schema.ts") && entries.some((e) => e.name === APP_FILE))
      found.push(dir);
    for (const entry of entries) {
      if (entry.isDirectory() && !entry.name.startsWith(".") && !SKIPPED_DIRS.has(entry.name)) {
        walk(join(dir, entry.name), depth + 1);
      }
    }
  };
  walk(root, 0);
  if (found.length === 1) return found[0]!;
  if (found.length > 1) {
    throw new UpgradeRefusal(
      `${root} holds several apps (schema.ts beside jazz.app.json in ${found.map((d) => relative(root, d)).join(", ")}): pass --schema-dir for the one to upgrade`,
    );
  }
  const jsonOnly = SCHEMA_ROOTS.map((r) => join(root, r)).find((dir) =>
    existsSync(join(dir, APP_FILE)),
  );
  if (jsonOnly) throw new JsonOnlyApp(jsonOnly);
  throw new UpgradeRefusal(
    `no schema.ts in ${root}, ${join(root, "src")} or ${join(root, "src", "lib")}: run upgrade in the app's directory (or pass --dir, and --schema-dir where schema.ts lives elsewhere)`,
  );
}

function permissionsSources(ts: Ts, permissionsFile: string | undefined): string[] {
  const out: string[] = [];
  for (let file = permissionsFile; file && !out.includes(file); ) {
    out.push(file);
    const source = ts.createSourceFile(
      file,
      readFileSync(file, "utf8"),
      ts.ScriptTarget.Latest,
      true,
    );
    let next: string | undefined;
    for (const statement of source.statements) {
      if (
        ts.isExportDeclaration(statement) &&
        statement.moduleSpecifier &&
        ts.isStringLiteral(statement.moduleSpecifier) &&
        statement.moduleSpecifier.text.startsWith(".") &&
        statement.exportClause &&
        ts.isNamedExports(statement.exportClause) &&
        statement.exportClause.elements.some((e) => (e.propertyName ?? e.name).text === "default")
      ) {
        const base = join(dirname(file), statement.moduleSpecifier.text).replace(
          /\.(js|mjs|ts|mts)$/,
          "",
        );
        next = [`${base}.ts`, `${base}.mts`, `${base}.tsx`, join(base, "index.ts")].find((c) =>
          existsSync(c),
        );
      }
    }
    file = next;
  }
  return out;
}

function listSources(root: string, schemaDir: string): string[] {
  const out: string[] = [];
  const walk = (dir: string, top: boolean): void => {
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    // A nested package is part of this app (a backend, a shared `jazz/`
    // folder) unless it is an app of its own, with its own schema.
    if (!top && entries.some((e) => e.name === "package.json") && !isWithin(schemaDir, dir)) {
      if (
        SCHEMA_ROOTS.some(
          (r) => existsSync(join(dir, r, "schema.ts")) || existsSync(join(dir, r, APP_FILE)),
        )
      )
        return;
    }
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

function isWithin(path: string, dir: string): boolean {
  const rel = relative(dir, path);
  return rel === "" || (!rel.startsWith("..") && !rel.startsWith(sep));
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
