/**
 * `jazz-tools print-ts --dir <app>` (hidden): write an app's alpha.66 TS
 * sources from its `jazz.app.json` and `jazz.permissions.json`, for the
 * examples without TS history. Users run `upgrade` instead: it converts
 * apps that have alpha.65 TS and writes schema.ts for a JSON-only app
 * (checking it keeps the schema hashes and bundle id), which `generate`
 * and `migrations create` refuse until it has one.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import type { AppJson, PermissionsJson } from "./app-json.js";
import { formatTs, layout, loadPrettier } from "./format.js";
import { PrintRefusal, printSchemaTs } from "./index.js";
import { printMigrations, type Snapshot } from "./migrations.js";
import type { TableJson } from "./app-json.js";

const ROOTS = [".", "src", "src/lib"];

export interface PrintTsOptions {
  dir: string;
  /** Overwrite an existing schema.ts / migrations/. */
  force?: boolean;
  /** Keep the permissions in their own permissions.ts. */
  separatePermissions?: boolean;
  /** The engine snapshot of a version (default: `snapshotFromVersion` from the migrations tooling). */
  snapshot?: (tables: Record<string, TableJson>) => Snapshot | Promise<Snapshot>;
}

/** Write the TS sources; returns the files written, relative to `dir`. */
export async function printTs(options: PrintTsOptions): Promise<string[]> {
  const root = resolve(options.dir);
  const appDir = ROOTS.map((r) => join(root, r)).find((d) => existsSync(join(d, "jazz.app.json")));
  if (!appDir) throw new Error(`no jazz.app.json in ${root}, ${root}/src or ${root}/src/lib`);
  const app = JSON.parse(readFileSync(join(appDir, "jazz.app.json"), "utf8")) as AppJson;
  const permissionsFile = join(appDir, "jazz.permissions.json");
  const permissions: PermissionsJson = existsSync(permissionsFile)
    ? JSON.parse(readFileSync(permissionsFile, "utf8"))
    : { format: 1, tables: {} };

  const printed = printSchemaTs(app, permissions, {
    separatePermissions: options.separatePermissions,
  });
  const files: Record<string, string> = { ...printed.files };
  const unsupported = [...printed.unsupported];
  const migrations = await printMigrations(app, {
    snapshot: options.snapshot ?? (await defaultSnapshot()),
  });
  Object.assign(files, migrations.files);
  unsupported.push(...migrations.unsupported);
  if (unsupported.length > 0) throw new PrintRefusal(unsupported);

  const existing = Object.keys(files).filter((f) => existsSync(join(appDir, f)));
  if (existing.length > 0 && !options.force) {
    throw new Error(`${existing.join(", ")} exist already: pass --force to overwrite them`);
  }
  const prettier = await loadPrettier(root);
  const written: string[] = [];
  for (const [name, text] of Object.entries(files)) {
    const path = join(appDir, name);
    mkdirSync(dirname(path), { recursive: true });
    const out = name.endsWith(".ts")
      ? prettier
        ? await formatTs(text, { file: path, prettier })
        : layout(text)
      : text;
    writeFileSync(path, out);
    written.push(relative(root, path));
  }
  return written;
}

async function defaultSnapshot(): Promise<
  (tables: Record<string, TableJson>) => Promise<Snapshot>
> {
  const { snapshotFromVersion } = await import("../dev/migrations.js");
  return (tables) => snapshotFromVersion(tables as never);
}

export async function runPrintTs(args: string[]): Promise<void> {
  const flag = (name: string) => {
    const i = args.indexOf(name);
    return i >= 0 ? args[i + 1] : undefined;
  };
  const written = await printTs({
    dir: resolve(process.cwd(), flag("--dir") ?? "."),
    force: args.includes("--force"),
    separatePermissions: args.includes("--separate-permissions"),
  });
  for (const file of written) console.log(`wrote ${file}`);
}
