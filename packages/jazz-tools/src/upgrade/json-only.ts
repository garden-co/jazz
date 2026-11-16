/**
 * `jazz-tools upgrade` for an app whose source is its JSON files (an alpha.65
 * Rust or Go backend, or any app without schema.ts): the files are the
 * source, so there is nothing to check them against.
 *
 * 1. `defs` in jazz.permissions.json are expanded where they are called.
 * 2. schema.ts is printed from the files (tables, then the permissions, their
 *    repeated checks as helpers), and migrations/ from their `history` and
 *    `migrations`: a snapshot per version and a migration per step.
 * 3. `generate` runs (forced: the old files carry no `$generated` marker).
 * 4. The schema hashes and the bundle id must be what the files gave:
 *    otherwise every file is put back and upgrade refuses.
 *
 * It needs neither the app's jazz-tools nor TypeScript.
 */
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import type { AppJson, PermissionsJson } from "../printer/app-json.js";
import { expandDefs } from "../printer/defs.js";
import { formatTs, layout, loadPrettier } from "../printer/format.js";
import { printSchemaTs, type Unsupported } from "../printer/index.js";
import { printMigrations } from "../printer/migrations.js";
import { UpgradeRefusal } from "./errors.js";
import { appOutputs, snapshot } from "./snapshot.js";
import { APP_FILE, PERMISSIONS_FILE } from "./project.js";

export interface JsonOnlyOptions {
  dryRun?: boolean;
  generate: (dir: string) => void | Promise<void>;
  identity: (
    app: string,
    permissions: string,
  ) => Promise<{ bundle: string; current: string; schemas: string[] }>;
  differences: (
    before: { bundle: string; current: string; schemas: string[] },
    after: { bundle: string; current: string; schemas: string[] },
  ) => string[];
}

export interface JsonOnlyReport {
  written: string[];
  notes: string[];
}

export async function upgradeJsonOnly(
  dir: string,
  root: string,
  options: JsonOnlyOptions,
): Promise<JsonOnlyReport> {
  const rel = (path: string) => relative(root, path) || path;
  const appFile = join(dir, APP_FILE);
  const permissionsFile = join(dir, PERMISSIONS_FILE);
  const appText = readFileSync(appFile, "utf8");
  const app = JSON.parse(appText) as AppJson;
  const written = existsSync(permissionsFile)
    ? (JSON.parse(readFileSync(permissionsFile, "utf8")) as PermissionsJson)
    : ({ format: 1, tables: {} } as PermissionsJson);
  const notes: string[] = [];
  const permissions = written.defs ? expandDefs(written) : written;
  if (written.defs) {
    notes.push(
      `jazz.permissions.json's \`defs\` (${Object.keys(written.defs).join(", ")}) were expanded where they are called; repeated checks became helpers again.`,
    );
  }
  const before = await options.identity(appText, JSON.stringify(permissions));

  // The TS sources.
  const printed = printSchemaTs(app, permissions);
  const unsupported: Unsupported[] = [...printed.unsupported];
  const files: Record<string, string> = { ...printed.files };
  if (app.history || app.migrations?.length) {
    const { snapshotFromVersion } = await import("../dev/migrations.js");
    const migrations = await printMigrations(app, {
      snapshot: (tables) => snapshotFromVersion(tables as never),
    });
    unsupported.push(...migrations.unsupported);
    Object.assign(files, migrations.files);
  }
  if (unsupported.length > 0) {
    throw new UpgradeRefusal(
      `the TS DSL can't express these parts of the app's files yet:\n` +
        unsupported.map((u) => `  ${u.path}: ${u.text}\n    ${u.reason}`).join("\n"),
    );
  }
  const existing = Object.keys(files).filter((f) => existsSync(join(dir, f)));
  if (existing.length > 0) {
    throw new UpgradeRefusal(
      `${existing.map((f) => rel(join(dir, f))).join(", ")} exist${existing.length === 1 ? "s" : ""} already; upgrade writes ${existing.length === 1 ? "it" : "them"} from jazz.app.json: move ${existing.length === 1 ? "it" : "them"} away and run upgrade again`,
    );
  }
  const prettier = await loadPrettier(root);
  const writes: Array<[string, string]> = [];
  for (const [name, text] of Object.entries(files)) {
    const path = join(dir, name);
    writes.push([
      path,
      name.endsWith(".ts")
        ? prettier
          ? await formatTs(text, { file: path, prettier })
          : layout(text)
        : text,
    ]);
  }
  notes.push(
    "schema.ts was printed from jazz.app.json and jazz.permissions.json: repeated checks became helpers (rename them), and migrations/ holds the app's history.",
  );
  const report: JsonOnlyReport = { written: writes.map(([p]) => rel(p)), notes };
  if (options.dryRun) return report;

  // Write; `generate`; the same deployment, or everything back.
  // Byte-exact undo: the app files, the bindings, migrations/ (removed if upgrade made it);
  // anything else `generate` adds goes too.
  const undo = snapshot(
    appOutputs(
      dir,
      writes.map(([p]) => p),
    ),
  );
  const filesBefore = new Set(listFiles(dir));
  for (const [path, text] of writes) {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, text);
  }
  try {
    await options.generate(dir);
    const after = await options.identity(
      readFileSync(appFile, "utf8"),
      readFileSync(permissionsFile, "utf8"),
    );
    const differences = options.differences(before, after);
    if (differences.length > 0) {
      throw new UpgradeRefusal(
        `the TS printed from the app's files would deploy differently, so upgrade put every file back:\n${differences.map((d) => `  ${d}`).join("\n")}\n` +
          "Please report this with the app's jazz.app.json and jazz.permissions.json.",
      );
    }
  } catch (error) {
    for (const path of listFiles(dir)) if (!filesBefore.has(path)) rmSync(path, { force: true });
    undo.restore();
    if (error instanceof UpgradeRefusal) throw error;
    throw new UpgradeRefusal(
      `\`generate\` failed on the printed TS, so upgrade put every file back:\n  ${(error instanceof Error ? error.message : String(error)).replace(/\n/g, "\n  ")}`,
    );
  }
  report.written.push(rel(appFile), rel(permissionsFile));
  report.notes.push(
    `The schema hashes and the bundle id (${before.bundle.slice(0, 12)}) are unchanged.`,
  );
  return report;
}

function listFiles(dir: string): string[] {
  const out: string[] = [];
  const walk = (current: string) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      if (entry.name === "node_modules" || entry.name === ".git" || entry.name === "target")
        continue;
      const path = join(current, entry.name);
      if (entry.isDirectory()) walk(path);
      else out.push(path);
    }
  };
  walk(dir);
  return out;
}
