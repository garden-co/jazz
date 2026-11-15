/**
 * Step 3 of `jazz-tools upgrade`: jazz.app.json must already carry every
 * schema version and migration that migrations/ holds (the .64 writer puts
 * them there), since upgrade deletes migrations/.
 */
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";
import { entriesOf, isObject, type JsonObject } from "./app-json.js";
import { UpgradeRefusal } from "./errors.js";

export interface MigrationsCheck {
  versions: number;
  migrations: number;
  /** Migration files between versions the app never shipped (docs fragments, drafts): deleted with migrations/. */
  unshipped: string[];
}

/** Short hashes (12 hex) of migrations/snapshots, by either .64 naming. */
function snapshotHashes(migrationsDir: string): string[] {
  const dir = join(migrationsDir, "snapshots");
  if (!existsSync(dir)) return [];
  const out = new Set<string>();
  for (const name of readdirSync(dir)) {
    const match = /^(?:\d{8}T\d{6}-)?([0-9a-f]{12})\.json$/i.exec(name);
    if (match) out.add(match[1]!.toLowerCase());
  }
  return [...out];
}

/** `[from, to]` short hashes of each migration file in migrations/. */
function migrationFiles(migrationsDir: string): Array<{ file: string; from: string; to: string }> {
  const out = [];
  for (const name of readdirSync(migrationsDir)) {
    if (name === "index.ts" || !/\.(ts|js|mts|mjs)$/.test(name)) continue;
    const text = readFileSync(join(migrationsDir, name), "utf8");
    const from = /fromHash:\s*["']([0-9a-f]{12,64})["']/.exec(text)?.[1];
    const to = /toHash:\s*["']([0-9a-f]{12,64})["']/.exec(text)?.[1];
    const named = /([0-9a-f]{12})-([0-9a-f]{12})\.[mc]?[tj]s$/.exec(name);
    const pair = from && to ? [from, to] : named ? [named[1]!, named[2]!] : undefined;
    if (!pair) {
      throw new UpgradeRefusal(
        `can't tell which versions ${name} migrates between (no fromHash/toHash): check that jazz.app.json's migrations include it, then remove it from migrations/ and upgrade again`,
        join(migrationsDir, name),
      );
    }
    out.push({ file: name, from: pair[0]!.slice(0, 12), to: pair[1]!.slice(0, 12) });
  }
  return out;
}

/** The short hashes `migrations/index.ts` ships (its `"hash"` entries). */
export function shippedHashes(migrationsDir: string): string[] {
  const index = join(migrationsDir, "index.ts");
  if (!existsSync(index)) return [];
  const text = readFileSync(index, "utf8");
  return [...text.matchAll(/"hash":\s*"([0-9a-f]{64})"/g)].map((m) => m[1]!.slice(0, 12));
}

export function checkMigrations(
  app: JsonObject,
  migrationsDir: string | undefined,
): MigrationsCheck {
  const history = isObject(app.history) ? entriesOf(app.history).map(([name]) => name) : [];
  const entries = Array.isArray(app.migrations) ? app.migrations : [];
  if (!migrationsDir)
    return { versions: history.length + 1, migrations: entries.length, unshipped: [] };
  const versions = new Set([...snapshotHashes(migrationsDir), ...shippedHashes(migrationsDir)]);
  const missing: string[] = [];
  // Every version but the current one is a history entry; the current one has no snapshot name in the file.
  const unknown = [...versions].filter((hash) => !history.includes(hash));
  if (unknown.length > 1) {
    missing.push(
      ...unknown.map(
        (hash) => `schema version ${hash} (in migrations/) is not in jazz.app.json's history`,
      ),
    );
  }
  const fileMigrations = entries.filter(isObject).map((m) => ({
    from: typeof m.from === "string" ? m.from : undefined,
    to: typeof m.to === "string" ? m.to : undefined,
  }));
  const unshipped: string[] = [];
  for (const migration of migrationFiles(migrationsDir)) {
    // The app only ships migrations between versions it ships.
    if (!versions.has(migration.from) || !versions.has(migration.to)) {
      unshipped.push(migration.file);
      continue;
    }
    const found = fileMigrations.some(
      (m) =>
        m.from === migration.from &&
        (m.to === migration.to || (m.to === undefined && !history.includes(migration.to))),
    );
    if (!found) {
      missing.push(
        `${migration.file} (${migration.from} → ${migration.to}) is not in jazz.app.json's migrations`,
      );
    }
  }
  if (missing.length > 0) {
    throw new UpgradeRefusal(
      `jazz.app.json doesn't carry everything in migrations/, which upgrade deletes:\n${missing.map((m) => `  - ${m}`).join("\n")}\n` +
        "Run `npx jazz-tools validate` with the app's jazz-tools ≤ alpha.64 so it writes them into jazz.app.json, then upgrade again.",
    );
  }
  return { versions: history.length + 1, migrations: entries.length, unshipped };
}

/**
 * Snapshot files of this app's versions outside its migrations/: alpha.64's
 * `migrations create --schema-dir <elsewhere>` writes them under the
 * directory it ran in. They go with migrations/; others stay.
 */
export function straySnapshots(
  root: string,
  migrationsDir: string | undefined,
  versions: Set<string>,
): string[] {
  const out: string[] = [];
  const skip = new Set(["node_modules", ".git", "dist", "build", ".next", ".expo"]);
  const walk = (dir: string, depth: number): void => {
    if (depth > 4) return;
    let entries;
    try {
      entries = readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (!entry.isDirectory() || skip.has(entry.name)) continue;
      const path = join(dir, entry.name);
      if (entry.name === "snapshots" && basename(dir) === "migrations" && dir !== migrationsDir) {
        for (const name of readdirSync(path)) {
          const match = /^(?:\d{8}T\d{6}-)?([0-9a-f]{12})\.json$/i.exec(name);
          if (match && versions.has(match[1]!.toLowerCase())) out.push(join(path, name));
        }
      } else {
        walk(path, depth + 1);
      }
    }
  };
  walk(root, 0);
  return out;
}
