/**
 * Byte-exact undo for what `upgrade` (and the tools it runs) write: the
 * named files and directories are recorded before, and `restore` puts back
 * every byte, removes files that weren't there and directories it created.
 * Nothing else in the app is touched, so its mtimes stay.
 */
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";

export interface Snapshot {
  restore(): void;
}

/**
 * Record `paths`: files, and directories with everything under them (a path
 * that doesn't exist yet is recorded as absent, so `restore` removes it).
 */
export function snapshot(paths: string[]): Snapshot {
  const files = new Map<string, Buffer | null>();
  const dirs = new Set<string>();
  /** Ancestors of absent paths that didn't exist either: `restore` removes them if empty. */
  const missingParents: string[] = [];
  const record = (path: string) => {
    if (!existsSync(path)) {
      files.set(path, null);
      for (let parent = dirname(path); !existsSync(parent); parent = dirname(parent))
        missingParents.push(parent);
      return;
    }
    if (statSync(path).isDirectory()) {
      dirs.add(path);
      for (const entry of readdirSync(path)) record(join(path, entry));
    } else {
      files.set(path, readFileSync(path));
    }
  };
  const roots = [...new Set(paths)];
  for (const path of roots) record(path);
  return {
    restore() {
      // What appeared under a recorded directory (or at a path that was absent) goes.
      const remove = (path: string) => {
        if (!existsSync(path)) return;
        if (statSync(path).isDirectory()) {
          for (const entry of readdirSync(path)) remove(join(path, entry));
          if (!dirs.has(path)) rmdirSync(path);
        } else if (!files.has(path) || files.get(path) === null) {
          rmSync(path, { force: true });
        }
      };
      for (const path of roots) remove(path);
      for (const [path, bytes] of files) {
        if (bytes === null) continue;
        mkdirSync(dirname(path), { recursive: true });
        writeFileSync(path, bytes);
      }
      for (const dir of missingParents) {
        if (existsSync(dir) && readdirSync(dir).length === 0) rmdirSync(dir);
      }
    },
  };
}

/** Where `generate` writes each language's bindings, relative to the app (jazz-codegen's `FILE`s). */
function bindingsFiles(): string[] {
  return [
    "jazz.generated.ts",
    join("src", "jazz_app.rs"),
    join("jazzapp", "jazz_app.go"),
    join("src", "main", "kotlin", "JazzApp.kt"),
    "JazzApp.swift",
  ];
}

/** What `upgrade` and `generate` may write in an app's directory. */
export function appOutputs(dir: string, extra: string[] = []): string[] {
  return [
    join(dir, "jazz.app.json"),
    join(dir, "jazz.permissions.json"),
    join(dir, "schema.ts"),
    join(dir, "permissions.ts"),
    join(dir, "migrations"),
    ...bindingsFiles().map((f) => join(dir, f)),
    ...extra,
  ];
}
