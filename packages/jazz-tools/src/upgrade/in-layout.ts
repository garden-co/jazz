/**
 * `jazz-tools upgrade` for an app already on alpha.66's layout (schema.ts
 * with `s.defineSchema`): the later releases' renames, applied in place.
 *
 * alpha.69 (device accounts, see device-accounts.ts): `authMode`
 * `"local-first"` → `"device-account"` where it's compared with
 * `session.authMode`, the `AccountAuthError` codes, and the deprecated
 * `getLocalFirstIdentityProof` / `LocalFirstGuest` / `--allow-local-first-auth`.
 *
 * Then `generate` writes the JSON files and bindings, which must deploy as
 * the old files would with only the rename applied (the same schema hashes,
 * and the bundle id of the renamed permissions): otherwise every file is put
 * back and upgrade refuses.
 */
import { existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import {
  flagMentions,
  rewriteFile,
  rewritePermissionsText,
  rewriteScript,
} from "./device-accounts.js";
import { UpgradeRefusal } from "./errors.js";
import type { Project } from "./project.js";
import { appOutputs, snapshot } from "./snapshot.js";

type Identity = { bundle: string; current: string; schemas: string[] };

export interface InLayoutOptions {
  dryRun?: boolean;
  generate: (dir: string) => void | Promise<void>;
  identity: (app: string, permissions: string) => Promise<Identity>;
  differences: (before: Identity, after: Identity) => string[];
}

export interface InLayoutReport {
  written: string[];
  rewrites: string[];
  manual: string[];
  notes: string[];
}

const SKIPPED = new Set([
  "node_modules",
  ".git",
  "dist",
  "build",
  ".next",
  ".svelte-kit",
  "target",
  ".turbo",
]);

export async function upgradeInLayout(
  project: Project,
  options: InLayoutOptions,
): Promise<InLayoutReport> {
  const rel = (path: string) => relative(project.root, path) || path;
  const writes: Array<[string, string]> = [];
  const rewrites: string[] = [];
  const manual: string[] = [];

  for (const file of project.sources) {
    const text = readFileSync(file, "utf8");
    const out = rewriteFile(project.ts, file, text);
    if (out.text === text) continue;
    writes.push([file, out.text]);
    rewrites.push(...out.changes.map((c) => `${rel(file)}:${c.line}: ${c.what}`));
  }

  // package.json scripts; the flag elsewhere (shell scripts, CI) is reported, not edited.
  const manifest = join(project.root, "package.json");
  if (existsSync(manifest)) {
    const text = readFileSync(manifest, "utf8");
    let next = text;
    try {
      const scripts = (JSON.parse(text) as { scripts?: Record<string, string> }).scripts ?? {};
      for (const [name, script] of Object.entries(scripts)) {
        const renamed = rewriteScript(script);
        if (renamed === script) continue;
        next = next.replace(JSON.stringify(script), JSON.stringify(renamed));
        rewrites.push(
          `package.json: script "${name}": \`--allow-local-first-auth\` → \`--allow-device-accounts\``,
        );
      }
    } catch {
      // Not JSON: nothing of ours to edit.
    }
    if (next !== text) writes.push([manifest, next]);
  }
  const walk = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (SKIPPED.has(entry.name)) continue;
      const path = join(dir, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (/\.(sh|bash|zsh|ya?ml|toml)$|^(Dockerfile|Makefile|Procfile)$/.test(entry.name)) {
        for (const line of flagMentions(readFileSync(path, "utf8"))) {
          manual.push(
            `${rel(path)}:${line}: \`--allow-local-first-auth\` is deprecated: use \`--allow-device-accounts\``,
          );
        }
      }
    }
  };
  walk(project.root);

  // The deployment this should keep: the old files with only the rename applied.
  const appText = readFileSync(project.appFile, "utf8");
  const permissionsText = readFileSync(project.permissionsJsonFile, "utf8");
  const renamed = rewritePermissionsText(permissionsText);
  const notes: string[] = [];
  if (writes.length === 0 && renamed.count === 0) {
    notes.push(
      "This app is on alpha.66's layout and uses none of the names later releases renamed: nothing to change.",
    );
    return { written: [], rewrites, manual, notes };
  }
  if (renamed.count > 0) {
    notes.push(
      `${renamed.count} polic${renamed.count === 1 ? "y compares" : "ies compare"} \`session.authMode\` with "local-first": it is "device-account" from alpha.69, so the bundle id changes with it (the schema hashes don't). Redeploy the backend.`,
    );
  }
  const before = await options.identity(appText, renamed.text);
  const report: InLayoutReport = { written: writes.map(([p]) => rel(p)), rewrites, manual, notes };
  if (options.dryRun) return report;

  const undo = snapshot(
    appOutputs(
      project.schemaDir,
      writes.map(([p]) => p),
    ),
  );
  const files = (dir: string) => {
    const out: string[] = [];
    const go = (d: string) => {
      for (const e of readdirSync(d, { withFileTypes: true })) {
        if (e.name === "node_modules" || e.name === ".git") continue;
        const p = join(d, e.name);
        if (e.isDirectory()) go(p);
        else out.push(p);
      }
    };
    go(dir);
    return out;
  };
  const filesBefore = new Set(files(project.schemaDir));
  for (const [path, text] of writes) writeFileSync(path, text);
  try {
    await options.generate(project.schemaDir);
    const after = await options.identity(
      readFileSync(project.appFile, "utf8"),
      readFileSync(project.permissionsJsonFile, "utf8"),
    );
    const differences = options.differences(before, after);
    if (differences.length > 0) {
      throw new UpgradeRefusal(
        `the renamed app would deploy differently than its files with only the rename applied, so upgrade put every file back:\n${differences.map((d) => `  ${d}`).join("\n")}\n` +
          'A comparison of `session.authMode` upgrade didn\'t recognise is the likely cause: look for "local-first" next to `authMode` in schema.ts.',
      );
    }
  } catch (error) {
    for (const path of files(project.schemaDir))
      if (!filesBefore.has(path)) rmSync(path, { force: true });
    undo.restore();
    if (error instanceof UpgradeRefusal) throw error;
    throw new UpgradeRefusal(
      `\`generate\` failed on the renamed app, so upgrade put every file back:\n  ${(error instanceof Error ? error.message : String(error)).replace(/\n/g, "\n  ")}`,
    );
  }
  report.written.push(rel(project.appFile), rel(project.permissionsJsonFile));
  report.notes.push(
    `The schema hashes are unchanged, and the permissions deploy as their old text with only the rename applied (bundle ${before.bundle.slice(0, 12)}).`,
  );
  return report;
}
