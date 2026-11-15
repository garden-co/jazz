/**
 * Step 1 of `jazz-tools upgrade`: the jazz-tools the app was written for
 * (≤ alpha.64, with the schema DSL) must confirm that jazz.app.json and
 * jazz.permissions.json are what schema.ts, permissions.ts and migrations/
 * make. upgrade converts from those files, so a stale one would lose
 * whatever it doesn't say.
 */
import { spawnSync } from "node:child_process";
import { existsSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { UpgradeRefusal } from "./errors.js";
import {
  APP_FILE,
  PERMISSIONS_FILE,
  packageVersion,
  SCHEMA_ROOTS,
  runningPackageRoot,
  type Project,
} from "./project.js";

/** The last release with the schema DSL. */
export const LAST_DSL_ALPHA = 64;
const LAST_DSL_VERSION = `2.0.0-alpha.${LAST_DSL_ALPHA}`;

export interface LegacyJazzTools {
  /** The package directory. */
  dir: string;
  version: string;
}

/** The DSL-era alpha number of a jazz-tools version, or null when it has no DSL. */
export function dslAlpha(version: string): number | null {
  const match = /^2\.0\.0-alpha\.(\d+)/.exec(version);
  if (!match) return null;
  const alpha = Number(match[1]);
  return alpha <= LAST_DSL_ALPHA ? alpha : null;
}

/**
 * The project's installed jazz-tools, which must still have the DSL: the old
 * validate bundles schema.ts against it, and the checker reads its types.
 */
export function findLegacyJazzTools(project: Project): LegacyJazzTools {
  const require = createRequire(join(project.schemaDir, "package.json"));
  let manifest: string;
  try {
    manifest = require.resolve("jazz-tools/package.json");
  } catch {
    throw new UpgradeRefusal(
      `jazz-tools isn't installed in ${project.root}: install the jazz-tools the app was written for (≤ ${LAST_DSL_VERSION}) and run upgrade again`,
    );
  }
  const dir = dirname(manifest);
  const version = packageVersion(dir);
  const same = realpathSync(dir) === realpathSync(runningPackageRoot());
  if (dslAlpha(version) === null || same) {
    throw new UpgradeRefusal(
      `upgrade reads schema.ts with the schema DSL of the app's own jazz-tools (≤ ${LAST_DSL_VERSION}), but the app's jazz-tools ` +
        `is ${same ? "this jazz-tools itself" : version}. Install jazz-tools@${LAST_DSL_VERSION} in the app, ` +
        `run \`npx jazz-tools@2.0.0-alpha.${LAST_DSL_ALPHA + 1} upgrade\` there, then update jazz-tools.`,
    );
  }
  return { dir, version };
}

/**
 * The environment for the app's own jazz-tools: everything but what points
 * Jazz at other native or WASM builds. jazz-napi and jazz-tools honour
 * `JAZZ_CORRECTNESS_*` (the monorepo's sealed test artifacts) and napi-rs
 * `NAPI_RS_*` overrides; inherited, they would run alpha.64's validate on
 * this release's engine, which writes different files (no `slices`).
 */
export function legacyEnvironment(env: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const out: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(env)) {
    if (
      key.startsWith("JAZZ_CORRECTNESS_") ||
      key === "NAPI_RS_NATIVE_LIBRARY_PATH" ||
      key === "NAPI_RS_FORCE_WASI"
    )
      continue;
    out[key] = value;
  }
  return out as NodeJS.ProcessEnv;
}

/** What the old validate takes as `--schema-dir`: the root it searches (., src, src/lib), or schema.ts's own directory. */
function validateDir(project: Project): string {
  return SCHEMA_ROOTS.some((r) => join(project.root, r) === project.schemaDir)
    ? project.root
    : project.schemaDir;
}

/** Run the old `validate --check`, refusing what it refuses. */
export function checkLegacyFiles(project: Project, legacy: LegacyJazzTools): string[] {
  for (const file of [project.appFile, project.permissionsJsonFile]) {
    if (!existsSync(file)) {
      throw new UpgradeRefusal(
        `${file.slice(project.root.length + 1)} is missing: run \`npx jazz-tools validate\` with jazz-tools ${legacy.version} and commit ${APP_FILE} and ${PERMISSIONS_FILE} first` +
          (project.permissionsFile
            ? ""
            : " (that validate needs a permissions.ts, even for an app without a backend; a minimal one will do)"),
      );
    }
  }
  const bin = join(legacy.dir, "bin", "jazz-tools.js");
  const result = spawnSync(
    process.execPath,
    [bin, "validate", "--check", "--schema-dir", validateDir(project)],
    {
      cwd: project.root,
      encoding: "utf8",
      env: { ...legacyEnvironment(process.env), NO_COLOR: "1" },
    },
  );
  const output = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim();
  if (result.error) throw new UpgradeRefusal(`couldn't run ${bin}: ${result.error.message}`);
  if (result.status !== 0) {
    throw new UpgradeRefusal(
      `jazz-tools ${legacy.version} \`validate --check\` refused the app files, so upgrade would convert stale ones:\n${indent(output)}\n` +
        `Run \`npx jazz-tools validate\` with jazz-tools ${legacy.version}, commit what it writes, then upgrade again.`,
    );
  }
  if (/changed since the latest snapshot/.test(output)) {
    throw new UpgradeRefusal(
      `schema.ts changed since its latest snapshot in migrations/, so jazz.app.json's history lacks the version a store may hold. ` +
        `Run \`npx jazz-tools migrations create --name <name>\` with jazz-tools ${legacy.version} first, then upgrade again.`,
    );
  }
  // Its warnings are about permissions.ts's DSL, which upgrade deletes.
  return [];
}

function indent(text: string): string {
  return text
    .split("\n")
    .map((line) => `  ${line}`)
    .join("\n");
}
