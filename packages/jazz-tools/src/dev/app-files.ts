/**
 * An app's files (SPEC 10.10): `jazz.app.json` (schema, history, migrations,
 * computed fields and transforms), `jazz.permissions.json` (backends only),
 * and the typed bindings `jazz-tools generate` generates from the first,
 * `jazz.generated.ts`, whose `source` is the hash of the file it was
 * generated from. The files are the app's only source. Node only.
 */
import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join, relative } from "node:path";
import { schemaSourceHash } from "./codegen-napi.js";

export const APP_FILE = "jazz.app.json";
export const PERMISSIONS_FILE = "jazz.permissions.json";
export const GENERATED_FILE = "jazz.generated.ts";
export const COMPUTED_FILE = "jazz.computed.ts";

/** Where the docs explain the files and their expression grammar. */
export const APP_FILES_DOCS = "https://jazz.tools/docs/reference/app-files";

/** The code jazz-napi gives a refusal that only says the format can't express something yet. */
export const APP_FILES_UNSUPPORTED = "JAZZ_APP_FILES_UNSUPPORTED";

/** Where a project keeps its files, as the CLI looks for them: the root, ./src, ./src/lib. */
export const APP_ROOTS = [".", "src", join("src", "lib")];

/** The directory holding `jazz.app.json` in the project at `cwd`, if any. */
export function findAppDir(cwd: string): string | null {
  for (const root of APP_ROOTS) {
    const dir = join(cwd, root);
    if (existsSync(join(dir, APP_FILE))) return dir;
  }
  return null;
}

/** An app's files as they are on disk; the permissions and bindings may be missing. */
export interface AppProject {
  dir: string;
  app: string;
  permissions: string | null;
  bindings: string | null;
}

/** Read the files of the project at `cwd`, or refuse naming what is missing. */
export async function readAppProject(cwd: string): Promise<AppProject> {
  const dir = findAppDir(cwd);
  if (!dir) {
    const legacy = APP_ROOTS.map((root) => join(cwd, root, "schema.ts")).find(existsSync);
    throw new Error(
      legacy
        ? `${relative(cwd, legacy) || legacy} is an alpha.64 schema: jazz.app.json is now the app's schema. Run \`npx jazz-tools upgrade\` to convert the app.`
        : `No ${APP_FILE} in ${cwd} (looked in ${APP_ROOTS.join(", ")}). See ${APP_FILES_DOCS}.`,
    );
  }
  const optional = (file: string) => readFile(join(dir, file), "utf8").catch(() => null);
  return {
    dir,
    app: await readFile(join(dir, APP_FILE), "utf8"),
    permissions: await optional(PERMISSIONS_FILE),
    bindings: await optional(GENERATED_FILE),
  };
}

/**
 * `createJazz`'s check of the bindings it runs with against the project's
 * `jazz.app.json`: refused when the file changed since the bindings were
 * generated. A process with no `jazz.app.json` in reach (a deployed bundle,
 * which ships only the bindings) has nothing to compare and starts.
 */
export async function checkRunningBindings(
  source: string,
  cwd: string = process.cwd(),
): Promise<void> {
  const dir = findAppDir(cwd);
  if (!dir) return;
  const app = await readFile(join(dir, APP_FILE), "utf8");
  if ((await schemaSourceHash(app)) === source) return;
  const shown = relative(cwd, join(dir, GENERATED_FILE)) || GENERATED_FILE;
  throw new Error(
    `bindings out of date: ${shown} was generated from another ${APP_FILE}. Run \`npx jazz-tools generate\` and commit the ${GENERATED_FILE} it writes.`,
  );
}
