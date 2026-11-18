/**
 * The JSON → TS printer: `jazz.app.json` + `jazz.permissions.json` → the
 * alpha.66 sources (`schema.ts` with `s.defineSchema` and `definePermissions`,
 * and `migrations/` when the app has history). Used by `jazz-tools upgrade`,
 * `jazz-tools print-ts` and the parity test.
 */
import type { AppJson, PermissionsJson } from "./app-json.js";
import { printPermissions, type Unsupported } from "./permissions.js";
import { printTables } from "./schema.js";

export type { AppJson, PermissionsJson } from "./app-json.js";
export type { Unsupported } from "./permissions.js";
export { printPermissions } from "./permissions.js";
export { printTables } from "./schema.js";
export { formatTs, layout, loadPrettier } from "./format.js";

export interface PrintOptions {
  /** Comment lines at the top of `schema.ts`. */
  header?: string;
  /** Print the permissions into a separate `permissions.ts`. */
  separatePermissions?: boolean;
}

export interface PrintedApp {
  /** Relative path → contents (unformatted: run `formatTs`). */
  files: Record<string, string>;
  /** What the DSL can't express, by JSON path: callers refuse when non-empty. */
  unsupported: Unsupported[];
}

export class PrintRefusal extends Error {
  constructor(readonly unsupported: Unsupported[]) {
    super(
      `the TS DSL can't express ${unsupported.length === 1 ? "this" : "these"} yet:\n` +
        unsupported.map((u) => `  ${u.path}: ${u.text}\n    ${u.reason}`).join("\n"),
    );
  }
}

export function printSchemaTs(
  app: AppJson,
  permissions: PermissionsJson,
  options: PrintOptions = {},
): PrintedApp {
  const unsupported: Unsupported[] = [];
  const tables = printTables(app.tables, {
    path: "tables",
    unsupported,
    transforms: app.transforms,
  });
  // Transforms are spelt on the columns that use them: one no column uses would be lost.
  const used = new Set(
    JSON.stringify(app.tables)
      .match(/"transform":"[^"]*"/g)
      ?.map((m) => m.slice(13, -1)),
  );
  for (const name of Object.keys(app.transforms ?? {})) {
    if (!used.has(name))
      unsupported.push({
        path: `transforms.${name}`,
        text: name,
        reason: "a transform no column uses",
      });
  }
  const printed = printPermissions(app, permissions);
  unsupported.push(
    ...printed.unsupported.map((u) => ({ ...u, path: `jazz.permissions.json: ${u.path}` })),
  );
  const header = options.header ? `${options.header.trimEnd()}\n` : "";
  const schemaTs = `${header}import { schema as s } from "jazz-tools";\n\nexport const schema = s.defineSchema(${tables});\n`;
  const files: Record<string, string> = {};
  if (options.separatePermissions) {
    files["schema.ts"] = schemaTs;
    files["permissions.ts"] =
      `import type { schema as s } from "jazz-tools";\nimport { schema } from "./schema";\n\n${printed.code}`;
  } else {
    files["schema.ts"] = `${schemaTs}\n${printed.code}`;
  }
  return { files, unsupported };
}
