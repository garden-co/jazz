import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "./schema-permissions.js";
import { resolveSchemaSource, type SchemaSourceInput } from "./schema-source.js";
import { testApp, testPermissions } from "./testing/index.js";

const dir = mkdtempSync(join(tmpdir(), "jazz-cjs-interop-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
const require = createRequire(join(dir, "index.js"));

/**
 * `value` as an ES module's default import of a compiled CommonJS module
 * (`export default value` in a package without "type": "module") gets it:
 * the whole `module.exports`.
 */
let modules = 0;
function asCommonJsModule<T>(value: T): T {
  const key = Symbol.for(`jazz-tools.test.module-interop.${modules++}`);
  (globalThis as Record<symbol, unknown>)[key] = value;
  const file = join(dir, `module-${modules}.js`);
  writeFileSync(
    file,
    `"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = globalThis[Symbol.for(${JSON.stringify(key.description)})];
`,
  );
  return require(file) as T;
}

const app = testApp({
  tables: {
    todos: { columns: { title: "string" } },
    notes: { columns: { body: "string" } },
  },
});
const schema = resolveSchemaSource(app);

const wrapperError = (file: string) =>
  new RegExp(`^${file} arrived as \\{ default: … \\}.*\\{"type": "module"\\}`);

describe("an app module imported as CommonJS from ESM", () => {
  it("names the mismatch for a wrapped schema", () => {
    const wrapped = asCommonJsModule(app);
    expect(Object.keys(wrapped)).toEqual(["default"]);
    expect(() => resolveSchemaSource(wrapped as SchemaSourceInput)).toThrow(
      wrapperError("jazz.generated.ts"),
    );
  });

  it("keeps a real table named `default` working", () => {
    const withDefault = testApp({
      tables: {
        default: { columns: { title: "string" } },
        todos: { columns: { title: "string" } },
      },
    });
    const itsPermissions = testPermissions(withDefault, {
      format: 1,
      tables: { default: { read: "true" }, todos: { read: "true" } },
    });
    const resolved = resolveSchemaSource(withDefault);
    expect(Object.keys(resolved)).toContain("default");
    const merged = mergePermissionsIntoWasmSchema(resolved, itsPermissions);
    expect(merged.default.policies?.select).toBeDefined();
    // Only a `default` table's policies, as a permissions map.
    expect(() =>
      mergePermissionsIntoWasmSchema(resolved, { default: itsPermissions.default }),
    ).not.toThrow();
  });

  it("leaves other unknown tables to the usual error", () => {
    expect(() =>
      mergePermissionsIntoWasmSchema(schema, {
        default: { select: { using: { type: "True" } } },
      } as CompiledPermissionsMap),
    ).toThrow("jazz.permissions.json defines permissions for unknown table(s): default.");
    expect(() =>
      mergePermissionsIntoWasmSchema(schema, {
        default: { todos: {}, archive: {} },
      } as unknown as CompiledPermissionsMap),
    ).toThrow("unknown table(s): default.");
    expect(() => resolveSchemaSource({ default: { nope: 1 } } as never)).toThrow(
      /^Invalid schema source/,
    );
  });
});
