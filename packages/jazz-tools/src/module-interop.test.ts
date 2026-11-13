import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { schema as s } from "./index.js";
import {
  mergePermissionsIntoWasmSchema,
  type CompiledPermissionsMap,
} from "./schema-permissions.js";
import { resolveSchemaSource, type SchemaSourceInput } from "./schema-source.js";

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

const app = s.defineApp(
  { todos: s.table({ title: s.string() }, {}), notes: s.table({ body: s.string() }, {}) },
  { migrations: "none" },
);
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.todos.allowRead.always();
  policy.notes.allowRead.always();
});
const schema = resolveSchemaSource(app);

const wrapperError = (file: string) =>
  new RegExp(`^${file} arrived as \\{ default: … \\}.*\\{"type": "module"\\}`);

describe("a schema or permissions module imported as CommonJS from ESM", () => {
  it("names the mismatch for a wrapped schema", () => {
    const wrapped = asCommonJsModule(app);
    expect(Object.keys(wrapped)).toEqual(["default"]);
    expect(() => resolveSchemaSource(wrapped as SchemaSourceInput)).toThrow(
      wrapperError("schema.ts"),
    );
  });

  it("names the mismatch for wrapped permissions, not an unknown table `default`", () => {
    const wrapped = asCommonJsModule(permissions);
    expect(() => mergePermissionsIntoWasmSchema(schema, wrapped)).toThrow(
      wrapperError("permissions.ts"),
    );
  });

  it("keeps a real table named `default` working, and still spots its wrapped permissions", () => {
    const withDefault = s.defineApp(
      { default: s.table({ title: s.string() }, {}), todos: s.table({ title: s.string() }, {}) },
      { migrations: "none" },
    );
    const itsPermissions = s.definePermissions(withDefault, ({ policy }) => {
      policy.default.allowRead.always();
      policy.todos.allowRead.always();
    });
    const resolved = resolveSchemaSource(withDefault);
    expect(Object.keys(resolved)).toContain("default");
    const merged = mergePermissionsIntoWasmSchema(resolved, itsPermissions);
    expect(merged.default.policies?.select).toBeDefined();
    // Only a `default` table's policies, as a permissions map: no wrapper.
    expect(() =>
      mergePermissionsIntoWasmSchema(resolved, { default: itsPermissions.default }),
    ).not.toThrow();
    expect(() =>
      mergePermissionsIntoWasmSchema(resolved, asCommonJsModule(itsPermissions)),
    ).toThrow(wrapperError("permissions.ts"));
  });

  it("leaves other unknown tables to the usual error", () => {
    expect(() =>
      mergePermissionsIntoWasmSchema(schema, {
        default: { select: { using: { type: "True" } } },
      } as CompiledPermissionsMap),
    ).toThrow("permissions.ts defines permissions for unknown table(s): default.");
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
