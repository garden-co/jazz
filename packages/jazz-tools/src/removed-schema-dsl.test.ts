import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import * as root from "./index.js";

// The alpha.64 schema/permissions/migrations DSL was removed in alpha.65 and
// kept as throwing stubs until beta.0. Since beta.0 the names are not
// exported at all; `npx jazz-tools upgrade` still converts alpha.64/65 apps.

const ROOT_NAMES = [
  "defineApp",
  "defineSchema",
  "defineSliceableApp",
  "definePermissions",
  "defineMigration",
  "renameTableFrom",
  "schemaToWasm",
  "table",
  "anyOf",
  "allOf",
] as const;

const SCHEMA_NAMESPACE_NAMES = ["defineApp", "defineSliceableApp", "definePermissions"] as const;

const readSource = (relative: string) =>
  readFileSync(fileURLToPath(new URL(relative, import.meta.url)), "utf8");

describe("the schema DSL alpha.65 removed", () => {
  it("is not exported from the package root", () => {
    for (const name of ROOT_NAMES) expect(root, name).not.toHaveProperty(name);
  });

  it("is not a member of the `schema` authoring namespace", () => {
    for (const name of SCHEMA_NAMESPACE_NAMES) expect(root.schema, name).not.toHaveProperty(name);
  });

  it("keeps the replacements: `s.defineSchema`, `s.table` and `m.defineMigration`", () => {
    expect(typeof root.schema.defineSchema).toBe("function");
    expect(typeof root.schema.table).toBe("function");
    expect(typeof root.migration.defineMigration).toBe("function");
  });

  it("no longer publishes the `jazz-tools/permissions` subpath", () => {
    const packageJson = JSON.parse(readSource("../package.json")) as {
      exports: Record<string, unknown>;
    };
    expect(Object.keys(packageJson.exports)).not.toContain("./permissions");
  });

  it("is not re-exported from the React Native entry", () => {
    // Read as source: importing the RN entry needs the native runtime.
    const source = readSource("./react-native/index.ts");
    expect(source).not.toContain("removed-schema-api");
    expect(source).not.toMatch(/export\s*\{\s*schema\s*,\s*migration\s*\}/);
  });
});
