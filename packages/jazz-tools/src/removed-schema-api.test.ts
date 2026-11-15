import { describe, expect, it } from "vitest";
import * as root from "./index.js";
import { UPGRADE_FROM_ALPHA_64_URL } from "./removed-schema-api.js";

const TOP_LEVEL = [
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

const SCHEMA_MEMBERS = [
  "table",
  "computed",
  "string",
  "uuid",
  "boolean",
  "int",
  "bigint",
  "timestamp",
  "float",
  "bytes",
  "json",
  "enum",
  "array",
  "rel",
  "reverse",
  "defineApp",
  "defineSchema",
  "defineSliceableApp",
  "definePermissions",
  "allowExternalProvenanceName",
] as const;

const MIGRATION_MEMBERS = [
  "add",
  "drop",
  "renameFrom",
  "defineMigration",
  "renameTableFrom",
] as const;

const stubs: [name: string, stub: unknown][] = [
  ...TOP_LEVEL.map((name) => [name, root[name]] as [string, unknown]),
  ...SCHEMA_MEMBERS.map((name) => [`schema.${name}`, root.schema[name]] as [string, unknown]),
  ...MIGRATION_MEMBERS.map(
    (name) => [`migration.${name}`, root.migration[name]] as [string, unknown],
  ),
];

function thrownBy(run: () => unknown): unknown {
  try {
    run();
  } catch (error) {
    return error;
  }
  return undefined;
}

describe("the schema DSL alpha.65 removed", () => {
  it("is still exported from the package root under its old names", () => {
    for (const name of [...TOP_LEVEL, "schema", "migration"] as const) {
      expect(root, name).toHaveProperty(name);
    }
    expect(Object.keys(root.schema).sort()).toEqual([...SCHEMA_MEMBERS].sort());
    expect(Object.keys(root.migration).sort()).toEqual([...MIGRATION_MEMBERS].sort());
  });

  it("stays inert until used: plain named functions in frozen namespaces", () => {
    expect(Object.isFrozen(root.schema)).toBe(true);
    expect(Object.isFrozen(root.migration)).toBe(true);
    for (const [name, stub] of stubs) {
      expect(typeof stub, name).toBe("function");
      expect((stub as () => unknown).name).toBe(name);
    }
  });

  it("throws when called, naming itself and pointing at `jazz-tools upgrade`", () => {
    for (const [name, stub] of stubs) {
      const thrown = thrownBy(() => (stub as (...args: unknown[]) => unknown)({}, {}));
      expect(thrown, name).toBeInstanceOf(Error);
      expect(thrown, name).toMatchObject({ code: "JAZZ_API_REMOVED" });
      const { message } = thrown as Error;
      expect(
        message.startsWith(`[JAZZ_API_REMOVED] ${name} was removed in jazz-tools alpha.65`),
      ).toBe(true);
      expect(message).toContain("`npx jazz-tools upgrade`");
      expect(message).toContain(UPGRADE_FROM_ALPHA_64_URL);
    }
  });

  it("throws when constructed", () => {
    expect(() => new (root.defineApp as unknown as new () => unknown)()).toThrow(
      /\[JAZZ_API_REMOVED\] defineApp was removed in jazz-tools alpha\.65/,
    );
    expect(() => new (root.schema.table as unknown as new () => unknown)()).toThrow(
      /\[JAZZ_API_REMOVED\] schema\.table was removed/,
    );
  });

  it("refuses enum cases with fields by name, at definition time", () => {
    const enumStub = root.schema.enum as unknown as (...args: unknown[]) => unknown;
    expect(() => enumStub({ bug: { severity: {} }, feature: { area: {} } })).toThrow(
      'enum cases with fields are not supported; use s.enum("a", "b") plus a nullable column per field, or s.json() (see https://github.com/garden-co/jazz/issues/5758)',
    );
    expect(thrownBy(() => enumStub("a", "b"))).toMatchObject({ code: "JAZZ_API_REMOVED" });
  });

  it("names the files that replaced it", () => {
    expect(() => (root.definePermissions as unknown as () => unknown)()).toThrow(
      /jazz\.app\.json and jazz\.permissions\.json are now the app's schema, migrations and permissions/,
    );
  });
});
