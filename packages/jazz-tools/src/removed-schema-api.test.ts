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

// alpha.66 brought the `schema` and `migration` namespaces back as the TS
// authoring DSL; only the top-level names stay removed.
const stubs: [name: string, stub: unknown][] = TOP_LEVEL.map(
  (name) => [name, root[name]] as [string, unknown],
);

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
    for (const name of TOP_LEVEL) expect(root, name).toHaveProperty(name);
  });

  it("stays inert until used: plain named functions", () => {
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
  });

  it("refuses enum cases with fields by name, at definition time", () => {
    const enumColumn = root.schema.enum as unknown as (...args: unknown[]) => unknown;
    expect(() => enumColumn({ bug: { severity: {} }, feature: { area: {} } })).toThrow(
      'enum cases with fields are not supported; use s.enum("a", "b") plus a nullable column per field, or s.json() (see https://github.com/garden-co/jazz/issues/5758)',
    );
    expect(() => enumColumn("a", "b")).not.toThrow();
  });

  it("names the files that replaced it", () => {
    expect(() => (root.definePermissions as unknown as () => unknown)()).toThrow(
      /jazz\.app\.json and jazz\.permissions\.json are now the app's schema, migrations and permissions/,
    );
  });
});
