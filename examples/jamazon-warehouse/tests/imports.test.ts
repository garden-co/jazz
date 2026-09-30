import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const root = join(import.meta.dirname, "..");

/** Everything Next bundles: the routes, the components and the modules they import. */
const BUNDLED = ["app", "components", "src", "schema.ts", "permissions.ts"];

function sources(path: string): string[] {
  if (statSync(path).isFile()) return /\.tsx?$/.test(path) ? [path] : [];
  return readdirSync(path).flatMap((entry) => sources(join(path, entry)));
}

describe("bundled imports", () => {
  // Turbopack, which `next dev` and `next build` use, does not map a ".js"
  // specifier to its ".ts" source, so one such import breaks every page.
  it("uses extensionless relative specifiers", () => {
    const offenders = BUNDLED.flatMap((entry) => sources(join(root, entry))).flatMap((file) =>
      [...readFileSync(file, "utf8").matchAll(/from\s+["'](\.{1,2}\/[^"']+\.js)["']/g)].map(
        ([, specifier]) => `${relative(root, file)}: ${specifier}`,
      ),
    );
    expect(offenders).toEqual([]);
  });
});
