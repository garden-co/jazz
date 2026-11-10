import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

// A sidebar entry is either a page link or a folder expander, never both. A folder
// with its own index page renders as both, so only the docs root may have one.
const root = join(__dirname, "../content/docs");

function folders(dir: string): string[] {
  return readdirSync(dir)
    .map((name) => join(dir, name))
    .filter((path) => statSync(path).isDirectory())
    .flatMap((path) => [path, ...folders(path)]);
}

describe("docs sidebar", () => {
  it("has no folder that is also a page", () => {
    const offenders = folders(root).filter((dir) => {
      const files = readdirSync(dir);
      const hasIndex = files.some((name) => /^index\.mdx?$/.test(name));
      const meta = files.includes("meta.json")
        ? (JSON.parse(readFileSync(join(dir, "meta.json"), "utf8")) as { pages?: string[] })
        : {};
      return hasIndex || (meta.pages ?? []).includes("index");
    });
    expect(offenders.map((dir) => relative(root, dir))).toEqual([]);
  });
});
