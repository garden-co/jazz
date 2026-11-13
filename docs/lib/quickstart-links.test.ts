import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";

// The agent quickstart (/quickstart.md, printed by `npm create jazz`) points
// agents at docs pages by path. A moved or missing page leaves an agent with a
// dead link (garden-co/jazz#4773), so every `/docs/...` link must name a page,
// and every `#anchor` a heading on it.
const docs = join(__dirname, "../content");
const brief = readFileSync(join(__dirname, "../public/quickstart.md"), "utf8");

/** Heading ids as the docs site makes them: lower case, punctuation dropped, spaces as `-`. */
function slug(heading: string): string {
  return heading
    .replace(/`/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9 _-]/g, "")
    .trim()
    .replace(/ /g, "-");
}

/**
 * The headings a page shows: its own, and those of the Markdown partials it
 * pulls in with `<include>` (paths relative to the including file).
 */
function headings(file: string, seen = new Set<string>()): Set<string> {
  const ids = new Set<string>();
  if (seen.has(file)) return ids;
  seen.add(file);
  // Lines in code fences are code, not headings.
  const page = readFileSync(file, "utf8").replace(/^```[\s\S]*?^```/gm, "");
  for (const match of page.matchAll(/^#{1,6}\s+(.+?)\s*$/gm)) ids.add(slug(match[1]!));
  for (const match of page.matchAll(/<include>\s*([^<#\s]+\.mdx?)\s*<\/include>/g)) {
    const partial = join(dirname(file), match[1]!);
    if (existsSync(partial)) for (const id of headings(partial, seen)) ids.add(id);
  }
  return ids;
}

const links = [...new Set(brief.match(/\/docs\/[a-z0-9/_-]+(?:#[a-z0-9_-]+)?/g) ?? [])];

describe("the agent quickstart", () => {
  it("links to docs pages", () => {
    expect(links.length).toBeGreaterThan(10);
  });

  it.each(links)("%s resolves to a docs page and heading", (link) => {
    const [path, anchor] = link.split("#");
    const file = join(docs, `${path}.mdx`);
    expect(existsSync(file), `${path}.mdx exists`).toBe(true);
    if (anchor) expect([...headings(file)]).toContain(anchor);
  });
});
