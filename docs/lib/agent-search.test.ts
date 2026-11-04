import { describe, expect, it } from "vitest";
import {
  groupByPage,
  markdownPath,
  parseLimit,
  renderMarkdown,
  wantsMarkdown,
  type SearchHit,
} from "./agent-search";

const hits: SearchHit[] = [
  {
    id: "/docs/schemas/policies",
    type: "page",
    content: "Permissions\n",
    breadcrumbs: ["Jazz 2", "Schemas & Policies"],
    url: "/docs/schemas/policies",
  },
  {
    id: "p-2",
    type: "heading",
    content: "Basic <mark>policies</mark>\n",
    url: "/docs/schemas/policies#basic",
  },
  {
    id: "p-3",
    type: "text",
    content: "How to write <mark>policies</mark>.",
    url: "/docs/schemas/policies#basic",
  },
  {
    id: "p-4",
    type: "text",
    content: "A second match in the same section.",
    url: "/docs/schemas/policies#basic",
  },
  { id: "p-5", type: "text", content: "Page summary.", url: "/docs/schemas/policies" },
  {
    id: "p-6",
    type: "text",
    content: "Only matched in body.",
    url: "/docs/schemas/policies#claims",
  },
  { id: "r", type: "page", content: "Relationships", url: "/docs/schemas/relationships" },
  {
    id: "r-1",
    type: "text",
    content: "Policies that follow references",
    url: "/docs/schemas/relationships",
  },
];

describe("wantsMarkdown", () => {
  const u = (q: string) => new URL(`https://jazz.tools/api/search?${q}`);

  it("follows format= over Accept", () => {
    expect(wantsMarkdown(u("q=a&format=md"), "application/json")).toBe(true);
    expect(wantsMarkdown(u("q=a&format=json"), "text/markdown")).toBe(false);
  });

  it("uses Accept when format is absent", () => {
    expect(wantsMarkdown(u("q=a"), "text/markdown")).toBe(true);
    expect(wantsMarkdown(u("q=a"), "text/plain, */*")).toBe(true);
    expect(wantsMarkdown(u("q=a"), "application/json, text/plain, */*")).toBe(false);
    expect(wantsMarkdown(u("q=a"), "*/*")).toBe(false);
    expect(wantsMarkdown(u("q=a"), null)).toBe(false);
  });
});

describe("parseLimit", () => {
  it("defaults, clamps and ignores junk", () => {
    expect(parseLimit(null, 8, 20)).toBe(8);
    expect(parseLimit("3", 8, 20)).toBe(3);
    expect(parseLimit("99", 8, 20)).toBe(20);
    expect(parseLimit("0", 8, 20)).toBe(8);
    expect(parseLimit("abc", 8, 20)).toBe(8);
  });
});

describe("groupByPage", () => {
  it("groups ranked hits into pages with sections keyed by anchor", () => {
    const pages = groupByPage(hits, 8, (path, anchor) =>
      path === "/docs/schemas/policies" && anchor === "claims" ? "Session claims" : undefined,
    );

    expect(pages.map((p) => p.path)).toEqual([
      "/docs/schemas/policies",
      "/docs/schemas/relationships",
    ]);
    expect(pages[0]).toMatchObject({
      title: "Permissions",
      breadcrumbs: ["Jazz 2", "Schemas & Policies"],
      sections: [
        { anchor: "basic", heading: "Basic policies", snippet: "How to write policies." },
        { anchor: null, heading: null, snippet: "Page summary." },
        { anchor: "claims", heading: "Session claims", snippet: "Only matched in body." },
      ],
    });
  });

  it("stops at the page limit", () => {
    expect(groupByPage(hits, 1)).toHaveLength(1);
  });

  it("truncates long snippets at a word boundary", () => {
    const long = Array.from({ length: 100 }, (_, i) => `word${i}`).join(" ");
    const [page] = groupByPage([{ id: "x", type: "text", content: long, url: "/docs/x" }], 8);
    const snippet = page!.sections[0]!.snippet!;
    expect(snippet.length).toBeLessThanOrEqual(281);
    expect(snippet.endsWith("…")).toBe(true);
    expect(snippet).toMatch(/ word\d+…$/);
  });
});

describe("markdownPath", () => {
  it("maps docs pages to their Markdown route", () => {
    expect(markdownPath("/docs/schemas/policies")).toBe("/llms.mdx/docs/schemas/policies");
    expect(markdownPath("/docs")).toBe("/llms.mdx/docs");
    expect(markdownPath("/blog/x")).toBe("/blog/x");
  });
});

describe("renderMarkdown", () => {
  const origin = "https://jazz.tools";

  it("renders pages, sections and Markdown links", () => {
    const md = renderMarkdown({ query: "policies", origin, pages: groupByPage(hits, 8) });
    expect(md).toContain("# Jazz docs search: policies");
    expect(md).toContain("## 1. Jazz 2 › Schemas & Policies › Permissions");
    expect(md).toContain("- Markdown: https://jazz.tools/llms.mdx/docs/schemas/policies");
    expect(md).toContain(
      "- [Basic policies](https://jazz.tools/docs/schemas/policies#basic): How to write policies.",
    );
    expect(md).toContain("- Top of page: Page summary.");
    expect(md).toContain("## 2. Relationships");
  });

  it("inlines full pages when given", () => {
    const md = renderMarkdown({
      query: "policies",
      origin,
      pages: groupByPage(hits, 8),
      fullText: new Map([["/docs/schemas/policies", "# Permissions\n\nBody."]]),
    });
    expect(md).toContain(
      '<page url="https://jazz.tools/docs/schemas/policies">\n\n# Permissions\n\nBody.\n\n</page>',
    );
  });

  it("points to the index and issues when nothing matches", () => {
    const md = renderMarkdown({ query: "zzz", origin, pages: [] });
    expect(md).toContain("No matching pages.");
    expect(md).toContain("https://jazz.tools/llms.txt");
    expect(md).toContain("https://github.com/garden-co/jazz/issues/new");
  });
});
