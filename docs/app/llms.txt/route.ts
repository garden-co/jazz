import { docsNavSections } from "@/lib/docs-nav";
import { source } from "@/lib/source";
import type { NavEntry } from "@/components/docs/docs-side-nav";

export const revalidate = false;

const ORIGIN = "https://jazz.tools";

/**
 * The docs index for agents. Sections and order follow the sidebar groups in
 * `content/docs/meta.json`; every link points at the page's Markdown version.
 */
export async function GET() {
  const descriptions = new Map(source.getPages().map((page) => [page.url, page.data.description]));

  const lines: string[] = [
    "# Jazz",
    "",
    "> Jazz is a relational database that runs inside your backend, with real-time queries and permissions built in.",
    "",
    "**Start here as an agent:** before building with Jazz, read the [Agent Quickstart](https://jazz.tools/quickstart.md). It walks you through the decisions to make with your user and how to build, test and ship.",
    "",
    "This file is an index of the docs. Append `.md` to any docs page URL for Markdown.",
    "Search the docs as Markdown: GET https://jazz.tools/api/search?q=<words>&format=md (add &full=1 to inline the top pages).",
  ];

  function entryLines(entry: NavEntry, depth: number) {
    const indent = "  ".repeat(depth);
    const description = entry.url ? descriptions.get(entry.url) : undefined;
    const label = entry.url ? `[${entry.label}](${ORIGIN}${entry.url}.md)` : entry.label;
    lines.push(`${indent}- ${label}${description ? `: ${description}` : ""}`);
    for (const child of entry.children ?? []) entryLines(child, depth + 1);
  }

  for (const section of docsNavSections(source.getPageTree())) {
    lines.push("", `## ${section.title}`, "");
    for (const entry of section.entries) entryLines(entry, 0);
  }

  return new Response(lines.join("\n") + "\n", {
    // Markdown, served as plain text: some agent browsing tools refuse text/markdown (#2570).
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
