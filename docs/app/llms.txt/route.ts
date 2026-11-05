import { source } from "@/lib/source";

export const revalidate = false;

export async function GET() {
  const lines: string[] = [];
  lines.push("# Documentation");
  lines.push("");
  lines.push(
    "Search the docs as Markdown: GET /api/search?q=<words>&format=md (add &full=1 to inline the top pages).",
  );
  lines.push("Every page below is also available as Markdown at /llms.mdx/docs/<path>.");
  lines.push("");
  for (const page of source.getPages()) {
    lines.push(`- [${page.data.title}](${page.url}): ${page.data.description}`);
  }
  return new Response(lines.join("\n"), {
    headers: {
      "Content-Type": "text/markdown; charset=utf-8",
    },
  });
}
