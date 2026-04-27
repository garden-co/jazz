import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import type { IssueItem, ItemKind } from "./types.js";
import { validateSlug } from "./slugs.js";

function section(markdown: string, heading: string): string {
  const pattern = new RegExp(`^## ${heading}\\n+([\\s\\S]*?)(?=\\n## |\\n*$)`, "m");
  return markdown.match(pattern)?.[1]?.trim() ?? "";
}

function title(markdown: string, fallback: string): string {
  return markdown.match(/^# (.+)$/m)?.[1]?.trim() ?? fallback;
}

async function importFiles(dir: string, kind: ItemKind): Promise<IssueItem[]> {
  const entries = await readdir(dir, { withFileTypes: true }).catch(() => []);
  const items: IssueItem[] = [];

  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      items.push(...(await importFiles(path, kind)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".md")) {
      const slug = validateSlug(basename(entry.name, ".md"));
      const markdown = await readFile(path, "utf8");
      items.push({
        kind,
        slug,
        title: title(markdown, slug),
        description: section(markdown, "What"),
      });
    }
  }

  return items;
}

export async function importMarkdownTodo(todoDir: string): Promise<IssueItem[]> {
  const items = [
    ...(await importFiles(join(todoDir, "ideas"), "idea")),
    ...(await importFiles(join(todoDir, "issues"), "issue")),
  ].sort((a, b) => a.slug.localeCompare(b.slug));
  const seen = new Set<string>();

  for (const item of items) {
    if (seen.has(item.slug)) {
      throw new Error(`Duplicate item slug: ${item.slug}`);
    }
    seen.add(item.slug);
  }

  return items;
}

function formatIdea(item: IssueItem): string {
  return `# ${item.title}\n\n## What\n\n${item.description}\n\n## Notes\n\n`;
}

function formatIssue(item: IssueItem): string {
  return `# ${item.title}\n\n## What\n\n${item.description}\n\n## Priority\n\nunknown\n\n## Notes\n\n`;
}

export async function exportMarkdownTodo(todoDir: string, items: IssueItem[]): Promise<void> {
  const seen = new Set<string>();

  for (const item of items) {
    validateSlug(item.slug);
    if (seen.has(item.slug)) {
      throw new Error(`Duplicate item slug: ${item.slug}`);
    }
    seen.add(item.slug);

    const file =
      item.kind === "idea"
        ? join(todoDir, "ideas", "1_mvp", `${item.slug}.md`)
        : join(todoDir, "issues", `${item.slug}.md`);
    await mkdir(dirname(file), { recursive: true });
    await writeFile(file, item.kind === "idea" ? formatIdea(item) : formatIssue(item));
  }
}
