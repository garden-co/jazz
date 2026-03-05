import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  buildIndex,
  extractDescription,
  parseFrontmatter,
  resolveIncludes,
  splitIntoSections,
} from "./build-index.js";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const QUICKSTART_MDX = `---
title: Quickstart
description: Get started building a simple front-end app with Jazz in 10 minutes.
---

Build your first Jazz app. The jazz-tools package includes everything you need. Here is a third sentence.

## Install Jazz

Install jazz-tools to get started. Run npm install jazz-tools to install the package.

## Set Up Your Schema

Define a CoMap schema to describe your app's data structure using co.map().
`;

const COVALUES_MDX = `---
title: CoValues
---

CoValues are Jazz's collaborative values. A CoValue can be a CoMap, CoList, or CoFeed. CoValues are reactive and sync automatically.

## CoMap

Use CoMap to define collaborative key-value objects.

<Tabs items={["TypeScript"]}>
  <Tab value="TypeScript">
    <include cwd lang="ts">
      ../../examples/snippets.ts#comap-example
    </include>
  </Tab>
</Tabs>

## CoList

Use CoList for ordered collections. CoLists work like JavaScript arrays.
`;

const SUBSCRIPTIONS_MDX = `---
title: Subscriptions & Deep Loading
description: Learn how to subscribe to CoValues and handle loading states.
---

Set up subscriptions in your Jazz application.

## Subscription Hooks

Use useCoState to subscribe to a CoValue in React.
`;

const SNIPPETS_TS = `import { co } from "jazz-tools";

// #region comap-example
const TodoItem = co.map({
  title: co.string,
  done: co.boolean,
});
// #endregion comap-example

// #region another-region
const other = true;
// #endregion another-region
`;

// ---------------------------------------------------------------------------
// Test setup
// ---------------------------------------------------------------------------

let tmpDir: string;

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "build-index-test-"));

  // content/docs/
  await mkdir(join(tmpDir, "content", "docs", "core-concepts"), {
    recursive: true,
  });
  // examples/ (for <include> resolution)
  await mkdir(join(tmpDir, "examples"), { recursive: true });

  await writeFile(
    join(tmpDir, "content", "docs", "quickstart.mdx"),
    QUICKSTART_MDX,
  );
  await writeFile(
    join(tmpDir, "content", "docs", "core-concepts", "covalues.mdx"),
    COVALUES_MDX,
  );
  await writeFile(
    join(
      tmpDir,
      "content",
      "docs",
      "core-concepts",
      "subscription-and-loading.mdx",
    ),
    SUBSCRIPTIONS_MDX,
  );
  await writeFile(join(tmpDir, "examples", "snippets.ts"), SNIPPETS_TS);
});

afterEach(async () => {
  await rm(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Unit tests — pure helpers
// ---------------------------------------------------------------------------

describe("parseFrontmatter", () => {
  it("extracts title and description from frontmatter", () => {
    const result = parseFrontmatter(
      "---\ntitle: My Title\ndescription: My description.\n---\n\nBody text.",
    );
    expect(result.title).toBe("My Title");
    expect(result.description).toBe("My description.");
    expect(result.body).toBe("Body text.");
  });

  it("returns undefined description when not in frontmatter", () => {
    const result = parseFrontmatter("---\ntitle: Just Title\n---\n\nBody.");
    expect(result.title).toBe("Just Title");
    expect(result.description).toBeUndefined();
    expect(result.body).toBe("Body.");
  });

  it("handles missing frontmatter block", () => {
    const result = parseFrontmatter("# No frontmatter here\n\nBody text.");
    expect(result.title).toBeUndefined();
    expect(result.description).toBeUndefined();
    expect(result.body).toBe("# No frontmatter here\n\nBody text.");
  });
});

describe("extractDescription", () => {
  it("returns the first three sentences of plain text", () => {
    const body = "First sentence. Second sentence. Third sentence. Fourth.";
    expect(extractDescription(body)).toBe(
      "First sentence. Second sentence. Third sentence.",
    );
  });

  it("handles fewer than three sentences gracefully", () => {
    expect(extractDescription("Only one sentence.")).toBe("Only one sentence.");
    expect(extractDescription("One. Two.")).toBe("One. Two.");
  });

  it("skips leading fenced code blocks when extracting sentences", () => {
    const body = "```ts\nconst x = 1;\n```\n\nFirst sentence. Second. Third.";
    expect(extractDescription(body)).toBe("First sentence. Second. Third.");
  });

  it("returns empty string for empty body", () => {
    expect(extractDescription("")).toBe("");
  });
});

describe("resolveIncludes", () => {
  it("replaces an include directive with a fenced code block", async () => {
    const mdxFilePath = join(
      tmpDir,
      "content",
      "docs",
      "core-concepts",
      "covalues.mdx",
    );
    // fileCwd mirrors what buildIndex passes: the content root, not the file's own dir
    const fileCwd = join(tmpDir, "content", "docs");
    const content = `<include cwd lang="ts">
  ../../examples/snippets.ts#comap-example
</include>`;
    const result = await resolveIncludes(content, mdxFilePath, fileCwd);

    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    expect(result).toContain("const TodoItem = co.map({");
    // Should not include lines from outside the region
    expect(result).not.toContain("#region comap-example");
    expect(result).not.toContain("another-region");
  });

  it("includes full file content when no anchor is specified", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "quickstart.mdx");
    const fileCwd = join(tmpDir, "content", "docs");
    const content = `<include cwd lang="ts">
  ../../examples/snippets.ts
</include>`;
    const result = await resolveIncludes(content, mdxFilePath, fileCwd);

    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    // Full file content should be present
    expect(result).toContain("another-region");
  });

  it("handles extra attributes such as meta alongside cwd and lang", async () => {
    const mdxFilePath = join(
      tmpDir,
      "content",
      "docs",
      "core-concepts",
      "covalues.mdx",
    );
    const fileCwd = join(tmpDir, "content", "docs");
    const content = `<include
  cwd
  lang="ts"
  meta='title="examples/snippets.ts"'
>
  ../../examples/snippets.ts#comap-example
</include>`;
    const result = await resolveIncludes(content, mdxFilePath, fileCwd);
    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    expect(result).toContain("const TodoItem = co.map({");
  });

  it("leaves content without include directives unchanged", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "quickstart.mdx");
    const content = "## Section\n\nJust plain text.";
    const result = await resolveIncludes(content, mdxFilePath);
    expect(result).toBe(content);
  });
});

describe("splitIntoSections", () => {
  it("returns one entry per ## heading", () => {
    const body =
      "Intro text.\n\n## Section One\n\nContent one.\n\n## Section Two\n\nContent two.";
    const sections = splitIntoSections(body);
    expect(sections).toHaveLength(3); // preamble + 2 sections
    expect(sections[1]!.heading).toBe("Section One");
    expect(sections[2]!.heading).toBe("Section Two");
  });

  it("treats content before first ## as a section with empty heading", () => {
    const body = "Preamble text.\n\n## First Section\n\nContent.";
    const sections = splitIntoSections(body);
    expect(sections[0]!.heading).toBe("");
    expect(sections[0]!.body).toContain("Preamble text.");
  });

  it("returns a single section with empty heading for pages with no ## headings", () => {
    const body = "Just content, no sections.";
    const sections = splitIntoSections(body);
    expect(sections).toHaveLength(1);
    expect(sections[0]!.heading).toBe("");
    expect(sections[0]!.body).toBe("Just content, no sections.");
  });
});

// ---------------------------------------------------------------------------
// Integration tests — buildIndex end-to-end
// ---------------------------------------------------------------------------

describe("buildIndex", () => {
  const outputDir = () => join(tmpDir, "output");

  beforeEach(() => mkdir(join(tmpDir, "output"), { recursive: true }));

  it("produces docs-index.db and docs-index.txt", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const { existsSync } = await import("node:fs");
    expect(existsSync(join(outputDir(), "docs-index.db"))).toBe(true);
    expect(existsSync(join(outputDir(), "docs-index.txt"))).toBe(true);
  });

  it("pages table has correct schema", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row = db
      .prepare("SELECT title, slug, description, body FROM pages LIMIT 1")
      .get();
    expect(row).toBeDefined();
    db.close();
  });

  it("every MDX file produces a page row", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db.prepare("SELECT slug FROM pages ORDER BY slug").all();
    const slugs = rows.map((r: any) => r.slug);
    expect(slugs).toEqual(
      expect.arrayContaining([
        "quickstart",
        "core-concepts/covalues",
        "core-concepts/subscription-and-loading",
      ]),
    );
    expect(slugs).toHaveLength(3);
    db.close();
  });

  it("slug is path relative to contentDir without .mdx extension", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT slug FROM pages WHERE slug = 'core-concepts/covalues'")
      .get();
    expect(row).toBeDefined();
    db.close();
  });

  it("title comes from MDX frontmatter", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT title FROM pages WHERE slug = 'quickstart'")
      .get();
    expect(row.title).toBe("Quickstart");
    db.close();
  });

  it("description comes from frontmatter when present", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT description FROM pages WHERE slug = 'quickstart'")
      .get();
    expect(row.description).toBe(
      "Get started building a simple front-end app with Jazz in 10 minutes.",
    );
    db.close();
  });

  it("description falls back to first three sentences when no frontmatter description", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare(
        "SELECT description FROM pages WHERE slug = 'core-concepts/covalues'",
      )
      .get();
    // First three sentences from the body
    expect(row.description).toBe(
      "CoValues are Jazz's collaborative values. A CoValue can be a CoMap, CoList, or CoFeed. CoValues are reactive and sync automatically.",
    );
    db.close();
  });

  it("resolves <include> directives: body contains code, not include tag", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT body FROM pages WHERE slug = 'core-concepts/covalues'")
      .get();
    expect(row.body).not.toContain("<include");
    expect(row.body).toContain("const TodoItem = co.map({");
    db.close();
  });

  it("strips JSX component tags from body, preserving text and code content", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT body FROM pages WHERE slug = 'core-concepts/covalues'")
      .get();
    expect(row.body).not.toContain("<Tabs");
    expect(row.body).not.toContain("<Tab");
    // Code content from inside the Tab should still be present
    expect(row.body).toContain("const TodoItem = co.map({");
    db.close();
  });

  it("each ## heading produces a sections_fts row", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db
      .prepare(
        "SELECT section_heading FROM sections_fts WHERE slug = 'quickstart' ORDER BY section_heading",
      )
      .all();
    const headings = rows.map((r: any) => r.section_heading);
    expect(headings).toContain("Install Jazz");
    expect(headings).toContain("Set Up Your Schema");
    db.close();
  });

  it("sections_fts is queryable via FTS5 MATCH", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db
      .prepare(
        "SELECT slug, section_heading FROM sections_fts WHERE sections_fts MATCH 'useCoState' ORDER BY bm25(sections_fts)",
      )
      .all();
    expect(rows.length).toBeGreaterThan(0);
    const match: any = rows[0];
    expect(match.slug).toBe("core-concepts/subscription-and-loading");
    db.close();
  });

  it("docs-index.txt contains ===PAGE:slug=== markers for all pages", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const txt = await readFile(join(outputDir(), "docs-index.txt"), "utf8");
    expect(txt).toContain("===PAGE:quickstart===");
    expect(txt).toContain("===PAGE:core-concepts/covalues===");
    expect(txt).toContain("===PAGE:core-concepts/subscription-and-loading===");
  });

  it("docs-index.txt includes TITLE and DESCRIPTION lines per page", async () => {
    await buildIndex({
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    });

    const txt = await readFile(join(outputDir(), "docs-index.txt"), "utf8");
    expect(txt).toContain("TITLE:Quickstart");
    expect(txt).toContain(
      "DESCRIPTION:Get started building a simple front-end app with Jazz in 10 minutes.",
    );
  });

  it("is deterministic: running twice produces identical output", async () => {
    const opts = {
      contentDir: join(tmpDir, "content", "docs"),
      outputDir: outputDir(),
    };

    await buildIndex(opts);
    const txt1 = await readFile(join(outputDir(), "docs-index.txt"), "utf8");

    // Remove db and txt, rebuild
    await rm(join(outputDir(), "docs-index.db"));
    await rm(join(outputDir(), "docs-index.txt"));

    await buildIndex(opts);
    const txt2 = await readFile(join(outputDir(), "docs-index.txt"), "utf8");

    expect(txt1).toBe(txt2);
  });
});
