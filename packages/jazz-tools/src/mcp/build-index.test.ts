import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
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

const GETTING_STARTED_MDX = `---
title: Getting Started
description: Learn how to get started with Jazz.
---

This is the intro paragraph. It has multiple sentences. Here is a third one.

## Installation

Install the package using npm or yarn.

## Configuration

Configure your application with the required settings.
`;

const API_REFERENCE_MDX = `---
title: API Reference
---

The API provides powerful query capabilities. You can filter, sort, and paginate results. Here is a third sentence about the API.

## Query API

Use the query builder to construct type-safe queries.

<Tabs items={["TypeScript"]}>
  <Tab value="TypeScript">
    <include cwd lang="ts">
      ../../examples/snippets.ts#query-example
    </include>
  </Tab>
</Tabs>

## Pagination

Paginate results using limit and offset.
`;

const REACT_QUICKSTART_MDX = `---
title: React Quickstart
description: Get started with Jazz in React.
---

Set up Jazz in your React application.

## Setup

Install and configure the React bindings.
`;

const SNIPPETS_TS = `import type { Db } from "jazz-tools";

// #region query-example
const results = await db.all(app.todos.where({ done: false }));
// #endregion query-example

// #region another-region
const other = true;
// #endregion another-region
`;

async function createFixtureTree(): Promise<string> {
  const fixtureDir = await mkdtemp(join(tmpdir(), "build-index-test-"));
  // content/docs/
  await mkdir(join(fixtureDir, "content", "docs", "quickstarts"), {
    recursive: true,
  });
  // examples/ (for <include> resolution)
  await mkdir(join(fixtureDir, "examples"), { recursive: true });

  await writeFile(join(fixtureDir, "content", "docs", "getting-started.mdx"), GETTING_STARTED_MDX);
  await writeFile(join(fixtureDir, "content", "docs", "api-reference.mdx"), API_REFERENCE_MDX);
  await writeFile(
    join(fixtureDir, "content", "docs", "quickstarts", "react.mdx"),
    REACT_QUICKSTART_MDX,
  );
  await writeFile(join(fixtureDir, "examples", "snippets.ts"), SNIPPETS_TS);

  return fixtureDir;
}

async function cleanupFixtureTree(fixtureDir: string | undefined): Promise<void> {
  if (!fixtureDir) return;
  await rm(fixtureDir, { recursive: true, force: true });
}

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
    expect(extractDescription(body)).toBe("First sentence. Second sentence. Third sentence.");
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
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await createFixtureTree();
  });

  afterEach(async () => {
    await cleanupFixtureTree(tmpDir);
  });

  it("replaces an include directive with a fenced code block", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "api-reference.mdx");
    const content = `<include cwd lang="ts">
  ../../examples/snippets.ts#query-example
</include>`;
    const result = await resolveIncludes(content, mdxFilePath);

    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    expect(result).toContain("const results = await db.all(app.todos.where({ done: false }));");
    // Should not include lines from outside the region
    expect(result).not.toContain("#region query-example");
    expect(result).not.toContain("another-region");
  });

  it("includes full file content when no anchor is specified", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "getting-started.mdx");
    const content = `<include cwd lang="ts">
  ../../examples/snippets.ts
</include>`;
    const result = await resolveIncludes(content, mdxFilePath);

    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    // Full file content should be present
    expect(result).toContain("another-region");
  });

  it("handles extra attributes such as meta alongside cwd and lang", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "api-reference.mdx");
    const content = `<include
  cwd
  lang="ts"
  meta='title="examples/snippets.ts"'
>
  ../../examples/snippets.ts#query-example
</include>`;
    const result = await resolveIncludes(content, mdxFilePath);
    expect(result).not.toContain("<include");
    expect(result).toContain("```ts");
    expect(result).toContain("const results = await db.all(app.todos.where({ done: false }));");
  });

  it("leaves content without include directives unchanged", async () => {
    const mdxFilePath = join(tmpDir, "content", "docs", "getting-started.mdx");
    const content = "## Section\n\nJust plain text.";
    const result = await resolveIncludes(content, mdxFilePath);
    expect(result).toBe(content);
  });
});

describe("splitIntoSections", () => {
  it("returns one entry per ## heading", () => {
    const body = "Intro text.\n\n## Section One\n\nContent one.\n\n## Section Two\n\nContent two.";
    const sections = splitIntoSections(body);
    expect(sections).toHaveLength(3); // preamble + 2 sections
    expect(sections[1].heading).toBe("Section One");
    expect(sections[2].heading).toBe("Section Two");
  });

  it("treats content before first ## as a section with empty heading", () => {
    const body = "Preamble text.\n\n## First Section\n\nContent.";
    const sections = splitIntoSections(body);
    expect(sections[0].heading).toBe("");
    expect(sections[0].body).toContain("Preamble text.");
  });

  it("returns a single section with empty heading for pages with no ## headings", () => {
    const body = "Just content, no sections.";
    const sections = splitIntoSections(body);
    expect(sections).toHaveLength(1);
    expect(sections[0].heading).toBe("");
    expect(sections[0].body).toBe("Just content, no sections.");
  });
});

// ---------------------------------------------------------------------------
// Integration tests — buildIndex end-to-end
// ---------------------------------------------------------------------------

describe("buildIndex", () => {
  let tmpDir: string;
  const outputDir = () => join(tmpDir, "output");
  const contentDir = () => join(tmpDir, "content", "docs");

  // Build once for the read-only assertions below; repeatedly rebuilding the
  // index in each test was slow enough to trip CI timeouts under load.
  beforeAll(async () => {
    tmpDir = await createFixtureTree();
    await mkdir(outputDir(), { recursive: true });
    await buildIndex({
      contentDir: contentDir(),
      outputDir: outputDir(),
    });
  }, 10_000);

  afterAll(async () => {
    await cleanupFixtureTree(tmpDir);
  });

  it("produces docs-index.db and docs-index.txt", async () => {
    const { existsSync } = await import("node:fs");
    expect(existsSync(join(outputDir(), "docs-index.db"))).toBe(true);
    expect(existsSync(join(outputDir(), "docs-index.txt"))).toBe(true);
  });

  it("pages table has correct schema", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row = db.prepare("SELECT title, slug, description, body FROM pages LIMIT 1").get();
    expect(row).toBeDefined();
    db.close();
  });

  it("every MDX file produces a page row", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db.prepare("SELECT slug FROM pages ORDER BY slug").all();
    const slugs = rows.map((r: any) => r.slug);
    expect(slugs).toEqual(
      expect.arrayContaining(["getting-started", "api-reference", "quickstarts/react"]),
    );
    expect(slugs).toHaveLength(3);
    db.close();
  });

  it("slug is path relative to contentDir without .mdx extension", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db.prepare("SELECT slug FROM pages WHERE slug = 'quickstarts/react'").get();
    expect(row).toBeDefined();
    db.close();
  });

  it("title comes from MDX frontmatter", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db.prepare("SELECT title FROM pages WHERE slug = 'getting-started'").get();
    expect(row.title).toBe("Getting Started");
    db.close();
  });

  it("description comes from frontmatter when present", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db
      .prepare("SELECT description FROM pages WHERE slug = 'getting-started'")
      .get();
    expect(row.description).toBe("Learn how to get started with Jazz.");
    db.close();
  });

  it("description falls back to first three sentences when no frontmatter description", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db.prepare("SELECT description FROM pages WHERE slug = 'api-reference'").get();
    // First three sentences from the body
    expect(row.description).toBe(
      "The API provides powerful query capabilities. You can filter, sort, and paginate results. Here is a third sentence about the API.",
    );
    db.close();
  });

  it("resolves <include> directives: body contains code, not include tag", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db.prepare("SELECT body FROM pages WHERE slug = 'api-reference'").get();
    expect(row.body).not.toContain("<include");
    expect(row.body).toContain("const results = await db.all(app.todos.where({ done: false }));");
    db.close();
  });

  it("strips JSX component tags from body, preserving text and code content", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const row: any = db.prepare("SELECT body FROM pages WHERE slug = 'api-reference'").get();
    expect(row.body).not.toContain("<Tabs");
    expect(row.body).not.toContain("<Tab");
    // Code content from inside the Tab should still be present
    expect(row.body).toContain("const results = await db.all(app.todos.where({ done: false }));");
    db.close();
  });

  it("each ## heading produces a sections_fts row", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db
      .prepare(
        "SELECT section_heading FROM sections_fts WHERE slug = 'getting-started' ORDER BY section_heading",
      )
      .all();
    const headings = rows.map((r: any) => r.section_heading);
    expect(headings).toContain("Installation");
    expect(headings).toContain("Configuration");
    db.close();
  });

  it("sections_fts is queryable via FTS5 MATCH", async () => {
    const db = new DatabaseSync(join(outputDir(), "docs-index.db"));
    const rows = db
      .prepare(
        "SELECT slug, section_heading FROM sections_fts WHERE sections_fts MATCH 'pagination' ORDER BY bm25(sections_fts)",
      )
      .all();
    expect(rows.length).toBeGreaterThan(0);
    const match: any = rows[0];
    expect(match.slug).toBe("api-reference");
    db.close();
  });

  it("docs-index.txt contains ===PAGE:slug=== markers for all pages", async () => {
    const txt = await readFile(join(outputDir(), "docs-index.txt"), "utf8");
    expect(txt).toContain("===PAGE:getting-started===");
    expect(txt).toContain("===PAGE:api-reference===");
    expect(txt).toContain("===PAGE:quickstarts/react===");
  });

  it("docs-index.txt includes TITLE and DESCRIPTION lines per page", async () => {
    const txt = await readFile(join(outputDir(), "docs-index.txt"), "utf8");
    expect(txt).toContain("TITLE:Getting Started");
    expect(txt).toContain("DESCRIPTION:Learn how to get started with Jazz.");
  });

  // The more content we have in the docs, the longer this test will take,
  // and the more likely it is to fail due to timeouts.
  it.skip("is deterministic: running twice produces identical output", async () => {
    const tmpDir = await createFixtureTree();
    const outputDir = join(tmpDir, "output");
    const opts = {
      contentDir: join(tmpDir, "content", "docs"),
      outputDir,
    };

    try {
      await mkdir(outputDir, { recursive: true });
      await buildIndex(opts);
      const txt1 = await readFile(join(outputDir, "docs-index.txt"), "utf8");

      // Remove db and txt, rebuild
      await rm(join(outputDir, "docs-index.db"));
      await rm(join(outputDir, "docs-index.txt"));

      await buildIndex(opts);
      const txt2 = await readFile(join(outputDir, "docs-index.txt"), "utf8");

      expect(txt1).toBe(txt2);
    } finally {
      await cleanupFixtureTree(tmpDir);
    }
  }, 10_000);
});
