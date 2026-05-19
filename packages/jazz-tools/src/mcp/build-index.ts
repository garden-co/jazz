import { mkdir, readdir, readFile, unlink, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

import {
  extractDescription,
  parseFrontmatter,
  resolveIncludes,
  splitIntoSections,
  stripJsx,
} from "./parse.js";

// Re-exported so existing importers (and tests) keep a single entry point.
// The implementations live in parse.ts, which has no node:sqlite dependency —
// the text-search fallback imports them from there, never from this module.
export {
  extractDescription,
  parseFrontmatter,
  resolveIncludes,
  splitIntoSections,
  stripJsx,
} from "./parse.js";
export type { FrontmatterResult, Section } from "./parse.js";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface BuildIndexOptions {
  /** Path to the content/docs directory containing .mdx files. */
  contentDir: string;
  /** Directory where docs-index.db and docs-index.txt are written. */
  outputDir: string;
  /**
   * Base directory used to resolve <include cwd> paths.
   * Mirrors fumadocs' file.cwd (the app working directory).
   * Defaults to contentDir when not specified.
   */
  fileCwd?: string;
}

// ---------------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------------

async function findMdxFiles(dir: string): Promise<string[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await findMdxFiles(fullPath)));
    } else if (entry.name.endsWith(".mdx")) {
      files.push(fullPath);
    }
  }

  return files.sort();
}

// ---------------------------------------------------------------------------
// Main build function
// ---------------------------------------------------------------------------

export async function buildIndex({
  contentDir,
  outputDir,
  fileCwd,
}: BuildIndexOptions): Promise<void> {
  await mkdir(outputDir, { recursive: true });

  const mdxFiles = await findMdxFiles(contentDir);

  // fileCwd: base for <include cwd> resolution.
  // Defaults to contentDir (works for tests). Production callers pass the
  // docs app working directory (docs/) where ../examples/... resolves correctly.
  const resolvedFileCwd = fileCwd ?? contentDir;

  const pages = await Promise.all(
    mdxFiles.map(async (filePath) => {
      const raw = await readFile(filePath, "utf8");
      const { title, description, body: rawBody } = parseFrontmatter(raw);

      const withIncludes = await resolveIncludes(rawBody, filePath, resolvedFileCwd);
      const body = stripJsx(withIncludes);

      // Slug: path relative to contentDir, no extension, forward slashes
      const slug = relative(contentDir, filePath)
        .replace(/\.mdx$/, "")
        .replace(/\\/g, "/");

      const finalDescription = description ?? extractDescription(body);

      return {
        slug,
        title: title ?? slug,
        description: finalDescription,
        body,
      };
    }),
  );

  // Sort by slug for determinism
  pages.sort((a, b) => a.slug.localeCompare(b.slug));

  // --- SQLite DB ---
  const dbPath = join(outputDir, "docs-index.db");
  // Remove any stale DB so we always start fresh
  try {
    await unlink(dbPath);
  } catch {
    // File didn't exist — fine
  }

  const db = new DatabaseSync(dbPath);

  db.exec(`
    CREATE TABLE pages (
      title       TEXT NOT NULL,
      slug        TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      body        TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE sections_fts USING fts5(
      title,
      slug        UNINDEXED,
      section_heading,
      body,
      tokenize    = 'unicode61'
    );
  `);

  const insertPage = db.prepare(
    "INSERT INTO pages (title, slug, description, body) VALUES (?, ?, ?, ?)",
  );
  const insertSection = db.prepare(
    "INSERT INTO sections_fts (title, slug, section_heading, body) VALUES (?, ?, ?, ?)",
  );

  for (const page of pages) {
    insertPage.run(page.title, page.slug, page.description, page.body);

    for (const section of splitIntoSections(page.body)) {
      insertSection.run(page.title, page.slug, section.heading, section.body);
    }
  }

  db.close();

  // --- Plain-text file ---
  const txtParts = pages.map(
    (p) => `===PAGE:${p.slug}===\nTITLE:${p.title}\nDESCRIPTION:${p.description}\n\n${p.body}`,
  );
  await writeFile(join(outputDir, "docs-index.txt"), txtParts.join("\n\n"), "utf8");
}

// ---------------------------------------------------------------------------
// Script entry point
// ---------------------------------------------------------------------------

const isMain = typeof process !== "undefined" && process.argv[1] === fileURLToPath(import.meta.url);

if (isMain) {
  const here = dirname(fileURLToPath(import.meta.url));
  const contentDir = resolve(here, "../../../../docs/content/docs");
  const outDir = resolve(here, "../../bin");
  // docs/ is the Next.js app working directory; <include cwd> paths in MDX
  // are relative to it (e.g. ../examples/... resolves to the repo examples/).
  const fileCwd = resolve(here, "../../../../docs");

  buildIndex({ contentDir, outputDir: outDir, fileCwd })
    .then(() => console.log("docs index built →", outDir))
    .catch((err: unknown) => {
      console.error("build-index failed:", err);
      process.exit(1);
    });
}
