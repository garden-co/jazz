import { mkdir, readdir, readFile, unlink, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface FrontmatterResult {
  title?: string;
  description?: string;
  body: string;
}

export interface Section {
  heading: string;
  body: string;
}

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
// Pure helpers (exported for unit testing)
// ---------------------------------------------------------------------------

/**
 * Splits YAML frontmatter from MDX content.
 * Returns title, optional description, and the body after the closing `---`.
 */
export function parseFrontmatter(content: string): FrontmatterResult {
  const match = content.match(/^---\n([\s\S]*?)\n---\n*([\s\S]*)$/);
  if (!match) {
    return { body: content };
  }

  const fm = match[1];
  const body = match[2].trimStart();

  const titleMatch = fm.match(/^title:\s*(.+)$/m);
  const descMatch = fm.match(/^description:\s*(.+)$/m);

  return {
    title: titleMatch?.[1]?.trim(),
    description: descMatch?.[1]?.trim(),
    body,
  };
}

/**
 * Extracts a description from rendered body text.
 * Strips fenced code blocks and headings, then returns the first three sentences.
 */
export function extractDescription(body: string): string {
  if (!body) return "";

  // Strip all fenced code blocks
  let text = body.replace(/```[\s\S]*?```/g, "");
  // Strip markdown headings
  text = text.replace(/^#{1,6}\s+.*/gm, "");
  // Strip JSX tags (in case they weren't stripped already)
  text = text.replace(/<\/?[A-Z][a-zA-Z]*[^>]*>/g, "");
  // Collapse all whitespace to single spaces
  text = text.replace(/\s+/g, " ").trim();

  const sentences: string[] = [];
  const sentenceRe = /[^.!?]+[.!?]+/g;
  let m: RegExpExecArray | null;
  while ((m = sentenceRe.exec(text)) !== null && sentences.length < 3) {
    sentences.push(m[0].trim());
  }

  return sentences.join(" ");
}

/**
 * Resolves `<include cwd lang="…">path[#anchor]</include>` directives in MDX
 * content, replacing each with a fenced code block.  Paths are resolved
 * relative to the MDX file's directory unless the `cwd` attribute is present,
 * in which case fileCwd is used (mirrors fumadocs' file.cwd semantics).
 * Additional attributes (e.g. meta='…') are tolerated and ignored.
 */
export async function resolveIncludes(
  content: string,
  mdxFilePath: string,
  fileCwd?: string,
): Promise<string> {
  // Match <include ...attrs...>path</include> — any attribute combination.
  const includeRe = /<include\s+([^>]*?)>\s*([\s\S]*?)\s*<\/include>/g;

  // Collect all replacements first, then apply (to avoid regex state issues)
  const replacements: Array<{ original: string; replacement: string }> = [];
  let m: RegExpExecArray | null;

  while ((m = includeRe.exec(content)) !== null) {
    const attrs = m[1];
    const hasCwd = /\bcwd\b/.test(attrs);
    const langMatch = attrs.match(/\blang="([^"]+)"/);
    if (!langMatch) continue; // not a code include — leave untouched
    const lang = langMatch[1];
    const rawPath = m[2].trim();
    const hashIdx = rawPath.indexOf("#");
    const filePath = hashIdx === -1 ? rawPath : rawPath.slice(0, hashIdx);
    const anchor = hashIdx === -1 ? null : rawPath.slice(hashIdx + 1);

    const base = hasCwd && fileCwd ? fileCwd : dirname(mdxFilePath);
    const resolvedPath = resolve(base, filePath.trim());
    let fileContent = await readFile(resolvedPath, "utf8");

    if (anchor) {
      const regionRe = new RegExp(`// #region ${anchor}\\n([\\s\\S]*?)// #endregion ${anchor}`);
      const regionMatch = fileContent.match(regionRe);
      if (regionMatch) {
        fileContent = regionMatch[1].trimEnd();
      }
    }

    replacements.push({
      original: m[0],
      replacement: `\`\`\`${lang}\n${fileContent}\n\`\`\``,
    });
  }

  let result = content;
  for (const { original, replacement } of replacements) {
    result = result.replace(original, replacement);
  }
  return result;
}

/**
 * Strips JSX component tags (uppercase-initial or namespaced) from content,
 * preserving text and fenced code blocks inside them.
 * Also strips MDX import/export declarations.
 */
export function stripJsx(content: string): string {
  let result = content;
  // Strip MDX import/export lines
  result = result.replace(/^(?:import|export)\s+.*$/gm, "");
  // Self-closing JSX: <Component />
  result = result.replace(/<[A-Z][a-zA-Z]*(?:\.[a-zA-Z]+)?[^>]*\/>/g, "");
  // Opening JSX: <Component ...>
  result = result.replace(/<[A-Z][a-zA-Z]*(?:\.[a-zA-Z]+)?[^>]*>/g, "");
  // Closing JSX: </Component>
  result = result.replace(/<\/[A-Z][a-zA-Z]*(?:\.[a-zA-Z]+)?>/g, "");
  // Strip region marker lines left over from include expansion
  // Covers: // #region, # #region, <!-- #region, /* #region */
  result = result.replace(/^[ \t]*(?:\/\/|#|<!--|\/\*)\s*#?(?:end)?region\b.*$/gm, "");
  // Collapse lines that contain only whitespace into empty lines
  // (artefact of Tab/Tabs wrapper components being stripped)
  result = result.replace(/^[ \t]+$/gm, "");
  // Collapse 3+ blank lines to 2
  result = result.replace(/\n{3,}/g, "\n\n");
  return result.trim();
}

/**
 * Splits a body string into sections on `## ` heading boundaries.
 * Content before the first heading is returned as a section with an empty heading.
 */
export function splitIntoSections(body: string): Section[] {
  const parts = body.split(/(?=^## )/m);

  return parts
    .map((part): Section => {
      const headingMatch = part.match(/^## (.+)\n/);
      if (!headingMatch) {
        return { heading: "", body: part.trim() };
      }
      return {
        heading: headingMatch[1].trim(),
        body: part.replace(/^## .+\n/, "").trim(),
      };
    })
    .filter((s) => s.body.length > 0 || s.heading.length > 0);
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
