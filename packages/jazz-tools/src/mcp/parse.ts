// Pure MDX → pages/sections parsing helpers.
//
// This module deliberately has NO `node:sqlite` dependency (directly or
// transitively) so it stays importable on Node < 22 and non-Node runtimes.
// The text-search fallback backend depends on it; only the index *writer*
// (build-index.ts) needs node:sqlite.

import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

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
  const bodyMatch = match[2];
  if (fm === undefined || bodyMatch === undefined) {
    return { body: content };
  }
  const body = bodyMatch.trimStart();

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
    const includePath = m[2];
    if (attrs === undefined || includePath === undefined) continue;
    const hasCwd = /\bcwd\b/.test(attrs);
    const langMatch = attrs.match(/\blang="([^"]+)"/);
    if (!langMatch) continue; // not a code include — leave untouched
    const lang = langMatch[1];
    if (lang === undefined) continue;
    const rawPath = includePath.trim();
    const hashIdx = rawPath.indexOf("#");
    const filePath = hashIdx === -1 ? rawPath : rawPath.slice(0, hashIdx);
    const anchor = hashIdx === -1 ? null : rawPath.slice(hashIdx + 1);

    const base = hasCwd && fileCwd ? fileCwd : dirname(mdxFilePath);
    const resolvedPath = resolve(base, filePath.trim());
    let fileContent = await readFile(resolvedPath, "utf8");

    if (anchor) {
      const regionRe = new RegExp(`// #region ${anchor}\\n([\\s\\S]*?)// #endregion ${anchor}`);
      const regionMatch = fileContent.match(regionRe);
      const regionBody = regionMatch?.[1];
      if (regionBody !== undefined) {
        fileContent = regionBody.trimEnd();
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
      const heading = headingMatch[1];
      return {
        heading: heading?.trim() ?? "",
        body: part.replace(/^## .+\n/, "").trim(),
      };
    })
    .filter((s) => s.body.length > 0 || s.heading.length > 0);
}
