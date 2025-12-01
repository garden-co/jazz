import fs from "node:fs";
import path from "node:path";
import { visit } from "unist-util-visit";

interface Options {
  dir: string;
}

const COMMENT_STYLES = {
  jsStyle: (pattern: string) => `//\\s*${pattern}`,
  htmlStyle: (pattern: string) => `<!--\\s*${pattern}\\s*-->`,
  jsxStyle: (pattern: string) => `\\{\\s*\\/\\*\\s*${pattern}\\s*\\*\\/\\s*\\}`,
} as const;

function createMultiStylePattern(pattern: string, flags = ""): RegExp[] {
  return [
    new RegExp(`^\\s*${COMMENT_STYLES.jsStyle(pattern)}`, flags),
    new RegExp(`^\\s*${COMMENT_STYLES.htmlStyle(pattern)}`, flags),
    new RegExp(`^\\s*${COMMENT_STYLES.jsxStyle(pattern)}`, flags),
  ];
}

function matchesAnyPattern(
  line: string,
  patterns: RegExp[],
): RegExpMatchArray | null {
  for (const pattern of patterns) {
    const match = line.match(pattern);
    if (match) return match;
  }
  return null;
}

/**
 * Remark plugin to import and process code snippets from external files.
 *
 * @description
 * This plugin enables importing code from external source files into markdown code blocks,
 * with support for:
 * - Smart path resolution: short paths resolve to the current page's code-snippets directory
 * - Extracting specific regions using `#region` markers
 * - Hiding lines with `[!code hide]` or `[!code hide:N]` sentinels
 * - Marking diff additions/removals with `[!code ++]`, `[!code ++:N]`, `[!code --]`, or `[!code --:N]`
 * - Passing diff line numbers via metadata to custom Shiki transformer for styling
 * - Stripping region markers and sentinel comments from output
 *
 * @param {Options} options - Configuration options
 * @param {string} options.dir - Base directory to resolve snippet file paths from
 *
 * @returns {Function} A remark transformer function
 *
 * @example
 * Short path (resolves to current page's code-snippets directory):
 * In file: content/docs/core-concepts/schemas/accounts-and-migrations.mdx
 * ```ts snippet=schema.ts
 * ```
 * Resolves to: content/docs/code-snippets/core-concepts/schemas/accounts-and-migrations/schema.ts
 *
 * @example
 * Full path (resolves from base code-snippets directory):
 * ```ts snippet=core-concepts/covalues/cofeeds/index.ts
 * ```
 * Resolves to: content/docs/code-snippets/core-concepts/covalues/cofeeds/index.ts
 *
 * @example
 * With region extraction (hash syntax):
 * ```tsx snippet=index.ts#PropsType
 * ```
 *
 * @example
 * With region extraction (key-value syntax):
 * ```ts snippet=index.ts region=LoginFunction
 * ```
 *
 * @example
 * Compact syntax (no snippet= prefix):
 * ```ts components/Button.tsx#PropsType
 * ```
 *
 * @remarks
 * Supported languages: `ts`, `tsx`, `svelte`
 *
 * Path resolution:
 * - Short paths (no `/`) resolve to the current page's code-snippets subdirectory first, then fall back to base
 * - Paths with `/` always resolve from the base code-snippets directory
 * - Example: In `docs/core-concepts/schemas/accounts-and-migrations.mdx`, `snippet=schema.ts` looks for
 *   `code-snippets/core-concepts/schemas/accounts-and-migrations/schema.ts` first
 *
 * Supported sentinels:
 * - `// [!code hide]` or `// [!code hide:N]` - Hide lines from output
 * - `// [!code ++]` or `// [!code ++:N]` - Mark next N lines (or 1 line) as additions (green background)
 * - `// [!code --]` or `// [!code --:N]` - Mark next N lines (or 1 line) as removals (red background)
 * - `// #region Name` / `// #endregion` - Define extractable regions
 * - `// @ts-expect-error`, `// @ts-ignore`, `// @ts-nocheck` - Automatically hidden
 * - HTML-style comments (`<!-- ... -->`) also supported for Svelte files
 * - JSX comments (`{/* ... `) also supported for React/TSX files
 *
 * Diff sentinels are detected, removed from output, and the affected line numbers are stored in
 * node.data.hProperties for processing by the custom Shiki transformer. This approach works with
 * any syntax including JSX where inline comment markers would be invalid.
 */
export function codeSnippets(options: Options): Function {
  return (tree: any, file: any) => {
    visit(tree, "code", (node: any) => {
      const allowedLangs = ["ts", "tsx", "svelte"];
      if (!allowedLangs.includes(node.lang)) return;

      const params = parseMeta(node.meta);
      if (!params.snippet) return;

      const filePath = resolveSnippetPath(
        params.snippet,
        options.dir,
        file.path,
      );

      if (!fs.existsSync(filePath)) {
        throw new Error(`Snippet not found: ${filePath}`);
      }

      let content = fs.readFileSync(filePath, "utf8");

      if (params.region) {
        content = extractRegion(content, params.region, filePath);
      }

      const { clean, highlights, additions, deletions } =
        processAnnotations(content);

      node.value = clean;
      node.data ||= {};
      node.data.hProperties ||= {};

      if (highlights.length) {
        node.data.hProperties.highlight = highlights.join(",");
      }
      if (additions.length) {
        node.data.hProperties.add = additions.join(",");
      }
      if (deletions.length) {
        node.data.hProperties.del = deletions.join(",");
      }
    });
  };
}

/**
 * Resolves a snippet path with smart path resolution.
 *
 * For short paths (just filename), tries to resolve relative to the current MDX file's
 * corresponding code-snippets directory first, then falls back to the base snippets directory.
 *
 * Example:
 * - MDX file: content/docs/core-concepts/schemas/accounts-and-migrations.mdx
 * - Snippet: schema.ts
 * - First try: content/docs/code-snippets/core-concepts/schemas/accounts-and-migrations/schema.ts
 * - Fallback: content/docs/code-snippets/schema.ts
 *
 * For paths with directories, only tries relative to base snippets directory.
 */
function resolveSnippetPath(
  snippetPath: string,
  snippetsBaseDir: string,
  currentFilePath: string | undefined,
): string {
  // If the snippet path contains a directory separator, it's a full path
  // Just resolve it relative to the base snippets directory
  if (snippetPath.includes("/")) {
    return path.join(snippetsBaseDir, snippetPath);
  }

  // If we don't have the current file path, fall back to base directory
  if (!currentFilePath) {
    return path.join(snippetsBaseDir, snippetPath);
  }

  // Derive the corresponding snippets subdirectory from the MDX file path
  // Example: content/docs/core-concepts/schemas/accounts-and-migrations.mdx
  //       -> content/docs/code-snippets/core-concepts/schemas/accounts-and-migrations/
  const parsedPath = path.parse(currentFilePath);
  const dirPath = parsedPath.dir;

  // Find the '/docs/' or '/docs' part and replace it with '/code-snippets/'
  // This handles both Unix and Windows paths
  const docsMatch = dirPath.match(/(.*[/\\]docs)([/\\].*)?$/);
  if (!docsMatch) {
    // If we can't find /docs/, fall back to base directory
    return path.join(snippetsBaseDir, snippetPath);
  }

  const beforeDocs = docsMatch[1]; // e.g., "content/docs"
  const afterDocs = docsMatch[2] || ""; // e.g., "/core-concepts/schemas"

  // Build: content/docs/code-snippets/core-concepts/schemas/accounts-and-migrations
  const snippetsSubdir = beforeDocs + path.sep + "code-snippets" + afterDocs;
  const fullSnippetsDir = path.join(snippetsSubdir, parsedPath.name);

  // Try the local snippets directory first
  const localPath = path.join(fullSnippetsDir, snippetPath);
  if (fs.existsSync(localPath)) {
    return localPath;
  }

  // Fall back to the base snippets directory
  return path.join(snippetsBaseDir, snippetPath);
}

function parseMeta(meta: string | undefined) {
  const result: Record<string, string> = {};
  if (!meta) return result;

  // Remove twoslash keyword if present (backwards compatibility)
  meta = meta.replace(/\btwoslash\b/g, "").trim();

  // Compact form without snippet= prefix: examples/foo.ts#Bar or test/example.tsx#Region
  const direct = meta.match(/^([^#\s]+?\.\w+)(?:#([\w\-]+))?$/);
  if (direct) {
    result.snippet = direct[1];
    if (direct[2]) result.region = direct[2];
    return result;
  }

  // Key-value form: snippet=path/to/file.ts or snippet=path/to/file.ts#Region or snippet=path region=Region
  meta.split(/\s+/).forEach((part) => {
    const [key, val] = part.split("=");
    if (key && val) {
      // Handle snippet=path/to/file.ts#Region syntax
      if (key === "snippet" && val.includes("#")) {
        const [filePath, region] = val.split("#");
        result.snippet = filePath;
        result.region = region;
      } else {
        result[key] = val;
      }
    }
  });
  return result;
}

function extractRegion(
  content: string,
  region: string,
  filePath: string,
): string {
  const regionPatterns = [
    `${COMMENT_STYLES.jsStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsStyle("#endregion")}`,
    `${COMMENT_STYLES.htmlStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.htmlStyle("#endregion")}`,
    `${COMMENT_STYLES.jsxStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsxStyle("#endregion")}`,
  ];

  for (const pattern of regionPatterns) {
    const match = content.match(new RegExp(pattern, "m"));
    if (match) {
      return stripRegionMarkers(match[0], region);
    }
  }

  throw new Error(`Region "${region}" not found in ${filePath}`);
}

function stripRegionMarkers(source: string, region: string) {
  const markerPatterns = [
    [
      new RegExp(`${COMMENT_STYLES.jsStyle(`#region\\s*${region}`)}`),
      new RegExp(COMMENT_STYLES.jsStyle("#endregion")),
    ],
    [
      new RegExp(COMMENT_STYLES.htmlStyle(`#region\\s*${region}`)),
      new RegExp(COMMENT_STYLES.htmlStyle("#endregion")),
    ],
    [
      new RegExp(COMMENT_STYLES.jsxStyle(`#region\\s*${region}`)),
      new RegExp(COMMENT_STYLES.jsxStyle("#endregion")),
    ],
  ];

  let result = source;
  for (const [startPattern, endPattern] of markerPatterns) {
    result = result.replace(startPattern, "").replace(endPattern, "");
  }
  return result.trim();
}

function processAnnotations(source: string) {
  const lines = source.split("\n");
  const highlights: number[] = [];
  const additions: number[] = [];
  const deletions: number[] = [];

  const hideWithCountPatterns = createMultiStylePattern(
    `\\[!code\\s*hide:\\s*(\\d+)\\s*\\]`,
  );
  const hidePatterns = createMultiStylePattern(`\\[!code\\s*hide\\s*\\]`);
  const diffWithCountPatterns = createMultiStylePattern(
    `\\[!code\\s*(\\+\\+|--):\\s*(\\d+)\\s*\\]`,
  );
  const diffPatterns = createMultiStylePattern(
    `\\[!code\\s*(\\+\\+|--)\\s*\\]`,
  );
  const regionPatterns = createMultiStylePattern(`#(?:region|endregion)`);
  const tsDirectivePatterns = createMultiStylePattern(
    `@ts-(?:expect-error|ignore|nocheck)`,
  );

  let pendingDiff: { type: "add" | "remove"; count: number } | null = null;
  let hideCount = 0;

  const cleanLines: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Check for hide with count: [!code hide:N]
    const hideMatch = matchesAnyPattern(line, hideWithCountPatterns);
    if (hideMatch) {
      hideCount = +hideMatch[1];
      continue; // Skip sentinel line
    }

    // Check for single line hide: [!code hide]
    if (matchesAnyPattern(line, hidePatterns)) {
      hideCount = 1;
      continue; // Skip sentinel line
    }

    // Check for diff markers with count: [!code ++:N] or [!code --:N]
    const bracketDiffWithCountMatch = matchesAnyPattern(
      line,
      diffWithCountPatterns,
    );
    if (bracketDiffWithCountMatch) {
      const [, op, countStr] = bracketDiffWithCountMatch;
      pendingDiff = { type: op === "++" ? "add" : "remove", count: +countStr };
      continue; // Skip sentinel line
    }

    // Check for single line diff markers: [!code ++] or [!code --]
    const bracketDiffMatch = matchesAnyPattern(line, diffPatterns);
    if (bracketDiffMatch) {
      const [, op] = bracketDiffMatch;
      pendingDiff = { type: op === "++" ? "add" : "remove", count: 1 };
      continue; // Skip sentinel line
    }

    // Skip region markers
    if (matchesAnyPattern(line, regionPatterns)) {
      continue;
    }

    // Skip TypeScript compiler directives
    if (matchesAnyPattern(line, tsDirectivePatterns)) {
      continue;
    }

    // Apply hide count (skip these lines entirely)
    if (hideCount > 0) {
      hideCount--;
      continue;
    }

    // Add line to clean output
    cleanLines.push(line);
    const cleanLineNum = cleanLines.length;

    // Track diff lines by their output line number
    if (pendingDiff) {
      if (pendingDiff.type === "add") {
        additions.push(cleanLineNum);
      } else {
        deletions.push(cleanLineNum);
      }
      pendingDiff.count--;
      if (pendingDiff.count === 0) pendingDiff = null;
    }
  }

  if (cleanLines[cleanLines.length - 1] == "") cleanLines.pop(); // If the last line is blank, pop it off.
  return { clean: cleanLines.join("\n"), highlights, additions, deletions };
}
