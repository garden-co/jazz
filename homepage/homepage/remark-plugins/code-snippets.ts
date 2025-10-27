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

function matchesAnyPattern(line: string, patterns: RegExp[]): RegExpMatchArray | null {
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
 * - Extracting specific regions using `#region` markers
 * - Hiding lines with `[!code hide]` or `[!code hide:N]` sentinels
 * - Marking diff additions/removals with `[!code ++:N]` or `[!code --:N]`
 * - Stripping region markers and sentinel comments from output
 * 
 * @param {Options} options - Configuration options
 * @param {string} options.dir - Base directory to resolve snippet file paths from
 * 
 * @returns {Function} A remark transformer function
 * 
 * @example
 * Basic usage:
 * ```ts snippet=examples/hello.ts
 * ```
 * 
 * @example
 * With region extraction:
 * ```tsx snippet=components/Button.tsx#PropsType
 * ```
 * 
 * @example
 * With key-value syntax:
 * ```ts snippet=utils/auth.ts region=LoginFunction
 * ```
 * 
 * @remarks
 * Supported languages: `ts`, `tsx`, `svelte`
 * 
 * Supported sentinels:
 * - `// [!code hide]` or `// [!code hide:N]` - Hide lines from output
 * - `// [!code ++:N]` or `// [!code --:N]` - Mark diff additions/removals
 * - `// #region Name` / `// #endregion` - Define extractable regions
 * - `// @ts-expect-error`, `// @ts-ignore`, `// @ts-nocheck` - Automatically hidden
 * - HTML-style comments (`<!-- ... -->`) also supported for Svelte files
 * - JSX comments (curly-brace-slash-star format) also supported for React/TSX files
 * 
 * All sentinel comments are automatically stripped from the final output.
 */
export function codeSnippets(options: Options): Function {
  return (tree: any) => {
    visit(tree, "code", (node: any) => {
      const allowedLangs = ["ts", "tsx", "svelte"];
      if (!allowedLangs.includes(node.lang)) return;

      const params = parseMeta(node.meta);
      if (!params.snippet) return;

      const filePath = path.join(options.dir, params.snippet);

      if (!fs.existsSync(filePath)) {
        throw new Error(`Snippet not found: ${filePath}`);
      }

      let content = fs.readFileSync(filePath, "utf8");

      if (params.region) {
        content = extractRegion(content, params.region, filePath);
      }

      const { clean, highlights, diff } = processAnnotations(content);

      node.value = clean;
      node.data ||= {};
      node.data.hProperties ||= {};

      if (highlights.length) {
        node.data.hProperties.highlight = highlights.join(",");
      }
      if (diff.length) {
        node.data.hProperties.diff = JSON.stringify(diff);
      }
    });
  };
}

function parseMeta(meta: string | undefined) {
  const result: Record<string, string> = {};
  if (!meta) return result;

  // Remove twoslash keyword if present (backwards compatibility)
  meta = meta.replace(/\btwoslash\b/g, '').trim();

  // Compact form: examples/foo.ts#Bar or test/example.tsx#Region
  const direct = meta.match(/^([\w./-]+\.\w+)(?:#([\w-]+))?$/);
  if (direct) {
    result.snippet = direct[1];
    if (direct[2]) result.region = direct[2];
    return result;
  }

  meta.split(/\s+/).forEach((part) => {
    const [key, val] = part.split("=");
    if (key && val) result[key] = val;
  });
  return result;
}

function extractRegion(content: string, region: string, filePath: string): string {
  const regionPatterns = [
    `${COMMENT_STYLES.jsStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsStyle('#endregion')}`,
    `${COMMENT_STYLES.htmlStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.htmlStyle('#endregion')}`,
    `${COMMENT_STYLES.jsxStyle(`#region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsxStyle('#endregion')}`,
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
    [new RegExp(`${COMMENT_STYLES.jsStyle(`#region\\s*${region}`)}`), new RegExp(COMMENT_STYLES.jsStyle('#endregion'))],
    [new RegExp(COMMENT_STYLES.htmlStyle(`#region\\s*${region}`)), new RegExp(COMMENT_STYLES.htmlStyle('#endregion'))],
    [new RegExp(COMMENT_STYLES.jsxStyle(`#region\\s*${region}`)), new RegExp(COMMENT_STYLES.jsxStyle('#endregion'))],
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
  const diff: { line: number; type: "add" | "remove" }[] = [];

  const hideWithCountPatterns = createMultiStylePattern(`\\[!code\\s*hide:\\s*(\\d+)\\s*\\]`);
  const hidePatterns = createMultiStylePattern(`\\[!code\\s*hide\\s*\\]`);
  const diffPatterns = createMultiStylePattern(`\\[!code\\s*(\\+\\+|--):\\s*(\\d+)\\s*\\]`);
  const regionPatterns = createMultiStylePattern(`#(?:region|endregion)`);
  const tsDirectivePatterns = createMultiStylePattern(`@ts-(?:expect-error|ignore|nocheck)`);

  let inHighlightBlock = false;
  let pendingDiff: { type: "add" | "remove"; count: number } | null = null;
  let highlightNextLine = false;
  let diffNextLine: "add" | "remove" | null = null;
  let hideCount = 0;
  let hideNextLine = false;

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
      hideNextLine = true;
      continue; // Skip sentinel line
    }

    // Check for diff markers: [!code ++:N] or [!code --:N]
    const bracketDiffMatch = matchesAnyPattern(line, diffPatterns);
    if (bracketDiffMatch) {
      const [, op, countStr] = bracketDiffMatch;
      pendingDiff = { type: op === "++" ? "add" : "remove", count: +countStr };
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

    // Apply hide count
    if (hideCount > 0) {
      hideCount--;
      continue;
    }

    // Apply hide next line
    if (hideNextLine) {
      hideNextLine = false;
      continue;
    }

    // Handle pending diff
    if (pendingDiff) {
      cleanLines.push(line);
      const cleanLineNum = cleanLines.length;
      diff.push({ line: cleanLineNum, type: pendingDiff.type });

      pendingDiff.count--;
      if (pendingDiff.count === 0) pendingDiff = null;
      continue;
    }

    // Handle diff next line
    if (diffNextLine) {
      cleanLines.push(line);
      const cleanLineNum = cleanLines.length;
      diff.push({ line: cleanLineNum, type: diffNextLine });
      diffNextLine = null;
      continue;
    }

    // Add line to clean output
    cleanLines.push(line);
    const cleanLineNum = cleanLines.length;

    // Apply highlights
    if (highlightNextLine) {
      highlights.push(cleanLineNum);
      highlightNextLine = false;
    }
    if (inHighlightBlock) {
      highlights.push(cleanLineNum);
    }
  }

  return { clean: cleanLines.join("\n"), highlights, diff };
}
