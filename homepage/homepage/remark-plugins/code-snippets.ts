import fs from "node:fs";
import path from "node:path";
import { visit } from "unist-util-visit";

interface Options {
  dir: string;
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
        let regionRegex = new RegExp(
          `//\\s*#region\\s*${params.region}[\\s\\S]*?//\\s*#endregion`,
          "m"
        );
        let match = content.match(regionRegex);

        if (!match) {
          regionRegex = new RegExp(
            `<!--\\s*#region\\s*${params.region}\\s*-->[\\s\\S]*?<!--\\s*#endregion\\s*-->`,
            "m"
          );
          match = content.match(regionRegex);
        }

        if (!match) {
          throw new Error(`Region "${params.region}" not found in ${filePath}`);
        }
        content = stripRegionMarkers(match[0], params.region);
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

function stripRegionMarkers(source: string, region: string) {
  return source
    .replace(new RegExp(`//\\s*#region\\s*${region}`), "")
    .replace(/\/\/\s*#endregion/, "")
    .replace(new RegExp(`<!--\\s*#region\\s*${region}\\s*-->`), "")
    .replace(/<!--\s*#endregion\s*-->/, "")
    .trim();
}

function processAnnotations(source: string) {
  const lines = source.split("\n");
  const highlights: number[] = [];
  const diff: { line: number; type: "add" | "remove" }[] = [];

  let inHighlightBlock = false;
  let pendingDiff: { type: "add" | "remove"; count: number } | null = null;
  let highlightNextLine = false;
  let diffNextLine: "add" | "remove" | null = null;
  let hideCount = 0;
  let hideNextLine = false;

  const cleanLines: string[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    const hideMatch = line.match(/^\s*\/\/\s*\[!code\s*hide:\s*(\d+)\s*\]/)
      || line.match(/^\s*<!--\s*\[!code\s*hide:\s*(\d+)\s*\]\s*-->/);
    if (hideMatch) {
      hideCount = +hideMatch[1];
      continue; // Skip sentinel line
    }

    if (/^\s*\/\/\s*\[!code\s*hide\s*\]/.test(line) || /^\s*<!--\s*\[!code\s*hide\s*\]\s*-->/.test(line)) {
      hideNextLine = true;
      continue; // Skip sentinel line
    }

    const bracketDiffMatch = line.match(/^\s*\/\/\s*\[!code\s*(\+\+|--):\s*(\d+)\s*\]/)
      || line.match(/^\s*<!--\s*\[!code\s*(\+\+|--):\s*(\d+)\s*\]\s*-->/);
    if (bracketDiffMatch) {
      const [, op, countStr] = bracketDiffMatch;
      pendingDiff = { type: op === "++" ? "add" : "remove", count: +countStr };
      continue; // Skip sentinel line
    }

    if (/^\s*\/\/\s*#region/.test(line) || /^\s*\/\/\s*#endregion/.test(line)) {
      continue; // Skip JS/TS region markers
    }

    if (/^\s*<!--\s*#region/.test(line) || /^\s*<!--\s*#endregion/.test(line)) {
      continue; // Skip Svelte/HTML region markers
    }

    if (/^\s*\/\/\s*@ts-(expect-error|ignore|nocheck)/.test(line)) {
      continue; // Skip TypeScript compiler directives
    }

    if (hideCount > 0) {
      hideCount--;
      continue;
    }

    if (hideNextLine) {
      hideNextLine = false;
      continue;
    }

    if (pendingDiff) {
      cleanLines.push(line);
      const cleanLineNum = cleanLines.length;
      diff.push({ line: cleanLineNum, type: pendingDiff.type });

      pendingDiff.count--;
      if (pendingDiff.count === 0) pendingDiff = null;
      continue;
    }

    if (diffNextLine) {
      cleanLines.push(line);
      const cleanLineNum = cleanLines.length;
      diff.push({ line: cleanLineNum, type: diffNextLine });
      diffNextLine = null;
      continue;
    }

    cleanLines.push(line);
    const cleanLineNum = cleanLines.length;

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
