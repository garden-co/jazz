// @ts-check
import fs from "node:fs";
import path from "node:path";

const COMMENT_STYLES = {
  jsStyle: (pattern) => `//\\s*${pattern}`,
  htmlStyle: (pattern) => `<!--\\s*${pattern}\\s*-->`,
  jsxStyle: (pattern) => `\\{\\s*\\/\\*\\s*${pattern}\\s*\\*\\/\\s*\\}`,
};

function createMultiStylePattern(pattern, flags = "") {
  return [
    new RegExp(`^\\s*${COMMENT_STYLES.jsStyle(pattern)}`, flags),
    new RegExp(`^\\s*${COMMENT_STYLES.htmlStyle(pattern)}`, flags),
    new RegExp(`^\\s*${COMMENT_STYLES.jsxStyle(pattern)}`, flags),
  ];
}

function matchesAnyPattern(line, patterns) {
  for (const pattern of patterns) {
    const match = line.match(pattern);
    if (match) return match;
  }
  return null;
}

/**
 * Parses the meta string from a code fence to extract snippet information
 * @param {string | undefined} meta - The meta string after the language identifier
 * @returns {Record<string, string>} Object with snippet path and optional region
 */
function parseMeta(meta) {
  const result = {};
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

/**
 * Resolves a snippet path with smart path resolution.
 * For short paths (just filename), tries to resolve relative to the current MDX file's
 * corresponding code-snippets directory first, then falls back to the base snippets directory.
 */
function resolveSnippetPath(snippetPath, snippetsBaseDir, currentFilePath) {
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
  const parsedPath = path.parse(currentFilePath);
  const dirPath = parsedPath.dir;

  // Find the '/docs/' or '/docs' part and replace it with '/code-snippets/'
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

function extractRegion(content, region, filePath) {
  const regionPatterns = [
    `${COMMENT_STYLES.jsStyle(`#\\s*region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsStyle("#\\s*endregion")}`,
    `${COMMENT_STYLES.htmlStyle(`#\\s*region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.htmlStyle("#\\s*endregion")}`,
    `${COMMENT_STYLES.jsxStyle(`#\\s*region\\s*${region}`)}[\\s\\S]*?${COMMENT_STYLES.jsxStyle("#\\s*endregion")}`,
  ];

  for (const pattern of regionPatterns) {
    const match = content.match(new RegExp(pattern, "m"));
    if (match) {
      return stripRegionMarkers(match[0], region);
    }
  }

  throw new Error(`Region "${region}" not found in ${filePath}`);
}

function stripRegionMarkers(source, region) {
  const markerPatterns = [
    [
      new RegExp(`${COMMENT_STYLES.jsStyle(`#\\s*region\\s*${region}`)}`),
      new RegExp(COMMENT_STYLES.jsStyle("#\\s*endregion")),
    ],
    [
      new RegExp(COMMENT_STYLES.htmlStyle(`#\\s*region\\s*${region}`)),
      new RegExp(COMMENT_STYLES.htmlStyle("#\\s*endregion")),
    ],
    [
      new RegExp(COMMENT_STYLES.jsxStyle(`#\\s*region\\s*${region}`)),
      new RegExp(COMMENT_STYLES.jsxStyle("#\\s*endregion")),
    ],
  ];

  let result = source;
  for (const [startPattern, endPattern] of markerPatterns) {
    result = result.replace(startPattern, "").replace(endPattern, "");
  }
  return result.trim();
}

function processAnnotations(source) {
  const lines = source.split("\n");

  const hideWithCountPatterns = createMultiStylePattern(
    `\\[!code\\s*hide:\\s*(\\d+)\\s*\\]`,
  );
  const hidePatterns = createMultiStylePattern(`\\[!code\\s*hide\\s*\\]`);
  const regionPatterns = createMultiStylePattern(`#\\s*(?:region|endregion)`);
  const tsDirectivePatterns = createMultiStylePattern(
    `@ts-(?:expect-error|ignore|nocheck)`,
  );

  let hideCount = 0;
  let hideNextLine = false;

  const cleanLines = [];

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

    // Add line to clean output
    cleanLines.push(line);
  }

  if (cleanLines[cleanLines.length - 1] === "") cleanLines.pop(); // If the last line is blank, pop it off.
  return cleanLines.join("\n");
}

/**
 * Replaces code fences with snippet= syntax with actual code content
 * @param {string} source - The MDX source content
 * @param {string} filePath - The path to the current MDX file
 * @returns {string} The MDX source with code snippets replaced
 */
export function replaceCodeSnippets(source, filePath) {
  const snippetsBaseDir = path.join(process.cwd(), "content/docs/code-snippets");
  
  // Regex to match code fences with snippet syntax
  // Matches: ```ts snippet=path/to/file.ts or ```tsx path/to/file.ts#Region
  const codeFenceRegex = /^([ \t]*)```(\w+)\s+([^\n]+)\n([\s\S]*?)^[ \t]*```/gm;

  return source.replace(codeFenceRegex, (match, indent, lang, meta, content) => {
    const params = parseMeta(meta);
    
    // If no snippet parameter, leave it unchanged
    if (!params.snippet) {
      return match;
    }

    try {
      const snippetPath = resolveSnippetPath(params.snippet, snippetsBaseDir, filePath);

      if (!fs.existsSync(snippetPath)) {
        console.warn(`Snippet not found: ${snippetPath}`);
        return match;
      }

      let fileContent = fs.readFileSync(snippetPath, "utf8");

      if (params.region) {
        fileContent = extractRegion(fileContent, params.region, snippetPath);
      }

      const cleanContent = processAnnotations(fileContent);

      const indentedContent = cleanContent
        .split("\n")
        .map((line) => (line.length ? `${indent}${line}` : indent))
        .join("\n");

      // Return the code fence with the actual content
      return `${indent}\`\`\`${lang}\n${indentedContent}\n${indent}\`\`\``;
    } catch (error) {
      console.warn(`Error processing snippet: ${error.message}`);
      return match;
    }
  });
}

