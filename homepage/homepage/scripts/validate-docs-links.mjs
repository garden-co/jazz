#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fromMarkdown } from "mdast-util-from-markdown";
import { mdxjs } from "micromark-extension-mdxjs";
import { mdxFromMarkdown } from "mdast-util-mdx";
import { visit } from "unist-util-visit";
import GithubSlugger from "github-slugger";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Framework pattern for stripping from headers
const FRAMEWORK_PATTERN = /\s*\[\!framework=([a-zA-Z0-9,_-]+)\]\s*$/;

// Known frameworks
const FRAMEWORKS = ["react", "react-native", "react-native-expo", "svelte", "vanilla"];

/**
 * Generates a header ID using the same logic as header-ids.mjs
 */
function generateHeaderId(headerText) {
  const slugs = new GithubSlugger();
  slugs.reset();

  // Remove framework visibility markers
  const match = headerText.match(FRAMEWORK_PATTERN);
  const text = match ? headerText.replace(FRAMEWORK_PATTERN, "") : headerText;

  // Generate the slug
  return slugs.slug(text);
}

/**
 * Recursively walks a directory and returns all MDX file paths
 */
function walkDocsDir(dir, baseDir, fileList = []) {
  const entries = fs.readdirSync(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    const relativePath = path.relative(baseDir, fullPath);

    if (entry.isDirectory()) {
      walkDocsDir(fullPath, baseDir, fileList);
    } else if (entry.isFile() && entry.name.endsWith(".mdx")) {
      fileList.push({
        fullPath,
        relativePath,
        name: entry.name,
      });
    }
  }

  return fileList;
}

/**
 * Builds a map of doc paths to file info
 */
function buildFileMap(docsDir) {
  const files = walkDocsDir(docsDir, docsDir);
  const fileMap = new Map();

  for (const file of files) {
    // Normalize path: remove .mdx extension and convert to /docs/... format
    const normalizedPath = file.relativePath
      .replace(/\.mdx$/, "")
      .replace(/\\/g, "/");

    // Store both with and without leading slash
    const paths = [
      `/docs/${normalizedPath}`,
      `/docs/${normalizedPath}/`,
      normalizedPath,
    ];

    // Also handle index files
    if (file.name === "index.mdx") {
      const dirPath = path.dirname(file.relativePath).replace(/\\/g, "/");
      if (dirPath !== ".") {
        paths.push(`/docs/${dirPath}`, `/docs/${dirPath}/`);
      } else {
        paths.push("/docs", "/docs/");
      }
    }

    for (const p of paths) {
      if (!fileMap.has(p)) {
        fileMap.set(p, []);
      }
      fileMap.get(p).push(file);
    }

    // Handle framework-specific files
    const nameWithoutExt = file.name.replace(/\.mdx$/, "");
    if (FRAMEWORKS.includes(nameWithoutExt)) {
      const parentDir = path.dirname(file.relativePath).replace(/\\/g, "/");
      const genericPath = parentDir === "." ? "/docs" : `/docs/${parentDir}`;
      if (!fileMap.has(genericPath)) {
        fileMap.set(genericPath, []);
      }
      fileMap.get(genericPath).push(file);
    }
  }

  return fileMap;
}

/**
 * Extracts text from a heading node, handling JSX, expressions, and inline code
 */
function extractHeadingText(node) {
  let text = "";
  
  visit(node, (child) => {
    if (child.type === "text") {
      text += child.value;
    } else if (child.type === "inlineCode") {
      // Inline code - include the code value (without backticks)
      text += child.value;
    } else if (child.type === "code") {
      // Code blocks (shouldn't appear in headers, but handle just in case)
      text += child.value;
    } else if (child.type === "mdxTextExpression") {
      // Skip JSX expressions - they don't contribute to header ID
    } else if (child.type === "mdxJsxTextElement") {
      // Extract text from JSX elements (like <span>)
      visit(child, (jsxChild) => {
        if (jsxChild.type === "text") {
          text += jsxChild.value;
        } else if (jsxChild.type === "inlineCode") {
          text += jsxChild.value;
        } else if (jsxChild.type === "code") {
          text += jsxChild.value;
        }
      });
    } else if (child.type === "html") {
      // Extract text from HTML (strip tags)
      const textMatch = child.value.match(/>([^<]+)</);
      if (textMatch) {
        text += textMatch[1];
      }
    }
  });

  return text.trim();
}

/**
 * Extracts headers and their IDs from an MDX file
 */
function extractHeaders(filePath) {
  const content = fs.readFileSync(filePath, "utf-8");
  const tree = fromMarkdown(content, {
    extensions: [mdxjs()],
    mdastExtensions: [mdxFromMarkdown()],
  });

  const headers = new Set();

  visit(tree, (node) => {
    if (node.type === "heading") {
      const text = extractHeadingText(node);
      if (text) {
        const id = generateHeaderId(text);
        headers.add(id);
      }
    }
  });

  return headers;
}

/**
 * Checks if a node is inside a code block by walking up the tree
 */
function isInCodeBlock(node, tree) {
  // Simple check: if node itself is code
  if (node.type === "code") {
    return true;
  }

  // Check if inside code-related JSX components
  if (
    node.type === "mdxJsxFlowElement" &&
    (node.name === "CodeGroup" ||
      node.name === "TabbedCodeGroup" ||
      node.name === "TabbedCodeGroupItem" ||
      node.name === "CodeWithInterpolation")
  ) {
    return true;
  }

  // For a more thorough check, we'd need to walk up parents
  // But for now, we'll use a simpler heuristic: check if parent is code
  // This is a limitation but should work for most cases
  return false;
}

/**
 * Finds all code block nodes in the tree
 */
function findCodeBlocks(tree) {
  const codeBlocks = new Set();
  
  visit(tree, (node) => {
    if (node.type === "code") {
      codeBlocks.add(node);
    }
    if (
      node.type === "mdxJsxFlowElement" &&
      (node.name === "CodeGroup" ||
        node.name === "TabbedCodeGroup" ||
        node.name === "TabbedCodeGroupItem" ||
        node.name === "CodeWithInterpolation")
    ) {
      codeBlocks.add(node);
    }
  });

  return codeBlocks;
}

/**
 * Checks if a node is a descendant of any code block
 */
function isDescendantOfCodeBlock(node, codeBlocks, visited = new Set()) {
  if (visited.has(node)) return false;
  visited.add(node);

  // Check if node itself is a code block
  if (codeBlocks.has(node)) {
    return true;
  }

  // For simplicity, we'll check if the node is inside a code block
  // by checking if it shares the same position range
  for (const codeBlock of codeBlocks) {
    if (
      node.position &&
      codeBlock.position &&
      node.position.start.line >= codeBlock.position.start.line &&
      node.position.end.line <= codeBlock.position.end.line
    ) {
      return true;
    }
  }

  return false;
}

/**
 * Extracts all links from an MDX file
 */
function extractLinks(filePath) {
  const content = fs.readFileSync(filePath, "utf-8");
  const tree = fromMarkdown(content, {
    extensions: [mdxjs()],
    mdastExtensions: [mdxFromMarkdown()],
  });

  const codeBlocks = findCodeBlocks(tree);
  const links = [];

  visit(tree, (node) => {
    const inCode = isDescendantOfCodeBlock(node, codeBlocks);

    // Extract markdown links
    if (node.type === "link" && !inCode) {
      const url = node.url;
      if (url && !isExternalLink(url)) {
        const line = node.position?.start?.line || 0;
        links.push({
          url,
          text: extractLinkText(node),
          line,
          type: "markdown",
        });
      }
    }

    // Extract HTML anchor links
    if (node.type === "html" && !inCode) {
      // Skip HTML comments
      if (node.value.trim().startsWith("<!--")) {
        return;
      }
      const htmlMatch = node.value.match(/<a\s+[^>]*href=["']([^"']+)["'][^>]*>/i);
      if (htmlMatch) {
        const url = htmlMatch[1];
        if (url && !isExternalLink(url)) {
          const line = node.position?.start?.line || 0;
          links.push({
            url,
            text: extractHtmlLinkText(node.value),
            line,
            type: "html",
          });
        }
      }
    }

    // Extract JSX anchor links
    if (node.type === "mdxJsxFlowElement" && node.name === "a" && !inCode) {
      const hrefAttr = node.attributes?.find((attr) => attr.name === "href");
      if (hrefAttr && hrefAttr.value) {
        const url =
          typeof hrefAttr.value === "string"
            ? hrefAttr.value
            : hrefAttr.value?.value || "";
        if (url && !isExternalLink(url)) {
          const line = node.position?.start?.line || 0;
          links.push({
            url,
            text: extractJsxLinkText(node),
            line,
            type: "jsx",
          });
        }
      }
    }
  });

  return links;
}

/**
 * Extracts text content from a link node
 */
function extractLinkText(node) {
  let text = "";
  visit(node, (child) => {
    if (child.type === "text") {
      text += child.value;
    }
  });
  return text || node.url;
}

/**
 * Extracts text from HTML anchor tag
 */
function extractHtmlLinkText(html) {
  const match = html.match(/<a[^>]*>([^<]*)<\/a>/i);
  return match ? match[1].trim() : "";
}

/**
 * Extracts text from JSX anchor element
 */
function extractJsxLinkText(node) {
  let text = "";
  if (node.children) {
    for (const child of node.children) {
      if (child.type === "text") {
        text += child.value;
      } else if (child.type === "mdxTextExpression") {
        // Skip expressions
      }
    }
  }
  return text || "";
}

/**
 * Checks if a link is external
 */
function isExternalLink(url) {
  return (
    url.startsWith("http://") ||
    url.startsWith("https://") ||
    url.startsWith("mailto:") ||
    url.startsWith("tel:") ||
    url.startsWith("//")
  );
}

/**
 * Normalizes a link path for lookup
 */
function normalizeLinkPath(url) {
  // Remove query parameters but keep fragment
  const [pathPart, fragment] = url.split("#");
  let normalized = pathPart.split("?")[0];

  // Remove trailing slash
  normalized = normalized.replace(/\/$/, "");

  // Only validate links that start with /docs
  if (normalized.startsWith("/docs")) {
    return { path: normalized, fragment: fragment || null };
  }

  // Pure fragment (same-file anchor)
  if (url.startsWith("#")) {
    return { path: null, fragment: url.substring(1) };
  }

  // Other paths (like /pricing, /examples) are not docs links - skip them
  return { path: null, fragment: null };
}

/**
 * Checks if a path is a framework-specific path and extracts the framework
 */
function extractFrameworkFromPath(path) {
  if (!path.startsWith("/docs/")) {
    return null;
  }
  const parts = path.split("/");
  if (parts.length >= 3 && FRAMEWORKS.includes(parts[2])) {
    return {
      framework: parts[2],
      genericPath: "/docs/" + parts.slice(3).join("/"),
    };
  }
  return null;
}

/**
 * Validates all links in the docs directory
 */
function validateLinks(docsDir) {
  console.log(`Scanning docs directory: ${docsDir}\n`);

  const fileMap = buildFileMap(docsDir);
  const headerMap = new Map();
  const errors = [];
  const warnings = [];

  // Build header map for all files
  console.log("Extracting headers from MDX files...");
  for (const [linkPath, files] of fileMap.entries()) {
    for (const file of files) {
      if (!headerMap.has(file.fullPath)) {
        try {
          const headers = extractHeaders(file.fullPath);
          headerMap.set(file.fullPath, headers);
        } catch (err) {
          console.warn(`Warning: Could not parse ${file.fullPath}: ${err.message}`);
        }
      }
    }
  }

  // Find all MDX files and validate their links
  console.log("Extracting and validating links...\n");
  const allFiles = walkDocsDir(docsDir, docsDir);

  for (const file of allFiles) {
    try {
      const links = extractLinks(file.fullPath);
      const fileHeaders = headerMap.get(file.fullPath) || new Set();

      for (const link of links) {
        const { path: linkPath, fragment } = normalizeLinkPath(link.url);

        // Handle same-file anchors
        if (linkPath === null && fragment) {
          if (!fileHeaders.has(fragment)) {
            errors.push({
              file: file.relativePath,
              line: link.line,
              url: link.url,
              text: link.text,
              error: `Anchor "#${fragment}" not found in file`,
            });
          }
          continue;
        }

        // Validate file path
        if (!linkPath) {
          continue;
        }

        // Check if this is a framework-specific path
        const frameworkInfo = extractFrameworkFromPath(linkPath);
        let targetFiles = fileMap.get(linkPath);
        let resolvedPath = linkPath;
        let isFrameworkWarning = false;

        if (!targetFiles || targetFiles.length === 0) {
          // If framework-specific path not found, try generic path
          if (frameworkInfo) {
            const genericFiles = fileMap.get(frameworkInfo.genericPath);
            if (genericFiles && genericFiles.length > 0) {
              targetFiles = genericFiles;
              resolvedPath = frameworkInfo.genericPath;
              isFrameworkWarning = true;
            }
          }
        }

        if (!targetFiles || targetFiles.length === 0) {
          errors.push({
            file: file.relativePath,
            line: link.line,
            url: link.url,
            text: link.text,
            error: `File not found: ${linkPath}`,
          });
          continue;
        }

        // If we resolved to generic path, add warning
        if (isFrameworkWarning) {
          warnings.push({
            file: file.relativePath,
            line: link.line,
            url: link.url,
            text: link.text,
            warning: `Framework-specific path ${linkPath} not found, but generic path ${resolvedPath} exists`,
          });
        }

        // Validate anchor if present
        if (fragment) {
          let anchorFound = false;
          for (const targetFile of targetFiles) {
            const targetHeaders = headerMap.get(targetFile.fullPath);
            if (targetHeaders && targetHeaders.has(fragment)) {
              anchorFound = true;
              break;
            }
          }

          if (!anchorFound) {
            // Check if it's a framework-specific anchor issue
            // Always check generic path if this is a framework-specific link
            if (frameworkInfo && !isFrameworkWarning) {
              // Framework-specific file exists but anchor not found - check generic
              const genericFiles = fileMap.get(frameworkInfo.genericPath);
              if (genericFiles && genericFiles.length > 0) {
                for (const targetFile of genericFiles) {
                  const targetHeaders = headerMap.get(targetFile.fullPath);
                  if (targetHeaders && targetHeaders.has(fragment)) {
                    warnings.push({
                      file: file.relativePath,
                      line: link.line,
                      url: link.url,
                      text: link.text,
                      warning: `Anchor "#${fragment}" not found in ${linkPath}, but found in generic path ${frameworkInfo.genericPath}`,
                    });
                    anchorFound = true;
                    break;
                  }
                }
              }
            }

            if (!anchorFound) {
              errors.push({
                file: file.relativePath,
                line: link.line,
                url: link.url,
                text: link.text,
                error: `Anchor "#${fragment}" not found in ${resolvedPath}`,
              });
            }
          }
        }
      }
    } catch (err) {
      console.warn(`Warning: Could not process ${file.relativePath}: ${err.message}`);
    }
  }

  return { errors, warnings };
}

/**
 * Main function
 */
function main() {
  const args = process.argv.slice(2);
  const docsDir =
    args[0] || path.resolve(__dirname, "..", "content", "docs");

  if (!fs.existsSync(docsDir)) {
    console.error(`Error: Directory not found: ${docsDir}`);
    process.exit(1);
  }

  const { errors, warnings } = validateLinks(docsDir);

  console.log("\n" + "=".repeat(60));
  
  if (warnings.length > 0) {
    console.log(`⚠️  Found ${warnings.length} warning(s):\n`);
    for (const warning of warnings) {
      console.log(`  File: ${warning.file}:${warning.line}`);
      console.log(`  Link: [${warning.text}](${warning.url})`);
      console.log(`  Warning: ${warning.warning}`);
      console.log();
    }
    console.log();
  }

  if (errors.length === 0) {
    if (warnings.length === 0) {
      console.log("✅ All links are valid!");
    } else {
      console.log("✅ All links are valid (with warnings above)!");
    }
    process.exit(0);
  } else {
    console.log(`❌ Found ${errors.length} broken link(s):\n`);
    for (const error of errors) {
      console.log(`  File: ${error.file}:${error.line}`);
      console.log(`  Link: [${error.text}](${error.url})`);
      console.log(`  Error: ${error.error}`);
      console.log();
    }
    process.exit(1);
  }
}

main();
