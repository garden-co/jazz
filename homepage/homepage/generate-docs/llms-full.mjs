import { promises as fs } from "fs";
import path from "path";
import { join } from "path";
import { readFile, readdir } from "fs/promises";
import { DOC_SECTIONS } from "./utils/config.mjs";
import { writeDocsFile } from "./utils/index.mjs";

async function readMdxContent(url) {
  try {
    // Special case for the introduction
    if (url === "/docs") {
      const introPath = path.join(process.cwd(), "content/docs/index.mdx");
      try {
        const content = await fs.readFile(introPath, "utf8");
        // Remove imports and exports
        return content
          .replace(/^import[^\n]*\n/gm, "")
          .replace(/export const metadata[^;]*;/, "")
          .trim();
      } catch (err) {
        if (err.code !== "ENOENT") throw err;
      }
    }

    // Convert URL to file path
    // Remove leading slash and 'docs' from URL
    const relativePath = url.replace(/^\/docs\/?/, "");

    // Base directory for docs
    const baseDir = path.join(process.cwd(), "content/docs");

    // If it's a directory, try to read all framework variants
    const fullPath = path.join(baseDir, relativePath);
    try {
      const stats = await fs.stat(fullPath);
      if (stats.isDirectory()) {
        // Read all MDX files in the directory
        const files = await fs.readdir(fullPath);
        const mdxFiles = files.filter((f) => f.endsWith(".mdx"));

        if (mdxFiles.length === 0) return null;

        // Combine content from all framework variants
        const contents = await Promise.all(
          mdxFiles.map(async (file) => {
            const content = await fs.readFile(
              path.join(fullPath, file),
              "utf8",
            );
            // Remove imports and exports
            const cleanContent = content
              .replace(/^import[^\n]*\n/gm, "")
              .replace(/export const metadata[^;]*;/, "")
              .trim();
            return `### ${path.basename(file, ".mdx")} Implementation\n\n${cleanContent}`;
          }),
        );

        return contents.join("\n\n---\n\n");
      }
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }

    // Try as a single MDX file
    const mdxPath = fullPath + ".mdx";
    try {
      const content = await fs.readFile(mdxPath, "utf8");
      // Remove imports and exports
      return content
        .replace(/^import[^\n]*\n/gm, "")
        .replace(/export const metadata[^;]*;/, "")
        .trim();
    } catch (err) {
      if (err.code !== "ENOENT") throw err;
    }

    console.warn(`Could not find MDX content for ${url} at ${fullPath}`);
    return null;
  } catch (error) {
    console.warn(`Error reading MDX content for ${url}:`, error);
    return null;
  }
}

async function generateDetailedDocs() {
  const output = [];

  // Project title
  output.push("# Jazz\n");

  // Documentation sections with full content
  output.push("## Documentation\n");
  for (const section of DOC_SECTIONS) {
    output.push(`### ${section.title}\n`);

    for (const page of section.pages) {
      output.push(`#### ${page.title}\n`);
      const content = await readMdxContent(page.url);
      if (content) {
        // If the content contains framework-specific implementations, they're already properly formatted
        // Otherwise, just add the content directly
        output.push(content + "\n");
      }
      output.push("\n");
    }
  }

  // Optional section for additional resources
  output.push("## Resources\n\n");
  output.push(
    "- [Documentation](https://jazz.tools/docs): Detailed documentation about Jazz\n",
  );
  output.push(
    "- [Examples](https://jazz.tools/examples): Code examples and tutorials\n",
  );

  const outputWithExamples = [...output];
  await readMusicExample(outputWithExamples);

  await writeDocsFile("llms.txt", output.join("\n"));
  await writeDocsFile("llms-full.txt", outputWithExamples.join("\n"));
}

/**
 * @typedef {Object} FileContent
 * @property {string} filepath - The relative path to the file
 * @property {string} content - The content of the file
 */

/**
 * Recursively loads all files from a directory and its subdirectories
 * @param {string} directoryPath - The path to the directory to load
 * @param {Object} options - Optional configuration
 * @param {string[]} options.exclude - File patterns to exclude (e.g., ['*.md', '*.git'])
 * @param {string} options.encoding - File encoding (default: 'utf-8')
 * @returns {Promise<FileContent[]>} Array of filepath/content pairs
 */
async function loadDirectoryContent(directoryPath, options = {}) {
  const { exclude = [], encoding = "utf-8" } = options;

  async function processDirectory(currentPath) {
    const results = [];

    try {
      const entries = await readdir(currentPath, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = join(currentPath, entry.name);

        if (entry.isDirectory()) {
          // Recursively process subdirectories
          const subDirResults = await processDirectory(fullPath);
          results.push(...subDirResults);
        } else if (entry.isFile()) {
          // Check if file should be excluded
          const shouldExclude = exclude.some((pattern) => {
            if (pattern.startsWith("*.")) {
              const extension = pattern.slice(1);
              return entry.name.endsWith(extension);
            }
            return entry.name === pattern;
          });

          if (!shouldExclude) {
            try {
              const content = await readFile(fullPath, { encoding });
              results.push({
                filepath: fullPath,
                content: content,
              });
            } catch (error) {
              console.error(`Error reading file ${fullPath}:`, error);
            }
          }
        }
      }
    } catch (error) {
      console.error(`Error reading directory ${currentPath}:`, error);
    }

    return results;
  }

  try {
    return await processDirectory(directoryPath);
  } catch (error) {
    throw new Error(`Failed to load directory content: ${error.message}`);
  }
}

async function readMusicExample(output) {
  const files = await loadDirectoryContent(
    path.join(process.cwd(), "../../examples/music-player/src"),
  );

  output.push("## Music Example\n\n");
  for (const file of files) {
    output.push(`### ${file.filepath}\n`);
    output.push(`\`\`\`ts\n${file.content}\n\`\`\`\n`);
  }
}

// Main execution
async function main() {
  console.log("Generating detailed LLM docs...");
  await generateDetailedDocs();
}

main().catch(console.error);
