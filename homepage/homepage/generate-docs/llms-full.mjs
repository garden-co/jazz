import { promises as fs } from "fs";
import path from "path";
import { join } from "path";
import { readFile, readdir } from "fs/promises";
import { Deserializer } from "typedoc";
import { DOC_SECTIONS } from "./utils/config.mjs";
import { loadTypedocFiles, writeDocsFile } from "./utils/index.mjs";

function formatType(type) {
  if (!type) return "unknown";

  // Handle type aliases and references
  if (type.type === "reference") {
    const name = type.package ? `${type.package}.${type.name}` : type.name;
    return (
      name +
      (type.typeArguments
        ? `<${type.typeArguments.map(formatType).join(", ")}>`
        : "")
    );
  }

  // Handle union types
  if (type.type === "union") {
    return type.types.map(formatType).join(" | ");
  }

  // Handle array types
  if (type.type === "array") {
    return `${formatType(type.elementType)}[]`;
  }

  // Handle basic types
  if (type.type === "intrinsic" || type.type === "literal") {
    return typeof type.value !== "undefined"
      ? JSON.stringify(type.value)
      : type.name;
  }

  // Handle tuple types
  if (type.type === "tuple") {
    return `[${type.elements.map(formatType).join(", ")}]`;
  }

  // Handle intersection types
  if (type.type === "intersection") {
    return type.types.map(formatType).join(" & ");
  }

  // Handle template literal types
  if (type.type === "template-literal") {
    return `\`${type.head}${type.tail.map((t) => `\${${formatType(t[0])}}${t[1]}`).join("")}\``;
  }

  // Handle reflection types (object types and function types)
  if (type.type === "reflection") {
    if (type.declaration.signatures) {
      const sig = type.declaration.signatures[0];
      const params =
        sig.parameters
          ?.map(
            (p) =>
              `${p.name}${p.flags?.isOptional ? "?" : ""}: ${formatType(p.type)}`,
          )
          .join(", ") || "";
      return `(${params}) => ${formatType(sig.type)}`;
    }

    if (type.declaration.children) {
      return (
        "{ " +
        type.declaration.children
          .map((child) => {
            const optional = child.flags?.isOptional ? "?" : "";
            return `${child.name}${optional}: ${formatType(child.type)}`;
          })
          .join("; ") +
        " }"
      );
    }
  }

  // Handle query types
  if (type.type === "query") {
    return `typeof ${formatType(type.queryType)}`;
  }

  // Handle conditional types
  if (type.type === "conditional") {
    return `${formatType(type.checkType)} extends ${formatType(type.extendsType)} ? ${formatType(type.trueType)} : ${formatType(type.falseType)}`;
  }

  // Handle index access types
  if (type.type === "indexedAccess") {
    return `${formatType(type.objectType)}[${formatType(type.indexType)}]`;
  }

  // Handle mapped types
  if (type.type === "mapped") {
    const readonly = type.readonlyModifier === "+" ? "readonly " : "";
    const optional = type.optionalModifier === "+" ? "?" : "";
    return `{ ${readonly}[${type.parameter} in ${formatType(type.parameterType)}]${optional}: ${formatType(type.templateType)} }`;
  }

  // Handle type operators
  if (type.type === "typeOperator") {
    return `${type.operator} ${formatType(type.target)}`;
  }

  // Handle predicate types
  if (type.type === "predicate") {
    return `${type.name} is ${formatType(type.targetType)}`;
  }

  // Handle inferred types
  if (type.type === "inferred") {
    return `infer ${type.name}`;
  }

  // Handle rest types
  if (type.type === "rest") {
    return `...${formatType(type.elementType)}`;
  }

  // Handle unknown types with more detail
  if (type.toString) {
    return type.toString();
  }

  return "unknown";
}

function formatComment(comment) {
  if (!comment) return "";

  let text =
    comment.summary
      ?.map((part) => part.text)
      .join("")
      .trim() || "";

  // Add parameter descriptions if available
  if (comment.blockTags) {
    const params = comment.blockTags
      .filter((tag) => tag.tag === "@param")
      .map((tag) => {
        const paramName = tag.param;
        let description = "";
        let codeExample = "";

        tag.content.forEach((part) => {
          if (part.kind === "code") {
            // Don't wrap in code blocks since examples are already wrapped
            codeExample += "\n" + part.text + "\n";
          } else {
            description += part.text;
          }
        });

        return `- ${paramName}: ${description.trim()}${codeExample}`;
      });

    if (params.length > 0) {
      text += "\n\nParameters:\n" + params.join("\n");
    }

    // Add remarks if available
    const remarks = comment.blockTags
      .filter((tag) => tag.tag === "@remarks")
      .map((tag) =>
        tag.content
          .map((part) => part.text)
          .join("")
          .trim(),
      );

    if (remarks.length > 0) {
      text += "\n\nRemarks:\n" + remarks.join("\n");
    }

    // Add examples
    const examples = comment.blockTags
      .filter((tag) => tag.tag === "@example")
      .map((tag) =>
        tag.content
          .map((part) => {
            if (part.kind === "code") {
              // Don't wrap in code blocks since examples are already wrapped
              return "\n" + part.text + "\n";
            }
            return part.text;
          })
          .join("")
          .trim(),
      );

    if (examples.length > 0) {
      text += "\n\nExamples:\n" + examples.join("\n");
    }
  }

  return text;
}

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

async function generateDetailedDocs(docs) {
  const output = [];
  const deserializer = new Deserializer();

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
  const docs = await loadTypedocFiles();
  await generateDetailedDocs(docs);
}

main().catch(console.error);
