// llms-full.mjs
import { promises as fs } from "fs";
import path from "path";
import { DOC_SECTIONS } from "./utils/config.mjs";
import { writeDocsFile } from "./utils/index.mjs";
import { mdxToMd } from "./utils/mdx-processor.mjs";

const exclude = [/\/upgrade\//];
const CWD = process.cwd();

async function readMdxContent(url) {
  try {
    // Special case for the intro page
    if (url === "/docs") {
      const introPath = path.join(CWD, "content/docs/index.mdx");
      return mdxToMd(introPath);
    }

    const relativePath = url.replace(/^\/docs\/?/, "");
    const fullPath = path.join(CWD, "content/docs", relativePath);

    if (exclude.some((pattern) => pattern.test(fullPath))) return null;

    try {
      const stats = await fs.stat(fullPath);
      if (stats.isDirectory()) {
        const files = await fs.readdir(fullPath);
        const mdxFiles = files.filter(
          (f) => f.endsWith(".mdx") && !exclude.some((p) => p.test(f))
        );
        if (mdxFiles.length === 0) return null;

        const contents = await Promise.all(
          mdxFiles.map((file) => mdxToMd(path.join(fullPath, file)))
        );
        return contents.join("\n\n---\n\n");
      }
    } catch (err) {
      if (err.code !== "ENOENT") throw err; // Re-throw unexpected errors
    }

    // Fallback to reading as a single .mdx file
    return await mdxToMd(fullPath + ".mdx").catch((err) => {
      if (err.code !== "ENOENT") console.warn(`Error processing ${url}:`, err);
      return null;
    });

  } catch (error) {
    console.warn(`Failed to read MDX content for ${url}:`, error);
    return null;
  }
}

async function loadDirectoryContent(dirPath) {
  const results = [];
  const entries = await fs.readdir(dirPath, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      results.push(...(await loadDirectoryContent(fullPath)));
    } else {
      const content = await fs.readFile(fullPath, "utf-8");
      results.push({ filepath: fullPath, content });
    }
  }
  return results;
}

// Read the music player example source code (generally the most cutting-edge and append it to the output for llms-full.txt
// TODO: consider adding a server-side example too.
async function appendMusicExample(output) {
  try {
    const examplePath = path.resolve(CWD, "../../examples/music-player/src");
    const files = await loadDirectoryContent(examplePath);

    output.push("## Music Example\n");
    for (const file of files) {
      const relativePath = path.relative(examplePath, file.filepath);
      const lang = path.extname(relativePath).substring(1);
      output.push(`### ${relativePath}\n`);
      output.push(`\`\`\`${lang}\n${file.content}\n\`\`\`\n`);
    }
  } catch (error) {
    console.warn("Could not read music example:", error.message);
  }
}

async function generateDocs() {
  const output = ["# Jazz\n"];

  for (const section of DOC_SECTIONS) {
    output.push(`## ${section.title}\n`);
    for (const page of section.pages) {
      if (!page.url) continue;
      output.push(`### ${page.title}`);
      const content = await readMdxContent(page.url);
      if (content) {
        output.push(content);
      }
      output.push("\n");
    }
  }

  output.push("## Resources\n");
  output.push("- [Documentation](https://jazz.tools/docs): Detailed documentation about Jazz");
  output.push("- [Examples](https://jazz.tools/examples): Code examples and tutorials\n");

  const outputWithExample = [...output];
  await appendMusicExample(outputWithExample);

  await writeDocsFile("llms.txt", output.join("\n"));
  await writeDocsFile("llms-full.txt", outputWithExample.join("\n"));
}

async function main() {
  console.log("Generating detailed LLM docs...");
  await generateDocs();
  console.log("Docs generated successfully.");
}

main().catch(console.error);