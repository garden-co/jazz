import fs from "fs";
import path from "path";
import {
  collectMarkdownFiles,
  buildGroupedIndex,
  replaceBetweenSentinels,
} from "./docs-index-utils.mjs";

const DOCS_ROOT = path.resolve("public/docs");
const AGENTS_MD = path.resolve("../../AGENTS.md");

// ---- Run ----
try {
  const mdFiles = collectMarkdownFiles(DOCS_ROOT);
  const indexContent = buildGroupedIndex(
    mdFiles,
    DOCS_ROOT,
    "homepage/homepage/public/docs",
  );

  const text = fs.readFileSync(AGENTS_MD, "utf8");
  const updatedFileContent = replaceBetweenSentinels(text, indexContent);

  fs.writeFileSync(AGENTS_MD, updatedFileContent, "utf8");
  console.log(
    `\u2705 Success: Indexed ${mdFiles.length} files into ${path.basename(AGENTS_MD)}`,
  );
} catch (err) {
  console.error(`\u274C Error: ${err.message}`);
}
