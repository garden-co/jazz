import fs from "fs";
import path from "path";
import {
  collectMarkdownFiles,
  buildGroupedIndex,
  replaceBetweenSentinels,
} from "./docs-index-utils.mjs";

const DOCS_ROOT = path.resolve("public/docs");
const TEMPLATE = path.resolve("scripts/user-agents-template.md");
const OUTPUT = path.resolve("public/AGENTS.md");

try {
  const template = fs.readFileSync(TEMPLATE, "utf8");
  const mdFiles = collectMarkdownFiles(DOCS_ROOT);
  const indexContent = buildGroupedIndex(
    mdFiles,
    DOCS_ROOT,
    "https://jazz.tools/docs",
  );

  const output = replaceBetweenSentinels(template, indexContent);

  fs.mkdirSync(path.dirname(OUTPUT), { recursive: true });
  fs.writeFileSync(OUTPUT, output, "utf8");
  console.log(
    `\u2705 Success: Generated user AGENTS.md with ${mdFiles.length} indexed files`,
  );
} catch (err) {
  console.error(`\u274C Error: ${err.message}`);
}
