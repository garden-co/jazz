import { readFileSync, writeFileSync } from "node:fs";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2).filter((argument) => argument !== "--");

if (args.includes("--help") || args.length === 0) {
  console.log(`Usage: node scripts/render-skill-eval-results.mjs <report.json> [output.html]

Embeds a skill-evaluation report in the standalone HTML results template.`);
  process.exit(args.includes("--help") ? 0 : 1);
}

const reportPath = resolve(args[0]);
const extension = extname(reportPath);
const reportStem = extension ? reportPath.slice(0, -extension.length) : reportPath;
const outputPath = resolve(args[1] ?? `${reportStem}.html`);
const report = JSON.parse(readFileSync(reportPath, "utf8"));

if (!report.summary?.routing || !report.behavior?.baseline || !report.behavior?.loaded) {
  throw new Error("Report must contain routing, baseline behavior, and loaded behavior results");
}

const templatePath = join(packageRoot, "skill-evals", "results-template.html");
const template = readFileSync(templatePath, "utf8");
const serialized = JSON.stringify(report).replaceAll("<", "\\u003c");
const html = template.replace(
  /(<script id="eval-report" type="application\/json">)[\s\S]*?(<\/script>)/u,
  `$1${serialized}$2`,
);

if (html === template) throw new Error(`Missing embedded report marker in ${templatePath}`);

writeFileSync(outputPath, html);
console.log(outputPath);
