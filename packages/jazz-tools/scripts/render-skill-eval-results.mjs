import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { basename, dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const args = process.argv.slice(2).filter((argument) => argument !== "--");

if (args.includes("--help") || args.length === 0) {
  console.log(`Usage: node scripts/render-skill-eval-results.mjs <report.json> [output.html] [history-dir]

Embeds a skill-evaluation report and selectable historical runs in the standalone HTML results template.`);
  process.exit(args.includes("--help") ? 0 : 1);
}

const reportPath = resolve(args[0]);
const extension = extname(reportPath);
const reportStem = extension ? reportPath.slice(0, -extension.length) : reportPath;
const outputPath = resolve(args[1] ?? `${reportStem}.html`);
const historyDir = resolve(args[2] ?? join(dirname(reportPath), "history"));
function selectionResult(actual, expected) {
  const exact =
    actual.length === expected.length && expected.every((skill) => actual.includes(skill));
  const passed =
    expected.length === 0 ? actual.length === 0 : expected.every((skill) => actual.includes(skill));
  return {
    passed,
    exact,
    extraSkills: actual.filter((skill) => !expected.includes(skill)),
  };
}

function rescoreRouting(report) {
  const routing = report.routing.map((result) => ({
    ...result,
    ...selectionResult(result.selectedSkills, result.expectedSkills),
  }));
  return {
    ...report,
    summary: {
      ...report.summary,
      routing: {
        passed: routing.filter((result) => result.passed).length,
        exact: routing.filter((result) => result.exact).length,
        total: routing.length,
      },
    },
    routing,
  };
}

const report = rescoreRouting(JSON.parse(readFileSync(reportPath, "utf8")));

if (!report.summary?.routing || !report.behavior?.baseline || !report.behavior?.loaded) {
  throw new Error("Report must contain routing, baseline behavior, and loaded behavior results");
}

const templatePath = join(packageRoot, "skill-evals", "results-template.html");
const template = readFileSync(templatePath, "utf8");
const serialized = JSON.stringify(report).replaceAll("<", "\\u003c");
const history = existsSync(historyDir)
  ? readdirSync(historyDir)
      .filter((file) => file.endsWith(".json"))
      .sort()
      .map((file) => {
        const historical = rescoreRouting(JSON.parse(readFileSync(join(historyDir, file), "utf8")));
        if (!historical.summary?.routing) throw new Error(`Invalid historical report ${file}`);
        return { ...historical, filename: basename(file) };
      })
      .sort((left, right) => String(left.generatedAt).localeCompare(String(right.generatedAt)))
  : [];
const serializedHistory = JSON.stringify(history).replaceAll("<", "\\u003c");
const reportHtml = template.replace(
  /(<script id="eval-report" type="application\/json">)[\s\S]*?(<\/script>)/u,
  `$1${serialized}$2`,
);
if (reportHtml === template) throw new Error(`Missing embedded report marker in ${templatePath}`);
const html = reportHtml.replace(
  /(<script id="eval-history" type="application\/json">)[\s\S]*?(<\/script>)/u,
  `$1${serializedHistory}$2`,
);
if (html === reportHtml) throw new Error(`Missing embedded history marker in ${templatePath}`);

writeFileSync(outputPath, html);
console.log(outputPath);
