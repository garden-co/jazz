import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const defaultReportPath = join(packageRoot, "skill-evals", "results", "latest.json");

function parseArguments(args) {
  const options = {
    reportPath: defaultReportPath,
    historyDir: null,
    latestPath: null,
    harness: null,
    model: null,
    reasoning: null,
    commit: null,
    allowDirty: false,
    render: true,
  };
  let reportPathSet = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--") continue;
    if (argument === "--help") options.help = true;
    else if (argument === "--history-dir") options.historyDir = resolve(args[++index]);
    else if (argument === "--latest") options.latestPath = resolve(args[++index]);
    else if (argument === "--harness") options.harness = args[++index];
    else if (argument === "--model") options.model = args[++index];
    else if (argument === "--reasoning") options.reasoning = args[++index];
    else if (argument === "--commit") options.commit = args[++index];
    else if (argument === "--allow-dirty") options.allowDirty = true;
    else if (argument === "--no-render") options.render = false;
    else if (!argument.startsWith("-") && !reportPathSet) {
      options.reportPath = resolve(argument);
      reportPathSet = true;
    } else throw new Error(`Unknown argument ${argument}`);
  }

  const resultsDir = dirname(options.reportPath);
  options.historyDir ??= join(resultsDir, "history");
  options.latestPath ??= join(resultsDir, "latest.json");
  return options;
}

function printHelp() {
  console.log(`Usage: node scripts/archive-skill-eval-results.mjs [report.json] [options]

Adds execution and corpus metadata, writes an immutable historical run, updates latest.json, and
renders the standalone HTML dashboard.

Options:
  --model <name>         Model or agent family, for example gpt-5.6-sol
  --harness <name>       Evaluation harness, for example codex-subagents
  --reasoning <name>     Reasoning level, for example high
  --commit <hash>        Override the current Git commit
  --allow-dirty          Archive a run produced from an uncommitted skill or harness state
  --history-dir <path>   Historical-run directory (default: next to latest.json)
  --latest <path>        latest.json destination
  --no-render            Do not regenerate latest.html
  --help                 Show this help
`);
}

function slug(value, fallback) {
  const normalized = String(value ?? fallback)
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[^a-z0-9]+/gu, "-")
    .replace(/^-+|-+$/gu, "");
  return normalized || fallback;
}

function portableTimestamp(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new Error(`Invalid generatedAt timestamp ${value}`);
  return date
    .toISOString()
    .replace(/\.\d{3}Z$/u, "Z")
    .replaceAll(":", "-");
}

function gitOutput(args) {
  return execFileSync("git", args, { cwd: packageRoot, encoding: "utf8" }).trim();
}

function gitMetadata(commitOverride) {
  const full = commitOverride ?? gitOutput(["rev-parse", "HEAD"]);
  const short = full.slice(0, 8);
  const dirty = gitOutput(["status", "--porcelain", "--untracked-files=all", "--", "."]) !== "";
  return { full, short, dirty };
}

function reportCorpus(report) {
  const routing = report.routing.map(({ id, prompt, expectedSkills }) => ({
    id,
    prompt,
    expectedSkills,
  }));
  const behavior = report.behavior.baseline.map(({ id, prompt, expectedSkills, rubric }) => ({
    id,
    prompt,
    expectedSkills,
    rubric,
  }));
  const canonical = JSON.stringify({ routing, behavior });
  return {
    version: 1,
    hash: createHash("sha256").update(canonical).digest("hex").slice(0, 12),
    routingCases: routing.length,
    behaviorCases: behavior.length,
    rubricPoints: behavior.reduce((total, evalCase) => total + evalCase.rubric.length, 0),
  };
}

function collectFiles(directory) {
  if (!existsSync(directory)) return [];
  return readdirSync(directory)
    .flatMap((entry) => {
      const path = join(directory, entry);
      return statSync(path).isDirectory() ? collectFiles(path) : [path];
    })
    .sort();
}

function hashFiles(inputs) {
  const hash = createHash("sha256");
  for (const path of inputs.sort()) {
    hash.update(relative(packageRoot, path));
    hash.update("\0");
    hash.update(readFileSync(path));
    hash.update("\0");
  }
  return hash.digest("hex").slice(0, 12);
}

function sourceHash() {
  return hashFiles(
    [
      join(packageRoot, "skill-evals", "cases.json"),
      ...collectFiles(join(packageRoot, "skills")),
    ].filter((path) => path.endsWith(".json") || path.endsWith(".md")),
  );
}

function harnessHash(report) {
  const runnerPath = resolve(packageRoot, report.runner);
  return hashFiles([
    join(packageRoot, "scripts", "run-skill-evals.mjs"),
    join(packageRoot, "scripts", "eval-skills.mjs"),
    runnerPath,
  ]);
}

function validateReport(report) {
  if (!report.summary?.routing || !report.behavior?.baseline || !report.behavior?.loaded) {
    throw new Error("Report must contain routing, baseline behavior, and loaded behavior results");
  }
  if (!Array.isArray(report.routing) || !Array.isArray(report.behavior.baseline)) {
    throw new Error("Report result collections must be arrays");
  }
}

const options = parseArguments(process.argv.slice(2));
if (options.help) {
  printHelp();
  process.exit(0);
}

const report = JSON.parse(readFileSync(options.reportPath, "utf8"));
validateReport(report);
const git = gitMetadata(options.commit);
if (git.dirty && !options.allowDirty) {
  throw new Error(
    "Refusing to archive results from a dirty jazz-tools worktree. Commit the skill and harness state first, or pass --allow-dirty for an explicitly provisional run.",
  );
}
const runnerVersions = [
  ...new Set((report.runs ?? []).map((run) => run.runnerVersion).filter(Boolean)),
];
const execution = {
  commit: git.short,
  commitFull: git.full,
  dirty: git.dirty,
  sourceHash: sourceHash(),
  harnessHash: harnessHash(report),
  model: options.model ?? report.execution?.model ?? "unknown-model",
  harness: options.harness ?? report.execution?.harness ?? report.runner ?? "unknown-harness",
  reasoning: options.reasoning ?? report.execution?.reasoning ?? "unknown-reasoning",
  runnerVersion:
    runnerVersions.join(", ") || report.execution?.runnerVersion || "unknown-runner-version",
};
report.corpus = reportCorpus(report);
report.execution = execution;

const commitToken = `${slug(execution.commit, "unknown-commit")}${execution.dirty ? "-dirty" : ""}`;
const filename =
  [
    portableTimestamp(report.generatedAt),
    commitToken,
    slug(execution.model, "unknown-model"),
    slug(execution.harness, "unknown-harness"),
    slug(execution.reasoning, "unknown-reasoning"),
  ].join("-") + ".json";
const archivePath = join(options.historyDir, filename);
const output = `${JSON.stringify(report, null, 2)}\n`;

mkdirSync(options.historyDir, { recursive: true });
if (existsSync(archivePath) && readFileSync(archivePath, "utf8") !== output) {
  throw new Error(`Refusing to overwrite non-identical historical run ${archivePath}`);
}
if (!existsSync(archivePath)) writeFileSync(archivePath, output);
writeFileSync(options.latestPath, output);

if (options.render) {
  const renderer = join(packageRoot, "scripts", "render-skill-eval-results.mjs");
  const htmlPath = options.latestPath.replace(/\.json$/u, ".html");
  execFileSync(process.execPath, [renderer, options.latestPath, htmlPath, options.historyDir], {
    cwd: packageRoot,
    stdio: "inherit",
  });
}

console.log(
  JSON.stringify(
    {
      archive: relative(packageRoot, archivePath),
      latest: relative(packageRoot, options.latestPath),
      filename: basename(archivePath),
      execution,
      corpus: report.corpus,
    },
    null,
    2,
  ),
);
