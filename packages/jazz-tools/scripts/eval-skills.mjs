import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const evalPath = join(packageRoot, "skill-evals", "cases.json");
const skillsRoot = join(packageRoot, "skills");

function fail(message) {
  console.error(`Skill eval error: ${message}`);
  process.exitCode = 1;
}

function parseSkill(skillDir) {
  const content = readFileSync(join(skillsRoot, skillDir, "SKILL.md"), "utf8");
  const frontmatter = content.match(/^---\n([\s\S]*?)\n---/u)?.[1];
  const name = frontmatter?.match(/^name:\s*(.+)$/mu)?.[1]?.trim();
  const description = frontmatter?.match(/^description:\s*(.+)$/mu)?.[1]?.trim();

  if (!name || !description) {
    throw new Error(`Could not read name and description from skills/${skillDir}/SKILL.md`);
  }

  return { name, description };
}

const corpus = JSON.parse(readFileSync(evalPath, "utf8"));
const skills = readdirSync(skillsRoot, { withFileTypes: true })
  .filter((entry) => entry.isDirectory() && existsSync(join(skillsRoot, entry.name, "SKILL.md")))
  .map((entry) => parseSkill(entry.name))
  .sort((left, right) => left.name.localeCompare(right.name));
const knownSkills = new Set(skills.map((skill) => skill.name));

function validateCorpus({ quiet = false } = {}) {
  if (corpus.version !== 1) fail(`Unsupported corpus version ${corpus.version}`);
  if (!Array.isArray(corpus.cases) || corpus.cases.length === 0)
    fail("cases must be a non-empty array");

  const ids = new Set();
  let routing = 0;
  let behavior = 0;

  for (const evalCase of corpus.cases ?? []) {
    if (typeof evalCase.id !== "string" || !/^[a-z0-9-]+$/u.test(evalCase.id)) {
      fail(`Invalid case id ${JSON.stringify(evalCase.id)}`);
      continue;
    }
    if (ids.has(evalCase.id)) fail(`Duplicate case id ${evalCase.id}`);
    ids.add(evalCase.id);

    if (evalCase.kind === "routing") routing += 1;
    else if (evalCase.kind === "behavior") behavior += 1;
    else fail(`${evalCase.id}: kind must be routing or behavior`);

    if (typeof evalCase.prompt !== "string" || evalCase.prompt.trim().length < 20) {
      fail(`${evalCase.id}: prompt is missing or too short`);
    }
    if (!Array.isArray(evalCase.expectedSkills)) {
      fail(`${evalCase.id}: expectedSkills must be an array`);
      continue;
    }

    const selected = new Set();
    for (const skill of evalCase.expectedSkills) {
      if (!knownSkills.has(skill)) fail(`${evalCase.id}: unknown expected skill ${skill}`);
      if (selected.has(skill)) fail(`${evalCase.id}: duplicate expected skill ${skill}`);
      selected.add(skill);
    }

    if (evalCase.kind === "behavior") {
      if (!Array.isArray(evalCase.rubric) || evalCase.rubric.length === 0) {
        fail(`${evalCase.id}: behavior cases require a non-empty rubric`);
      } else if (
        evalCase.rubric.some((criterion) => typeof criterion !== "string" || !criterion.trim())
      ) {
        fail(`${evalCase.id}: rubric criteria must be non-empty strings`);
      }
    } else if (evalCase.rubric !== undefined) {
      fail(`${evalCase.id}: routing cases must not include a behavior rubric`);
    }
  }

  if (process.exitCode) return;
  if (!quiet) {
    console.log(
      `Validated ${corpus.cases.length} skill evals (${routing} routing, ${behavior} behavior)`,
    );
    console.log(`Skill catalog: ${skills.map((skill) => skill.name).join(", ")}`);
  }
}

function emitPrompts() {
  validateCorpus({ quiet: true });
  if (process.exitCode) return;

  for (const evalCase of corpus.cases) {
    console.log(
      JSON.stringify({
        id: evalCase.id,
        kind: evalCase.kind,
        availableSkills: skills,
        prompt: evalCase.prompt,
        responseFormat: {
          selectedSkills: "array of skill names, using an empty array when none apply",
          response: "answer or implementation plan for the user",
        },
      }),
    );
  }
}

function readResults(resultPath) {
  const text = readFileSync(resultPath, "utf8").trim();
  if (!text) return [];
  if (text.startsWith("[")) return JSON.parse(text);
  return text.split(/\r?\n/u).map((line) => JSON.parse(line));
}

function selectionResult(actual, expected) {
  const exact =
    actual.length === expected.length && expected.every((skill) => actual.includes(skill));
  const passed =
    expected.length === 0 ? actual.length === 0 : expected.every((skill) => actual.includes(skill));
  return { passed, exact };
}

function scoreResults(resultPath) {
  validateCorpus({ quiet: true });
  if (process.exitCode) return;
  if (!resultPath) {
    fail("score requires a JSON or JSONL results path");
    return;
  }

  const results = readResults(resultPath);
  const byId = new Map(results.map((result) => [result.id, result]));
  let routingPassed = 0;
  let routingExact = 0;
  let rubricPassed = 0;
  let rubricTotal = 0;

  for (const evalCase of corpus.cases) {
    const result = byId.get(evalCase.id);
    if (!result) {
      fail(`Missing result for ${evalCase.id}`);
      continue;
    }
    if (
      !Array.isArray(result.selectedSkills) ||
      result.selectedSkills.some((skill) => !knownSkills.has(skill))
    ) {
      fail(`${evalCase.id}: selectedSkills contains an invalid value`);
      continue;
    }

    const selection = selectionResult(result.selectedSkills, evalCase.expectedSkills);
    if (selection.passed) routingPassed += 1;
    else {
      console.error(
        `${evalCase.id}: expected [${evalCase.expectedSkills.join(", ")}], got [${result.selectedSkills.join(", ")}]`,
      );
      process.exitCode = 1;
    }
    if (selection.exact) routingExact += 1;

    if (evalCase.kind === "behavior" && result.rubricScores !== undefined) {
      if (
        !Array.isArray(result.rubricScores) ||
        result.rubricScores.length !== evalCase.rubric.length ||
        result.rubricScores.some((score) => score !== 0 && score !== 1)
      ) {
        fail(`${evalCase.id}: rubricScores must contain one 0 or 1 per rubric criterion`);
        continue;
      }
      rubricPassed += result.rubricScores.reduce((total, score) => total + score, 0);
      rubricTotal += result.rubricScores.length;
    }
  }

  console.log(
    `Skill selection: ${routingPassed}/${corpus.cases.length} acceptable matches (${routingExact} exact)`,
  );
  console.log(
    rubricTotal > 0
      ? `Behavior rubric: ${rubricPassed}/${rubricTotal} criteria`
      : "Behavior rubric: unscored (add rubricScores to behavior results)",
  );
}

const [command = "check", argument] = process.argv.slice(2).filter((argument) => argument !== "--");

if (command === "check") validateCorpus();
else if (command === "prompts") emitPrompts();
else if (command === "score") scoreResults(argument);
else fail(`Unknown command ${command}; use check, prompts, or score`);
