import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const corpus = JSON.parse(readFileSync(join(packageRoot, "skill-evals", "cases.json"), "utf8"));
const skillsRoot = join(packageRoot, "skills");

function parseArguments(args) {
  const options = {
    output: null,
    runner: join(packageRoot, "scripts", "codex-skill-eval-runner.mjs"),
  };

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--") continue;
    if (argument === "--help") options.help = true;
    else if (argument === "--output") options.output = args[++index];
    else if (argument === "--runner") options.runner = args[++index];
    else throw new Error(`Unknown argument ${argument}`);
  }

  if (options.output) options.output = resolve(options.output);
  options.runner = isAbsolute(options.runner)
    ? options.runner
    : resolve(packageRoot, options.runner);
  return options;
}

function printHelp() {
  console.log(`Usage: node scripts/run-skill-evals.mjs [options]

Runs blind routing and baseline-versus-loaded behavior evals.

Options:
  --runner <path>  Executable implementing the JSON runner protocol
                   (default: scripts/codex-skill-eval-runner.mjs)
  --output <path>  Write the full JSON report to a file instead of stdout
  --help           Show this help

Runner protocol:
  stdin:  { "prompt": string, "schema": JSONSchema }
  stdout: { "output": object, "meta": object }
`);
}

function readSkill(skillDir) {
  const directory = join(skillsRoot, skillDir);
  const skillPath = join(directory, "SKILL.md");
  if (!existsSync(skillPath)) return null;

  const content = readFileSync(skillPath, "utf8");
  const frontmatter = content.match(/^---\n([\s\S]*?)\n---/u)?.[1];
  const name = frontmatter?.match(/^name:\s*(.+)$/mu)?.[1]?.trim();
  const description = frontmatter?.match(/^description:\s*(.+)$/mu)?.[1]?.trim();
  if (!name || !description) throw new Error(`Invalid skill frontmatter in ${skillPath}`);

  const files = [skillPath];
  const referencesDir = join(directory, "references");
  if (existsSync(referencesDir)) {
    files.push(
      ...readdirSync(referencesDir)
        .filter((file) => file.endsWith(".md"))
        .sort()
        .map((file) => join(referencesDir, file)),
    );
  }

  const guidance = files
    .map((file) => `## ${relative(directory, file)}\n\n${readFileSync(file, "utf8")}`)
    .join("\n\n");
  return { name, description, guidance };
}

const skills = readdirSync(skillsRoot, { withFileTypes: true })
  .filter((entry) => entry.isDirectory())
  .map((entry) => readSkill(entry.name))
  .filter(Boolean)
  .sort((left, right) => left.name.localeCompare(right.name));
const skillsByName = new Map(skills.map((skill) => [skill.name, skill]));
const skillNames = skills.map((skill) => skill.name);

function invokeRunner(runnerPath, request) {
  return new Promise((resolvePromise, reject) => {
    const command = runnerPath.endsWith(".mjs") ? process.execPath : runnerPath;
    const args = runnerPath.endsWith(".mjs") ? [runnerPath] : [];
    const child = spawn(command, args, {
      cwd: packageRoot,
      env: process.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Runner exited with code ${code}\n${stderr}\n${stdout}`));
        return;
      }
      try {
        resolvePromise(JSON.parse(stdout));
      } catch (error) {
        reject(new Error(`Runner returned invalid JSON\n${stderr}\n${stdout}`, { cause: error }));
      }
    });
    child.stdin.end(JSON.stringify(request));
  });
}

function targetSchema(cases) {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      results: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            id: { type: "string", enum: cases.map((evalCase) => evalCase.id) },
            selectedSkills: {
              type: "array",
              items: { type: "string", enum: skillNames },
            },
            response: { type: "string" },
          },
          required: ["id", "selectedSkills", "response"],
        },
      },
    },
    required: ["results"],
  };
}

function judgeSchema(cases) {
  return {
    type: "object",
    additionalProperties: false,
    properties: {
      results: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            id: { type: "string", enum: cases.map((evalCase) => evalCase.id) },
            rubricScores: {
              type: "array",
              items: { type: "integer", enum: [0, 1] },
            },
            rationale: { type: "string" },
          },
          required: ["id", "rubricScores", "rationale"],
        },
      },
    },
    required: ["results"],
  };
}

function catalogText() {
  return skills.map((skill) => `- ${skill.name}: ${skill.description}`).join("\n");
}

function taskText(cases) {
  return cases.map((evalCase) => `- ${evalCase.id}: ${evalCase.prompt}`).join("\n");
}

function buildTargetPrompt(cases, { loadedSkill = null } = {}) {
  const loaded = loadedSkill
    ? `\nThe following skill is loaded. Follow it when answering.\n\n# ${loadedSkill.name}\n\n${loadedSkill.guidance}\n`
    : "\nNo full skill guidance is loaded. Answer using only the catalog and your existing knowledge.\n";

  return `You are completing a blind evaluation of task-to-skill routing and answer quality.
Do not discuss the evaluation. For every task, select exactly the skills that should be loaded from
the catalog, then give a concise but technically actionable answer. Select no skill for unrelated
tasks. Do not load a testing skill merely because existing tests should continue to pass.

Available skill catalog:
${catalogText()}
${loaded}
Tasks:
${taskText(cases)}

Return one result for every task and no additional results.`;
}

function buildJudgePrompt(cases, responses) {
  const records = cases.map((evalCase) => ({
    id: evalCase.id,
    prompt: evalCase.prompt,
    rubric: evalCase.rubric,
    rubricScoreCount: evalCase.rubric.length,
    response: responses.find((response) => response.id === evalCase.id)?.response,
  }));

  return `Grade these blind skill-evaluation responses for semantic correctness, not phrase matching.
For each rubric criterion, return 1 when the response correctly states, demonstrates, or
unambiguously entails the required behavior; otherwise return 0. Code, API usage, type signatures,
and concrete procedures can satisfy a prose criterion without repeating its wording. Accept ordinary
equivalents such as creating a new client for recreating one, and do not require redundant statements
when the required fact is already clear from a concrete example.

Do not invent facts that are absent, excuse a technically important omission, or reward a response
merely for selecting the expected skill. Do not claim knowledge of hidden tool calls: judge only the
response's observable claims and evidence. For a negative criterion, score 1 when the response avoids
the prohibited claim and does not otherwise contradict the criterion. Return rubric scores in the
same order as the criteria. Each rubricScores array must contain exactly the number of entries in
rubricScoreCount for that record—no more and no fewer.

${JSON.stringify(records, null, 2)}`;
}

function validateResults(cases, results, { judged = false } = {}) {
  if (!Array.isArray(results) || results.length !== cases.length) {
    throw new Error(`Expected ${cases.length} results, received ${results?.length ?? "none"}`);
  }
  const byId = new Map(results.map((result) => [result.id, result]));
  if (byId.size !== results.length) throw new Error("Runner returned duplicate result ids");

  for (const evalCase of cases) {
    const result = byId.get(evalCase.id);
    if (!result) throw new Error(`Runner omitted ${evalCase.id}`);
    if (judged && result.rubricScores.length !== evalCase.rubric.length) {
      throw new Error(
        `${evalCase.id} returned ${result.rubricScores.length} scores for ${evalCase.rubric.length} criteria`,
      );
    }
  }
}

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

function summarize(routingCases, routing, behaviorCases, baseline, loaded) {
  const routingSelections = routing.map((result) => {
    const evalCase = routingCases.find((candidate) => candidate.id === result.id);
    return selectionResult(result.selectedSkills, evalCase.expectedSkills);
  });
  const summarizeBehavior = (results) => {
    const rubricScores = results.flatMap((result) => result.rubricScores);
    const selections = results.map((result) => {
      const evalCase = behaviorCases.find((candidate) => candidate.id === result.id);
      return selectionResult(result.selectedSkills, evalCase.expectedSkills);
    });
    return {
      skillSelection: {
        passed: selections.filter((selection) => selection.passed).length,
        exact: selections.filter((selection) => selection.exact).length,
        total: behaviorCases.length,
      },
      rubric: {
        passed: rubricScores.reduce((sum, score) => sum + score, 0),
        total: rubricScores.length,
      },
    };
  };

  const baselineSummary = summarizeBehavior(baseline);
  const loadedSummary = summarizeBehavior(loaded);
  return {
    routing: {
      passed: routingSelections.filter((selection) => selection.passed).length,
      exact: routingSelections.filter((selection) => selection.exact).length,
      total: routingCases.length,
    },
    baseline: baselineSummary,
    loaded: loadedSummary,
    rubricUplift: loadedSummary.rubric.passed - baselineSummary.rubric.passed,
  };
}

async function runBatch(runner, label, cases, prompt, schema) {
  console.error(`Running ${label} (${cases.length} cases)...`);
  const response = await invokeRunner(runner, { prompt, schema });
  validateResults(cases, response.output.results);
  return response;
}

async function judgeBatch(runner, label, cases, responses) {
  const attempts = 3;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    console.error(
      `Judging ${label} (${cases.length} cases)${attempt === 1 ? "" : `, attempt ${attempt}`}...`,
    );
    const response = await invokeRunner(runner, {
      prompt: buildJudgePrompt(cases, responses),
      schema: judgeSchema(cases),
    });
    try {
      validateResults(cases, response.output.results, { judged: true });
      return response;
    } catch (error) {
      if (attempt === attempts) throw error;
      console.error(`Judge output was malformed; retrying: ${error.message}`);
    }
  }
  throw new Error(`Unable to judge ${label}`);
}

const options = parseArguments(process.argv.slice(2));
if (options.help) {
  printHelp();
  process.exit(0);
}
if (!existsSync(options.runner)) throw new Error(`Runner not found: ${options.runner}`);

const routingCases = corpus.cases.filter((evalCase) => evalCase.kind === "routing");
const behaviorCases = corpus.cases.filter((evalCase) => evalCase.kind === "behavior");
const runs = [];

const routingRun = await runBatch(
  options.runner,
  "metadata routing",
  routingCases,
  buildTargetPrompt(routingCases),
  targetSchema(routingCases),
);
runs.push(routingRun.meta);

const baselineRun = await runBatch(
  options.runner,
  "metadata-only behavior",
  behaviorCases,
  buildTargetPrompt(behaviorCases),
  targetSchema(behaviorCases),
);
runs.push(baselineRun.meta);

const loadedResults = [];
for (const skillName of skillNames) {
  const cases = behaviorCases.filter((evalCase) => evalCase.expectedSkills.includes(skillName));
  if (cases.length === 0) continue;
  const loadedRun = await runBatch(
    options.runner,
    `loaded behavior: ${skillName}`,
    cases,
    buildTargetPrompt(cases, { loadedSkill: skillsByName.get(skillName) }),
    targetSchema(cases),
  );
  runs.push(loadedRun.meta);
  loadedResults.push(...loadedRun.output.results);
}

const baselineJudge = await judgeBatch(
  options.runner,
  "metadata-only behavior",
  behaviorCases,
  baselineRun.output.results,
);
runs.push(baselineJudge.meta);
const loadedJudge = await judgeBatch(
  options.runner,
  "skill-loaded behavior",
  behaviorCases,
  loadedResults,
);
runs.push(loadedJudge.meta);

const baselineScores = new Map(baselineJudge.output.results.map((result) => [result.id, result]));
const loadedScores = new Map(loadedJudge.output.results.map((result) => [result.id, result]));
const baselineResults = baselineRun.output.results.map((result) => ({
  ...result,
  ...baselineScores.get(result.id),
}));
const scoredLoadedResults = loadedResults.map((result) => ({
  ...result,
  ...loadedScores.get(result.id),
}));
const summary = summarize(
  routingCases,
  routingRun.output.results,
  behaviorCases,
  baselineResults,
  scoredLoadedResults,
);

const routingResults = routingRun.output.results.map((result) => {
  const evalCase = routingCases.find((candidate) => candidate.id === result.id);
  return {
    ...result,
    prompt: evalCase.prompt,
    expectedSkills: evalCase.expectedSkills,
    ...selectionResult(result.selectedSkills, evalCase.expectedSkills),
  };
});
const enrichBehavior = (results) =>
  results.map((result) => {
    const evalCase = behaviorCases.find((candidate) => candidate.id === result.id);
    const selection = selectionResult(result.selectedSkills, evalCase.expectedSkills);
    return {
      ...result,
      prompt: evalCase.prompt,
      expectedSkills: evalCase.expectedSkills,
      rubric: evalCase.rubric,
      selectionPassed: selection.passed,
      selectionExact: selection.exact,
      extraSkills: selection.extraSkills,
    };
  });

const report = {
  version: 1,
  generatedAt: new Date().toISOString(),
  runner: relative(packageRoot, options.runner),
  summary,
  runs,
  routing: routingResults,
  behavior: {
    baseline: enrichBehavior(baselineResults),
    loaded: enrichBehavior(scoredLoadedResults),
  },
};

console.error(
  `Routing ${summary.routing.passed}/${summary.routing.total}; behavior rubric ${summary.baseline.rubric.passed}/${summary.baseline.rubric.total} baseline -> ${summary.loaded.rubric.passed}/${summary.loaded.rubric.total} loaded (${summary.rubricUplift >= 0 ? "+" : ""}${summary.rubricUplift})`,
);

const output = `${JSON.stringify(report, null, 2)}\n`;
if (options.output) writeFileSync(options.output, output);
else process.stdout.write(output);
