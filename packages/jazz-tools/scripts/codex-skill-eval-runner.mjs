import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync, spawn } from "node:child_process";

function readStdin() {
  return new Promise((resolve, reject) => {
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      input += chunk;
    });
    process.stdin.on("end", () => resolve(input));
    process.stdin.on("error", reject);
  });
}

function run(command, args, { cwd, input }) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
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
    child.on("close", (code) => resolve({ code, stdout, stderr }));
    child.stdin.end(input);
  });
}

const request = JSON.parse(await readStdin());
const codexBin = process.env.CODEX_BIN ?? "codex";
let runnerVersion = "unknown";
try {
  runnerVersion = execFileSync(codexBin, ["--version"], { encoding: "utf8" }).trim();
} catch {}
const runDir = mkdtempSync(join(tmpdir(), "jazz-skill-eval-"));
const schemaPath = join(runDir, "output-schema.json");
const outputPath = join(runDir, "output.json");

try {
  writeFileSync(schemaPath, JSON.stringify(request.schema));

  const args = [
    "exec",
    "--disable",
    "multi_agent",
    "--ephemeral",
    "--ignore-user-config",
    "--skip-git-repo-check",
    "--sandbox",
    "read-only",
    "--color",
    "never",
    "--cd",
    runDir,
    "--output-schema",
    schemaPath,
    "--output-last-message",
    outputPath,
  ];
  if (process.env.SKILL_EVAL_MODEL) args.push("--model", process.env.SKILL_EVAL_MODEL);
  if (process.env.SKILL_EVAL_REASONING) {
    args.push(
      "--config",
      `model_reasoning_effort=${JSON.stringify(process.env.SKILL_EVAL_REASONING)}`,
    );
  }
  args.push("-");

  const startedAt = Date.now();
  const result = await run(codexBin, args, {
    cwd: runDir,
    input: request.prompt,
  });
  if (result.code !== 0) {
    throw new Error(
      `Codex runner failed with exit code ${result.code}\n${result.stderr}\n${result.stdout}`,
    );
  }

  const output = JSON.parse(readFileSync(outputPath, "utf8"));
  const log = `${result.stdout}\n${result.stderr}`;
  const model = log.match(/^model:\s*(.+)$/mu)?.[1]?.trim() ?? process.env.SKILL_EVAL_MODEL ?? null;
  const reasoning =
    log.match(/^reasoning effort:\s*(.+)$/mu)?.[1]?.trim() ??
    process.env.SKILL_EVAL_REASONING ??
    null;
  const tokens = log.match(/tokens used\s*\n([\d,]+)/u)?.[1]?.replaceAll(",", "");

  process.stdout.write(
    JSON.stringify({
      output,
      meta: {
        runner: "codex",
        runnerVersion,
        model,
        reasoning,
        durationMs: Date.now() - startedAt,
        tokens: tokens ? Number(tokens) : null,
      },
    }),
  );
} finally {
  rmSync(runDir, { recursive: true, force: true });
}
