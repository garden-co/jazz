import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { stripVTControlCharacters } from "node:util";
import { fileURLToPath } from "node:url";

import {
  discoverPrebuiltTarballs,
  packWorkspaceTarballs,
  writeScaffoldedPnpmConfig,
} from "./run-starter.js";

const APP_NAME = "agent-skills-app";
const STARTER = "ts-localfirst";

interface PromptStep {
  prompt: string;
  answer: string;
}

function readFlagValue(args: string[], name: string): string | undefined {
  const index = args.indexOf(`--${name}`);
  if (index !== -1) return args[index + 1];
  const inline = args.find((arg) => arg.startsWith(`--${name}=`));
  return inline?.slice(name.length + 3);
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

function scriptCommand(executable: string, args: string[]): string[] {
  const executableCommand = [executable, ...args].map(shellQuote).join(" ");
  const command = `stty rows 40 columns 120; exec ${executableCommand}`;
  return ["-q", "-e", "-f", "-c", command, "/dev/null"];
}

function tclQuote(value: string): string {
  return `"${value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("$", "\\$")
    .replaceAll("[", "\\[")
    .replaceAll("]", "\\]")}"`;
}

function expectProgram(executable: string, args: string[], prompts: PromptStep[]): string {
  const command = [executable, ...args].map(tclQuote).join(" ");
  const interactions = prompts
    .map(
      ({ prompt, answer }) =>
        `expect -exact ${tclQuote(prompt)}\nsend -- [binary format H* "${Buffer.from(answer).toString("hex")}"]`,
    )
    .join("\n");
  return `set timeout 600
log_user 1
set stty_init "rows 40 columns 120"
spawn -noecho ${command}
${interactions}
expect eof
set result [wait]
exit [lindex $result 3]`;
}

async function runExpectCli(options: {
  executable: string;
  args: string[];
  cwd: string;
  env: NodeJS.ProcessEnv;
  prompts: PromptStep[];
  timeoutMs: number;
  verbose: boolean;
}): Promise<string> {
  return await new Promise((resolve, reject) => {
    const child = spawn(
      "expect",
      ["-c", expectProgram(options.executable, options.args, options.prompts)],
      {
        cwd: options.cwd,
        env: options.env,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const chunks: string[] = [];
    let settled = false;

    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback();
    };
    const handleOutput = (chunk: Buffer) => {
      const text = chunk.toString();
      chunks.push(text);
      if (options.verbose) process.stdout.write(text);
    };

    child.stdout.on("data", handleOutput);
    child.stderr.on("data", handleOutput);
    child.on("error", (error) => finish(() => reject(error)));
    child.on("exit", (code, signal) => {
      finish(() => {
        const output = stripVTControlCharacters(chunks.join(""));
        if (code === 0) resolve(output);
        else
          reject(
            new Error(
              `Interactive create-jazz exited with code=${code} signal=${signal ?? "none"}\n${output}`,
            ),
          );
      });
    });

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(() =>
        reject(new Error(`Interactive create-jazz timed out after ${options.timeoutMs}ms`)),
      );
    }, options.timeoutMs);
  });
}

async function runInteractiveCli(options: {
  executable: string;
  args: string[];
  cwd: string;
  env: NodeJS.ProcessEnv;
  prompts: PromptStep[];
  timeoutMs: number;
  verbose: boolean;
}): Promise<string> {
  if (process.platform === "darwin") return runExpectCli(options);
  if (process.platform !== "linux") {
    throw new Error(`Interactive create-jazz E2E is not supported on ${process.platform}.`);
  }

  return await new Promise((resolve, reject) => {
    const child = spawn("script", scriptCommand(options.executable, options.args), {
      cwd: options.cwd,
      env: options.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const chunks: string[] = [];
    let promptIndex = 0;
    let settled = false;

    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      callback();
    };

    const handleOutput = (chunk: Buffer) => {
      const text = chunk.toString();
      chunks.push(text);
      if (options.verbose) process.stdout.write(text);

      const output = stripVTControlCharacters(chunks.join(""));
      const step = options.prompts[promptIndex];
      if (step && output.includes(step.prompt)) {
        child.stdin.write(step.answer);
        promptIndex += 1;
      }
    };

    child.stdout.on("data", handleOutput);
    child.stderr.on("data", handleOutput);
    child.on("error", (error) => finish(() => reject(error)));
    child.on("exit", (code, signal) => {
      finish(() => {
        const output = stripVTControlCharacters(chunks.join(""));
        if (code !== 0) {
          reject(
            new Error(
              `Interactive create-jazz exited with code=${code} signal=${signal ?? "none"}\n${output}`,
            ),
          );
          return;
        }
        if (promptIndex !== options.prompts.length) {
          reject(
            new Error(
              `Interactive create-jazz answered ${promptIndex}/${options.prompts.length} prompts\n${output}`,
            ),
          );
          return;
        }
        resolve(output);
      });
    });

    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      finish(() =>
        reject(new Error(`Interactive create-jazz timed out after ${options.timeoutMs}ms`)),
      );
    }, options.timeoutMs);
  });
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const verbose = args.includes("--verbose");
  const keep = args.includes("--keep");
  const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const repoRoot = path.resolve(packageRoot, "../..");
  const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "create-jazz-agent-skills-e2e-"));
  const tarballDir = path.join(workDir, "_tarballs");
  const fixtureDir = path.join(repoRoot, "starters", `.agent-skills-e2e-${process.pid}`);
  const appDir = path.join(workDir, APP_NAME);

  try {
    fs.mkdirSync(tarballDir, { recursive: true });
    const prebuiltTarballDir = readFlagValue(args, "tarball-dir");
    const tarballs = prebuiltTarballDir
      ? discoverPrebuiltTarballs(path.resolve(prebuiltTarballDir))
      : await packWorkspaceTarballs(repoRoot, tarballDir, verbose);

    fs.cpSync(path.join(repoRoot, "starters", STARTER), fixtureDir, { recursive: true });
    writeScaffoldedPnpmConfig(fixtureDir, tarballs);

    const originalAgents = fs.readFileSync(path.join(fixtureDir, "AGENTS.md"), "utf8");
    assert.doesNotMatch(originalAgents, /intent-skills:start/);

    const cliEntry = path.join(repoRoot, "packages/create-jazz/src/index.ts");
    const tsxBin = path.join(repoRoot, "packages/create-jazz/node_modules/.bin/tsx");
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      JAZZ_STARTER_PATH: fixtureDir,
      npm_config_user_agent: `pnpm/${process.env.npm_package_version ?? "10"}`,
      COREPACK_ENABLE_DOWNLOAD_PROMPT: "0",
    };

    const transcript = await runInteractiveCli({
      executable: tsxBin,
      args: [cliEntry, APP_NAME, "--no-git"],
      cwd: workDir,
      env,
      prompts: [
        { prompt: "Framework", answer: "\u001b[B\u001b[B\u001b[B\r" },
        { prompt: "Hosting", answer: "\u001b[B\r" },
        { prompt: "Auth", answer: "\r" },
        { prompt: "Set up Jazz coding skills for your AI agent?", answer: "\r" },
      ],
      timeoutMs: 10 * 60_000,
      verbose,
    });

    assert.match(
      transcript,
      /(?:^|\n)│\s+Yes(?:\r?\n|$)/m,
      "pressing Enter should accept the prompt's default Yes answer",
    );
    assert.match(transcript, /Jazz coding skills are ready for your agent/);

    assert.ok(fs.existsSync(path.join(appDir, "pnpm-lock.yaml")), "pnpm installed dependencies");
    assert.ok(
      fs.existsSync(path.join(appDir, "node_modules/jazz-tools/skills/jazz-core/SKILL.md")),
      "the packed jazz-tools dependency includes its skills",
    );

    const generatedAgents = fs.readFileSync(path.join(appDir, "AGENTS.md"), "utf8");
    assert.notEqual(generatedAgents, originalAgents, "Intent should modify AGENTS.md");
    assert.match(generatedAgents, /<!-- intent-skills:start -->/);
    assert.match(generatedAgents, /@tanstack\/intent@latest list/);
    assert.match(generatedAgents, /<!-- intent-skills:end -->/);

    const generatedPackage = JSON.parse(
      fs.readFileSync(path.join(appDir, "package.json"), "utf8"),
    ) as { intent?: { skills?: string[] } };
    assert.deepEqual(generatedPackage.intent?.skills, ["jazz-tools"]);

    console.log("create-jazz interactive agent skill setup: passed");
  } finally {
    fs.rmSync(fixtureDir, { recursive: true, force: true });
    if (keep) console.log(`Kept E2E project at ${workDir}`);
    else fs.rmSync(workDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
