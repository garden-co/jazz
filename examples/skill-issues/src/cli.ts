import { pathToFileURL } from "node:url";
import { writeConfig } from "./config.js";
import { generateLocalFirstSecret } from "./local-auth.js";

export interface CliRuntime {
  cwd: string;
  env: NodeJS.ProcessEnv;
}

export interface CliResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function initAuth(runtime: CliRuntime): Promise<CliResult> {
  const appId = runtime.env.SKILL_ISSUES_APP_ID;
  const serverUrl = runtime.env.SKILL_ISSUES_SERVER_URL;
  const verifierUrl = runtime.env.SKILL_ISSUES_VERIFIER_URL;

  if (!appId) throw new Error("SKILL_ISSUES_APP_ID is required.");
  if (!serverUrl) throw new Error("SKILL_ISSUES_SERVER_URL is required.");

  await writeConfig(runtime.cwd, {
    appId,
    serverUrl,
    verifierUrl,
    localFirstSecret: generateLocalFirstSecret(),
  });

  return {
    exitCode: 0,
    stdout: "Initialized skill issues auth.\n",
    stderr: "",
  };
}

export async function runCli(args: string[], runtime: CliRuntime): Promise<CliResult> {
  try {
    const [command, subcommand] = args;

    if (command === "auth" && subcommand === "init") {
      return await initAuth(runtime);
    }

    return {
      exitCode: 1,
      stdout: "",
      stderr: `Unknown command: ${command ?? ""}\n`,
    };
  } catch (error) {
    return {
      exitCode: 1,
      stdout: "",
      stderr: `${errorMessage(error)}\n`,
    };
  }
}

export async function main(
  args = process.argv.slice(2),
  runtime: CliRuntime = { cwd: process.cwd(), env: process.env },
): Promise<void> {
  const result = await runCli(args, runtime);

  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  process.exitCode = result.exitCode;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  await main();
}
