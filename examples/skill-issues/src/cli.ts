import { existsSync } from "node:fs";
import { createServer as createHttpServer } from "node:http";
import { pathToFileURL } from "node:url";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer as createViteServer } from "vite";
import { readConfig, writeConfig } from "./config.js";
import { openRepository as defaultOpenRepository } from "./db.js";
import { exportMarkdownTodo, importMarkdownTodo } from "./domain/markdown.js";
import type { IssueItem, ItemKind, ItemStatus, ListedItem, ListFilters } from "./repository.js";
import {
  createLocalFirstProof as defaultCreateLocalFirstProof,
  generateLocalFirstSecret,
} from "./local-auth.js";
import {
  startDeviceAuthorization as defaultStartDeviceAuthorization,
  type GitHubDeviceStart,
} from "./server/github.js";
import { createSkillIssuesServer } from "./server/server.js";

export interface CliRuntime {
  cwd: string;
  env: NodeJS.ProcessEnv;
  writeStdout?: (text: string) => void;
}

export interface CliResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

export interface CliDependencies {
  openRepository?: typeof defaultOpenRepository;
  startDeviceAuthorization?: (clientId: string) => Promise<GitHubDeviceStart>;
  waitForGitHubAuthorization?: (device: GitHubDeviceStart) => Promise<void>;
  createLocalFirstProof?: (secret: string) => string;
  completeGitHubVerification?: (
    verifierUrl: string,
    payload: { deviceCode: string; jazzProof: string },
  ) => Promise<{ id: string; githubLogin: string }>;
}

const KINDS = ["idea", "issue"] as const;
const STATUSES = ["open", "in_progress", "done"] as const;

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function usage(stderr: string): CliResult {
  return {
    exitCode: 1,
    stdout: "",
    stderr: `${stderr}\n`,
  };
}

function valueAfter(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag);
  return index === -1 ? undefined : args[index + 1];
}

function requiredPositional(value: string | undefined): string | null {
  return value && !value.startsWith("--") ? value : null;
}

function requireValue(args: string[], flag: string, usageText: string): string {
  const value = valueAfter(args, flag);
  if (!value || value.startsWith("--")) {
    throw new Error(`${flag} is required.\n${usageText}`);
  }
  return value;
}

function parseKind(value: string | undefined): ItemKind | null {
  return KINDS.includes(value as ItemKind) ? (value as ItemKind) : null;
}

function parseStatus(value: string | undefined): ItemStatus | null {
  return STATUSES.includes(value as ItemStatus) ? (value as ItemStatus) : null;
}

function formatItem(item: ListedItem): string {
  return `${item.kind} ${item.slug} ${item.state.status} ${item.title}`;
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

function trimTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

async function defaultCompleteGitHubVerification(
  verifierUrl: string,
  payload: { deviceCode: string; jazzProof: string },
): Promise<{ id: string; githubLogin: string }> {
  const response = await fetch(`${trimTrailingSlash(verifierUrl)}/auth/github/complete`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const body: unknown = await response.json();

  if (!response.ok) {
    const message =
      typeof body === "object" && body !== null && "error" in body && typeof body.error === "string"
        ? body.error
        : "GitHub verification failed.";
    throw new Error(message);
  }

  if (
    typeof body !== "object" ||
    body === null ||
    !("id" in body) ||
    typeof body.id !== "string" ||
    !("githubLogin" in body) ||
    typeof body.githubLogin !== "string"
  ) {
    throw new Error("Verifier returned an invalid response.");
  }

  return {
    id: body.id,
    githubLogin: body.githubLogin,
  };
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
}

async function defaultWaitForGitHubAuthorization(device: GitHubDeviceStart): Promise<void> {
  await sleep(device.interval * 1000);
}

async function authGitHub(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const config = await readConfig(runtime.cwd, runtime.env);
  const verifierUrl = valueAfter(args, "--verifier-url") ?? config.verifierUrl;
  const clientId = runtime.env.GITHUB_CLIENT_ID;

  if (!verifierUrl) {
    throw new Error("--verifier-url or SKILL_ISSUES_VERIFIER_URL is required.");
  }
  if (!clientId) {
    throw new Error("GITHUB_CLIENT_ID is required.");
  }

  const startDeviceAuthorization = deps.startDeviceAuthorization ?? defaultStartDeviceAuthorization;
  const waitForGitHubAuthorization =
    deps.waitForGitHubAuthorization ?? defaultWaitForGitHubAuthorization;
  const createLocalFirstProof = deps.createLocalFirstProof ?? defaultCreateLocalFirstProof;
  const completeGitHubVerification =
    deps.completeGitHubVerification ?? defaultCompleteGitHubVerification;
  const deviceStart = await startDeviceAuthorization(clientId);
  const authPrompt = `Open ${deviceStart.verification_uri} and enter code ${deviceStart.user_code}.\n`;
  runtime.writeStdout?.(authPrompt);
  await waitForGitHubAuthorization(deviceStart);
  const jazzProof = createLocalFirstProof(config.localFirstSecret);
  const verified = await completeGitHubVerification(verifierUrl, {
    deviceCode: deviceStart.device_code,
    jazzProof,
  });

  return {
    exitCode: 0,
    stdout: `${runtime.writeStdout ? "" : authPrompt}Verified GitHub user ${verified.githubLogin}.\n`,
    stderr: "",
  };
}

async function openDataRepository(runtime: CliRuntime, deps: CliDependencies) {
  const openRepository = deps.openRepository ?? defaultOpenRepository;
  return openRepository(runtime.cwd, runtime.env);
}

async function addItem(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const usageText =
    "Usage: issues add <idea|issue> <slug> --title <title> --description <description>";
  const kind = parseKind(args[1]);
  const slug = requiredPositional(args[2]);
  if (!kind || !slug) {
    return usage(usageText);
  }

  const item: IssueItem = {
    kind,
    slug,
    title: requireValue(args, "--title", usageText),
    description: requireValue(args, "--description", usageText),
  };

  const repo = await openDataRepository(runtime, deps);
  await repo.upsertItem(item);

  return {
    exitCode: 0,
    stdout: `Saved ${kind} ${slug}.\n`,
    stderr: "",
  };
}

async function listItems(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const usageText = "Usage: issues list [--kind idea|issue] [--status open|in_progress|done]";
  const kindValue = valueAfter(args, "--kind");
  const statusValue = valueAfter(args, "--status");
  const kindFlagPresent = args.includes("--kind");
  const statusFlagPresent = args.includes("--status");
  const kind = kindFlagPresent ? parseKind(kindValue) : undefined;
  const status = statusFlagPresent ? parseStatus(statusValue) : undefined;

  if (kindFlagPresent && !kind) return usage(usageText);
  if (statusFlagPresent && !status) return usage(usageText);

  const filters: ListFilters = {
    ...(kind ? { kind } : {}),
    ...(status ? { status } : {}),
  };
  const repo = await openDataRepository(runtime, deps);
  const items = await repo.listItems(filters);
  const stdout = items.length ? `${items.map(formatItem).join("\n")}\n` : "";

  return {
    exitCode: 0,
    stdout,
    stderr: "",
  };
}

async function showItem(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const slug = requiredPositional(args[1]);
  if (!slug) return usage("Usage: issues show <slug>");

  const repo = await openDataRepository(runtime, deps);
  const item = await repo.getItem(slug);
  if (!item) {
    return usage(`Item not found: ${slug}`);
  }

  return {
    exitCode: 0,
    stdout: `${formatItem(item)}\n\n${item.description}\n`,
    stderr: "",
  };
}

async function assignMe(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const slug = requiredPositional(args[1]);
  if (!slug || !args.includes("--me")) {
    return usage("Usage: issues assign <slug> --me");
  }

  const repo = await openDataRepository(runtime, deps);
  await repo.assignMe(slug);

  return {
    exitCode: 0,
    stdout: `Assigned ${slug} to current user.\n`,
    stderr: "",
  };
}

async function setItemStatus(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const slug = requiredPositional(args[1]);
  const status = parseStatus(args[2]);
  if (!slug || !status) {
    return usage("Usage: issues status <slug> <open|in_progress|done>");
  }

  const repo = await openDataRepository(runtime, deps);
  await repo.setStatus(slug, status);

  return {
    exitCode: 0,
    stdout: `Set ${slug} to ${status}.\n`,
    stderr: "",
  };
}

async function importItems(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const dir = requiredPositional(args[1]);
  if (!dir) return usage("Usage: issues import <dir>");

  const repo = await openDataRepository(runtime, deps);
  const items = await importMarkdownTodo(join(runtime.cwd, dir));
  for (const item of items) {
    await repo.upsertItem(item);
  }

  return {
    exitCode: 0,
    stdout: `Imported ${items.length} items.\n`,
    stderr: "",
  };
}

async function exportItems(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies,
): Promise<CliResult> {
  const dir = requiredPositional(args[1]);
  if (!dir) return usage("Usage: issues export <dir>");

  const repo = await openDataRepository(runtime, deps);
  const items = await repo.listItems({});
  const plainItems: IssueItem[] = items.map(({ kind, slug, title, description }) => ({
    kind,
    slug,
    title,
    description,
  }));
  await exportMarkdownTodo(join(runtime.cwd, dir), plainItems);

  return {
    exitCode: 0,
    stdout: `Exported ${plainItems.length} items.\n`,
    stderr: "",
  };
}

function parsePort(value: string | undefined): number {
  const port = Number(value ?? "4242");
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error("Port must be an integer from 1 to 65535.");
  }
  return port;
}

function findUiRoot(runtime: CliRuntime): string {
  const moduleDir = dirname(fileURLToPath(import.meta.url));
  const candidates = [
    join(moduleDir, ".."),
    join(moduleDir, "..", ".."),
    join(runtime.cwd, "examples", "skill-issues"),
    runtime.cwd,
  ];

  return candidates.find((candidate) => existsSync(join(candidate, "index.html"))) ?? runtime.cwd;
}

async function serve(args: string[], runtime: CliRuntime): Promise<CliResult> {
  const port = parsePort(valueAfter(args, "--port") ?? runtime.env.PORT);
  const app = createSkillIssuesServer();
  const vite = await createViteServer({
    root: findUiRoot(runtime),
    server: { middlewareMode: true },
    appType: "spa",
  });
  app.use(vite.middlewares);

  const server = createHttpServer(app);
  try {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      server.once("error", onError);
      server.listen(port, "127.0.0.1", () => {
        server.off("error", onError);
        server.once("close", () => {
          void vite.close();
        });
        resolve();
      });
    });
  } catch (error) {
    await vite.close();
    throw error;
  }

  return {
    exitCode: 0,
    stdout: `Skill issues server running at http://localhost:${port}\n`,
    stderr: "",
  };
}

export async function runCli(
  args: string[],
  runtime: CliRuntime,
  deps: CliDependencies = {},
): Promise<CliResult> {
  try {
    const [command, subcommand] = args;

    if (command === "auth" && subcommand === "init") {
      return await initAuth(runtime);
    }
    if (command === "auth" && subcommand === "github") {
      return await authGitHub(args, runtime, deps);
    }

    if (command === "add") return await addItem(args, runtime, deps);
    if (command === "list") return await listItems(args, runtime, deps);
    if (command === "show") return await showItem(args, runtime, deps);
    if (command === "assign") return await assignMe(args, runtime, deps);
    if (command === "status") return await setItemStatus(args, runtime, deps);
    if (command === "import") return await importItems(args, runtime, deps);
    if (command === "export") return await exportItems(args, runtime, deps);
    if (command === "serve") return await serve(args, runtime);

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
  runtime: CliRuntime = {
    cwd: process.cwd(),
    env: process.env,
    writeStdout: (text) => process.stdout.write(text),
  },
): Promise<void> {
  const result = await runCli(args, runtime);

  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  process.exitCode = result.exitCode;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  await main();
}
