import { pathToFileURL } from "node:url";
import { join } from "node:path";
import { writeConfig } from "./config.js";
import { openRepository as defaultOpenRepository } from "./db.js";
import { exportMarkdownTodo, importMarkdownTodo } from "./domain/markdown.js";
import type { IssueItem, ItemKind, ItemStatus, ListedItem, ListFilters } from "./repository.js";
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

export interface CliDependencies {
  openRepository?: typeof defaultOpenRepository;
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

    if (command === "add") return await addItem(args, runtime, deps);
    if (command === "list") return await listItems(args, runtime, deps);
    if (command === "show") return await showItem(args, runtime, deps);
    if (command === "assign") return await assignMe(args, runtime, deps);
    if (command === "status") return await setItemStatus(args, runtime, deps);
    if (command === "import") return await importItems(args, runtime, deps);
    if (command === "export") return await exportItems(args, runtime, deps);

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
