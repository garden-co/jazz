import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { scanClaudeProjects, ClaudeTranscriptWatcher } from "./watcher.js";
import { ClaudeSessionStore } from "./store.js";
import type { ClaudeSessionSummary } from "./parser.js";

interface ServeOptions {
  dataPath: string;
  socketPath: string;
  claudeProjects: string;
  watch: boolean;
}

function expandHome(p: string): string {
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  if (p === "~") return os.homedir();
  return p;
}

function defaultSocketPath(): string {
  return path.join(os.homedir(), "Library/Application Support/Flow/claude-sessions.sock");
}

function defaultDataPath(): string {
  return path.join(os.homedir(), "Library/Caches/Flow/claude-sessions.db");
}

function defaultClaudeProjectsDir(): string {
  return path.join(os.homedir(), ".claude/projects");
}

function readFlag(argv: string[], name: string): string | undefined {
  const eq = `--${name}=`;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === `--${name}`) return argv[i + 1];
    if (a && a.startsWith(eq)) return a.slice(eq.length);
  }
  return undefined;
}

function readBooleanFlag(argv: string[], name: string, fallback: boolean): boolean {
  const raw = readFlag(argv, name);
  if (raw === undefined) return fallback;
  return raw === "true" || raw === "1" || raw === "yes";
}

function parseServeOptions(argv: string[]): ServeOptions {
  return {
    dataPath: expandHome(readFlag(argv, "data-path") ?? defaultDataPath()),
    socketPath: expandHome(readFlag(argv, "socket-path") ?? defaultSocketPath()),
    claudeProjects: expandHome(readFlag(argv, "claude-projects") ?? defaultClaudeProjectsDir()),
    watch: readBooleanFlag(argv, "watch", true),
  };
}

function summaryToResult(s: ClaudeSessionSummary): Record<string, unknown> {
  return {
    id: s.sessionId,
    sessionId: s.sessionId,
    cwd: s.cwd,
    projectRoot: s.projectRoot,
    branch: s.gitBranch ?? "-",
    entrypoint: s.entrypoint,
    version: s.version,
    updatedAt: Math.floor(s.updatedAtUnixMs / 1000),
    updatedAtUnixMs: s.updatedAtUnixMs,
    firstUserMessage: s.firstUserMessage,
    latestUserMessage: s.latestUserMessage,
    latestAssistantMessage: s.latestAssistantMessage,
    userTurnCount: s.userTurnCount,
    assistantTurnCount: s.assistantTurnCount,
    totalEntries: s.totalEntries,
    transcriptPath: s.transcriptPath,
    preview: s.latestAssistantMessage ?? s.latestUserMessage ?? s.firstUserMessage ?? "",
  };
}

interface Request {
  id?: string | number;
  method?: string;
  sessionId?: string;
  projectRoot?: string;
  query?: string;
  limit?: number;
}

function dispatch(store: ClaudeSessionStore, options: ServeOptions, req: Request): Record<string, unknown> {
  const method = req.method;
  const id = req.id;

  const wrap = (result: unknown) => ({ id, ok: true, result });
  const fail = (message: string) => ({ id, ok: false, error: message });

  switch (method) {
    case "health":
      return wrap({
        status: "ok",
        pid: process.pid,
        dataPath: options.dataPath,
        socketPath: options.socketPath,
        claudeProjects: options.claudeProjects,
        watch: options.watch,
        sessionCount: store.count(),
        timestamp: new Date().toISOString(),
      });
    case "get-session": {
      if (!req.sessionId) return fail("get-session requires sessionId");
      const session = store.getSession(req.sessionId);
      return session ? wrap(summaryToResult(session)) : fail("session not found");
    }
    case "list-sessions": {
      if (!req.projectRoot) return fail("list-sessions requires projectRoot");
      const root = expandHome(req.projectRoot);
      const limit = Math.max(1, Math.min(req.limit ?? 10, 100));
      const sessions = store.listForProjectRoot(root, limit).map(summaryToResult);
      return wrap(sessions);
    }
    case "search-sessions": {
      if (!req.query) return fail("search-sessions requires query");
      const limit = Math.max(1, Math.min(req.limit ?? 10, 100));
      const sessions = store.search(req.query, limit).map(summaryToResult);
      return wrap(sessions);
    }
    case "list-recent": {
      const limit = Math.max(1, Math.min(req.limit ?? 20, 200));
      const sessions = store.listRecent(limit).map(summaryToResult);
      return wrap(sessions);
    }
    default:
      return fail(`Unsupported session service method: ${method ?? "undefined"}`);
  }
}

function handleConnection(
  conn: net.Socket,
  store: ClaudeSessionStore,
  options: ServeOptions,
): void {
  let buffer = "";
  conn.setEncoding("utf-8");
  conn.on("data", (chunk: string) => {
    buffer += chunk;
    let newlineIndex = buffer.indexOf("\n");
    while (newlineIndex !== -1) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) {
        let response: Record<string, unknown>;
        try {
          const req = JSON.parse(line) as Request;
          response = dispatch(store, options, req);
        } catch (error) {
          response = { ok: false, error: `invalid JSON: ${(error as Error).message}` };
        }
        conn.write(JSON.stringify(response) + "\n");
      }
      newlineIndex = buffer.indexOf("\n");
    }
  });
  conn.on("error", () => {
    /* ignore transient */
  });
}

async function serve(argv: string[]): Promise<void> {
  const options = parseServeOptions(argv);

  fs.mkdirSync(path.dirname(options.socketPath), { recursive: true });
  if (fs.existsSync(options.socketPath)) {
    try {
      fs.unlinkSync(options.socketPath);
    } catch {
      /* fine */
    }
  }

  const store = new ClaudeSessionStore(options.dataPath);

  console.log(`[claude-sessions] initial scan of ${options.claudeProjects}`);
  const scan = scanClaudeProjects(options.claudeProjects, store);
  console.log(`[claude-sessions] ingested ${scan.ingested}, skipped ${scan.skipped}, total ${store.count()}`);

  let watcher: ClaudeTranscriptWatcher | null = null;
  if (options.watch) {
    watcher = new ClaudeTranscriptWatcher(options.claudeProjects, store);
    watcher.start();
    console.log(`[claude-sessions] watching ${options.claudeProjects}`);
  }

  const server = net.createServer((conn) => handleConnection(conn, store, options));
  server.listen(options.socketPath, () => {
    console.log(`[claude-sessions] listening on ${options.socketPath}`);
  });

  const shutdown = () => {
    console.log("[claude-sessions] shutting down");
    server.close();
    watcher?.stop();
    try {
      store.close();
    } catch {
      /* fine */
    }
    try {
      fs.unlinkSync(options.socketPath);
    } catch {
      /* fine */
    }
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

async function main(): Promise<void> {
  const [, , command, ...rest] = process.argv;
  switch (command) {
    case "serve":
      await serve(rest);
      break;
    default:
      console.error(`usage: cli <serve> [--data-path PATH] [--socket-path PATH] [--claude-projects PATH] [--watch true|false]`);
      process.exit(1);
  }
}

main().catch((error) => {
  console.error("[claude-sessions] fatal:", error);
  process.exit(1);
});
