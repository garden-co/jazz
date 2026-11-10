// `jazz-tools inspect`: read and change an app's data through its own
// backend, as the Inspector does. Every command prints one JSON document on
// stdout, errors included (`{"error": <code>, "message": ...}`), so agents
// can use it directly; `login` prints one JSON line per step so an agent can
// hand the approval URL to a person while the command keeps waiting.

import { spawn } from "node:child_process";
import {
  DEFAULT_APP_URL,
  callInspector,
  parseDuration,
  requestApproval,
  resolveEndpoint,
  tokenStore,
  type InspectEnv,
  type StoredToken,
} from "./client.js";
import { InspectError, INSPECT_TOKEN_PREFIX, type InspectRequest } from "./protocol.js";

export interface InspectCliIo {
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  env: InspectEnv;
  fetch?: typeof fetch;
  openBrowser?: (url: string) => void;
}

export const INSPECT_HELP = `jazz-tools inspect: read and change an app's data through its backend

  login [--write] [--ttl 1h] [--tables a,b] [--label <text>] [--no-browser]
                                Ask a person to approve an agent token in the
                                app's Inspector; stores it for this app URL
  login --token <token>         Store a token the Inspector showed you
  logout                        Forget this app's token
  whoami                        Who the backend says you are, and what you may do
  schema                        Tables, columns, history tables and permissions
  query '<json>' [--as <user>]  Run a query: {"table":"todos","where":{"done":false},
                                "orderBy":[["title","asc"]],"limit":20}
  get <table> <id> [--as <user>]
  history <table> <id>          The row's versions, with who made each
  insert <table> '<values>' [--id <id>] [--as <user>]
  update <table> <id> '<values>' [--as <user>]
  delete <table> <id> [--as <user>]
  sessions                      Sessions connected to this backend instance
  link ['<query json>'] [--table <t>] [--row <id>] [--as <user>]
                                An Inspector link that opens on exactly this
  mcp                           Serve all of the above as an MCP server over stdio,
                                e.g. \`claude mcp add jazz -- npx jazz-tools inspect mcp --app <url>\`

Options:
  --app <url>    The app (or the URL Jazz is served at). Default: $JAZZ_APP_URL,
                 else ${DEFAULT_APP_URL}. Jazz is assumed under /jazz unless the
                 URL names another path.
  --as <user>    Run under this user's permissions (the app's user id)

Locally (NODE_ENV=development, on this machine) no login is needed. Elsewhere,
\`login\` asks a person who passes the app's \`inspect\` hook to approve a token.
Tokens are kept in ~/.config/jazz/inspect-tokens.json; JAZZ_INSPECT_TOKEN wins.
`;

const VALUE_FLAGS = new Set([
  "--app",
  "--as",
  "--ttl",
  "--tables",
  "--label",
  "--token",
  "--id",
  "--table",
  "--row",
]);
const BOOLEAN_FLAGS = new Set(["--write", "--no-browser", "--help", "-h"]);

function parse(args: string[]) {
  const positionals: string[] = [];
  const flags = new Map<string, string | true>();
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!arg.startsWith("-") || arg === "-") {
      positionals.push(arg);
      continue;
    }
    const eq = arg.indexOf("=");
    const name = eq === -1 ? arg : arg.slice(0, eq);
    if (BOOLEAN_FLAGS.has(name)) {
      flags.set(name, true);
      continue;
    }
    if (!VALUE_FLAGS.has(name)) throw new InspectError("bad_request", `unknown option ${name}`);
    const value = eq === -1 ? args[++index] : arg.slice(eq + 1);
    if (value === undefined) throw new InspectError("bad_request", `missing value for ${name}`);
    flags.set(name, value);
  }
  return { positionals, flags };
}

function json(text: string | undefined, what: string): unknown {
  if (text === undefined) throw new InspectError("bad_request", `${what} is missing`);
  try {
    return JSON.parse(text);
  } catch {
    throw new InspectError("bad_request", `${what} is not valid JSON: ${text}`);
  }
}

function defaultOpenBrowser(url: string): void {
  const command =
    process.platform === "darwin" ? "open" : process.platform === "win32" ? "cmd" : "xdg-open";
  const args = process.platform === "win32" ? ["/c", "start", "", url] : [url];
  try {
    const child = spawn(command, args, { stdio: "ignore", detached: true });
    child.on("error", () => {});
    child.unref();
  } catch {
    // Printing the URL is the fallback.
  }
}

/** Run `jazz-tools inspect <args>`; returns the exit code. */
export async function runInspectCommand(args: string[], io: InspectCliIo): Promise<number> {
  const print = (value: unknown) => io.stdout(`${JSON.stringify(value, null, 2)}\n`);
  try {
    const { positionals, flags } = parse(args);
    const [command, ...rest] = positionals;
    if (!command || flags.has("--help") || flags.has("-h")) {
      io.stdout(INSPECT_HELP);
      return command || flags.size ? 0 : 1;
    }
    const flag = (name: string) => {
      const value = flags.get(name);
      return typeof value === "string" ? value : undefined;
    };
    const endpoint = resolveEndpoint(
      flag("--app") ?? io.env.JAZZ_APP_URL?.trim() ?? DEFAULT_APP_URL,
    );
    const store = tokenStore(io.env);
    const call = async (request: InspectRequest) =>
      callInspector(endpoint, request, {
        token: (await store.get(endpoint))?.token,
        fetch: io.fetch,
      });
    const as = flag("--as");
    const asPart = as === undefined ? {} : { as };

    switch (command) {
      case "login": {
        const given = flag("--token");
        if (given !== undefined) {
          if (!given.startsWith(INSPECT_TOKEN_PREFIX)) {
            throw new InspectError("bad_request", "that is not an Inspector agent token");
          }
          const whoami = await callInspector<{
            by: string;
            write: boolean;
            tables?: string[];
            expiresAt?: number;
          }>(endpoint, { op: "whoami" }, { token: given, fetch: io.fetch });
          const stored: StoredToken = {
            token: given,
            expiresAt: whoami.expiresAt ?? Math.floor(Date.now() / 1000) + 3600,
            by: whoami.by,
            write: whoami.write,
            ...(whoami.tables ? { tables: whoami.tables } : {}),
          };
          await store.set(endpoint, stored);
          print({
            loggedIn: true,
            endpoint,
            by: stored.by,
            write: stored.write,
            expiresAt: stored.expiresAt,
          });
          return 0;
        }
        const tables = flag("--tables")
          ?.split(",")
          .map((table) => table.trim())
          .filter(Boolean);
        const ttl = flag("--ttl");
        const pending = await requestApproval(
          endpoint,
          {
            write: flags.has("--write"),
            ...(ttl ? { ttlSeconds: parseDuration(ttl) } : {}),
            ...(tables?.length ? { tables } : {}),
            label: flag("--label") ?? "jazz-tools inspect",
          },
          { store, fetch: io.fetch },
        );
        io.stdout(
          `${JSON.stringify({
            step: "approve",
            url: pending.url,
            message:
              "Open this URL and approve. If the browser runs on another machine, approve there and run `jazz-tools inspect login --token <token>` with the token it shows.",
          })}\n`,
        );
        if (!flags.has("--no-browser")) (io.openBrowser ?? defaultOpenBrowser)(pending.url);
        const stored = await pending.token;
        io.stdout(
          `${JSON.stringify({ step: "done", loggedIn: true, endpoint, by: stored.by, write: stored.write, expiresAt: stored.expiresAt })}\n`,
        );
        return 0;
      }
      case "logout":
        print({ loggedOut: await store.remove(endpoint), endpoint });
        return 0;
      case "whoami":
        print(await call({ op: "whoami" }));
        return 0;
      case "schema":
        print(await call({ op: "schema" }));
        return 0;
      case "query":
        print(
          await call({
            op: "query",
            query: json(rest[0], "the query") as never,
            ...asPart,
          }),
        );
        return 0;
      case "get":
        print(
          await call({
            op: "get",
            table: need(rest[0], "table"),
            id: need(rest[1], "id"),
            ...asPart,
          }),
        );
        return 0;
      case "history":
        print(
          await call({
            op: "history",
            table: need(rest[0], "table"),
            id: need(rest[1], "id"),
          }),
        );
        return 0;
      case "insert": {
        const id = flag("--id");
        print(
          await call({
            op: "write",
            ops: [
              {
                kind: "insert",
                table: need(rest[0], "table"),
                values: json(rest[1], "the values") as Record<string, unknown>,
                ...(id ? { id } : {}),
              },
            ],
            ...asPart,
          }),
        );
        return 0;
      }
      case "update":
        print(
          await call({
            op: "write",
            ops: [
              {
                kind: "update",
                table: need(rest[0], "table"),
                id: need(rest[1], "id"),
                values: json(rest[2], "the values") as Record<string, unknown>,
              },
            ],
            ...asPart,
          }),
        );
        return 0;
      case "delete":
        print(
          await call({
            op: "write",
            ops: [
              {
                kind: "delete",
                table: need(rest[0], "table"),
                id: need(rest[1], "id"),
              },
            ],
            ...asPart,
          }),
        );
        return 0;
      case "sessions":
        print(await call({ op: "sessions" }));
        return 0;
      case "link": {
        const table = flag("--table");
        const row = flag("--row");
        print(
          await call({
            op: "link",
            ...(rest[0] !== undefined ? { query: json(rest[0], "the query") as never } : {}),
            ...(table ? { table } : {}),
            ...(row ? { row } : {}),
            ...asPart,
          }),
        );
        return 0;
      }
      default:
        throw new InspectError("bad_request", `unknown command ${command}; see --help`);
    }
  } catch (error) {
    const failed =
      error instanceof InspectError
        ? error
        : new InspectError("failed", error instanceof Error ? error.message : String(error));
    io.stdout(`${JSON.stringify({ error: failed.code, message: failed.message })}\n`);
    return 1;
  }
}

function need(value: string | undefined, what: string): string {
  if (!value) throw new InspectError("bad_request", `${what} is missing`);
  return value;
}
