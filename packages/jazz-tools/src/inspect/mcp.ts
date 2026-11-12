// `jazz-tools inspect mcp`: the Inspector protocol as an MCP server over stdio, for
// agents whose host speaks MCP. It calls the same `<basePath>/inspect/api` as
// the Inspector UI and `jazz-tools inspect`, with the same token store, so a
// token approved for one works for the others.
//
// Add it to an agent with e.g. `claude mcp add jazz -- npx jazz-tools inspect mcp --app <url>`.

import { createInterface } from "node:readline";
import {
  DEFAULT_APP_URL,
  callInspector,
  requestApproval,
  resolveEndpoint,
  tokenStore,
  type InspectEnv,
  type PendingApproval,
} from "./client.js";
import { InspectError, type InspectRequest } from "./protocol.js";

const SERVER_INFO = { name: "jazz-inspect", version: "1" };
const LATEST_PROTOCOL = "2025-06-18";

const AS = {
  type: "string",
  description:
    "Run under this user's permissions (the app's user id), to see what they see. Omit to use the backend's authority (every row).",
};
const CLAIMS = {
  type: "object",
  description:
    'With as: the user\'s session claims, as their sign-in would carry them, e.g. {"role":"manager"}. Policies that read claims see none unless you pass them; answers carry a warning when that may matter.',
};
const QUERY = {
  type: "object",
  description:
    'A query: {"table":"todos","where":{"done":false,"title":{"contains":"milk"}},"orderBy":[["title","asc"]],"limit":20}. select defaults to every column; add provenance columns like "$createdBy" or "$updatedAt". Query history.<table> for row history.',
  properties: {
    table: { type: "string" },
    where: { type: "object" },
    select: { type: "array", items: { type: "string" } },
    orderBy: { type: "array", items: { type: "array" } },
    limit: { type: "number" },
    offset: { type: "number" },
  },
  required: ["table"],
};

interface Tool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export const MCP_TOOLS: Tool[] = [
  {
    name: "whoami",
    description:
      "Who the app's backend says this agent is, whether it may write, when its token expires, and the Inspector URL. Call first; if it fails with unauthorized, call request_access.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "request_access",
    description:
      "Ask a person to approve an agent token. Returns a URL: give it to the person and wait for them to approve in their browser, then call whoami. Read-only unless write is true.",
    inputSchema: {
      type: "object",
      properties: {
        write: {
          type: "boolean",
          description: "Ask to be allowed to write too",
        },
        ttlSeconds: {
          type: "number",
          description: "How long the token lasts (default 3600)",
        },
        tables: {
          type: "array",
          items: { type: "string" },
          description: "Limit to these tables",
        },
        reason: {
          type: "string",
          description: "Shown to the person who approves",
        },
      },
    },
  },
  {
    name: "schema",
    description: "The app's tables and columns, their history tables, and its permissions.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "query",
    description:
      "Run a query against the app's live data. Answers carry an inspectorUrl: link it to show the person exactly these rows.",
    inputSchema: {
      type: "object",
      properties: { query: QUERY, as: AS, claims: CLAIMS },
      required: ["query"],
    },
  },
  {
    name: "get",
    description: "One row by id, with who created and last changed it.",
    inputSchema: {
      type: "object",
      properties: { table: { type: "string" }, id: { type: "string" }, as: AS, claims: CLAIMS },
      required: ["table", "id"],
    },
  },
  {
    name: "history",
    description: "Every version of one row, oldest first, with who wrote each and when.",
    inputSchema: {
      type: "object",
      properties: { table: { type: "string" }, id: { type: "string" } },
      required: ["table", "id"],
    },
  },
  {
    name: "write",
    description:
      "Insert, update or delete rows in one transaction. Needs a token that allows writes. Writes change real data: say what you will change and why before calling.",
    inputSchema: {
      type: "object",
      properties: {
        ops: {
          type: "array",
          description:
            'e.g. [{"kind":"update","table":"todos","id":"…","values":{"done":true}}, {"kind":"insert","table":"todos","values":{…}}, {"kind":"delete","table":"todos","id":"…"}]',
          items: { type: "object" },
        },
        as: AS,
        claims: CLAIMS,
      },
      required: ["ops"],
    },
  },
  {
    name: "sessions",
    description: "Sync sessions connected to the backend instance that answers.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "explain",
    description:
      "Which policy allowed or denied each row of a query for a user (not supported yet).",
    inputSchema: {
      type: "object",
      properties: { query: QUERY, as: AS, claims: CLAIMS },
      required: ["query", "as"],
    },
  },
  {
    name: "inspector_link",
    description:
      "An Inspector URL that opens on a query, a table or a row, to show a person what you mean. It carries no credential.",
    inputSchema: {
      type: "object",
      properties: {
        query: QUERY,
        table: { type: "string" },
        row: { type: "string" },
        as: AS,
        claims: CLAIMS,
      },
    },
  },
];

type Json = Record<string, unknown>;

/** Serve MCP on `input`/`output` (stdio by default) until `input` ends. */
export async function runMcpServer(options: {
  app?: string;
  env?: InspectEnv;
  input?: NodeJS.ReadableStream;
  output?: (line: string) => void;
  fetch?: typeof fetch;
}): Promise<void> {
  const env = options.env ?? process.env;
  const endpoint = resolveEndpoint(options.app ?? env.JAZZ_APP_URL?.trim() ?? DEFAULT_APP_URL);
  const store = tokenStore(env);
  const write = options.output ?? ((line: string) => process.stdout.write(`${line}\n`));
  let pending: PendingApproval | undefined;

  const call = async (request: InspectRequest) =>
    callInspector(endpoint, request, {
      token: (await store.get(endpoint))?.token,
      fetch: options.fetch,
    });

  async function callTool(name: string, args: Json): Promise<unknown> {
    const claims =
      args.claims && typeof args.claims === "object" && !Array.isArray(args.claims)
        ? (args.claims as Record<string, unknown>)
        : undefined;
    if (args.claims !== undefined && typeof args.as !== "string") {
      throw new InspectError("bad_request", "claims goes with as (the user they belong to)");
    }
    const as =
      typeof args.as === "string" ? { as: claims ? { userId: args.as, claims } : args.as } : {};
    switch (name) {
      case "whoami":
        return call({ op: "whoami" });
      case "request_access": {
        pending?.cancel();
        pending = await requestApproval(
          endpoint,
          {
            write: args.write === true,
            ...(typeof args.ttlSeconds === "number" ? { ttlSeconds: args.ttlSeconds } : {}),
            ...(Array.isArray(args.tables) ? { tables: args.tables as string[] } : {}),
            label: typeof args.reason === "string" ? args.reason : "MCP agent",
          },
          { store, fetch: options.fetch },
        );
        return {
          approveAt: pending.url,
          message:
            "Give this URL to the person and ask them to approve; then call whoami. If their browser runs on another machine, the page shows a token: they can run `jazz-tools inspect login --token <token>` here.",
        };
      }
      case "schema":
        return call({ op: "schema" });
      case "query":
        return call({ op: "query", query: args.query as never, ...as });
      case "get":
        return call({
          op: "get",
          table: String(args.table),
          id: String(args.id),
          ...as,
        });
      case "history":
        return call({
          op: "history",
          table: String(args.table),
          id: String(args.id),
        });
      case "write":
        return call({ op: "write", ops: args.ops as never, ...as });
      case "sessions":
        return call({ op: "sessions" });
      case "explain":
        return call({
          op: "explain",
          query: args.query as never,
          as: as.as ?? String(args.as),
        });
      case "inspector_link":
        return call({
          op: "link",
          ...(args.query ? { query: args.query as never } : {}),
          ...(typeof args.table === "string" ? { table: args.table } : {}),
          ...(typeof args.row === "string" ? { row: args.row } : {}),
          ...as,
        });
      default:
        throw new InspectError("bad_request", `unknown tool ${name}`);
    }
  }

  async function respond(message: Json): Promise<Json | null> {
    const id = message.id;
    const method = message.method;
    const result = (value: unknown) => ({ jsonrpc: "2.0", id, result: value });
    if (typeof method !== "string") return null;
    // Notifications (no id) get no answer.
    if (id === undefined || id === null) return null;
    switch (method) {
      case "initialize": {
        const params = (message.params ?? {}) as Json;
        return result({
          protocolVersion:
            typeof params.protocolVersion === "string" ? params.protocolVersion : LATEST_PROTOCOL,
          capabilities: { tools: {} },
          serverInfo: SERVER_INFO,
          instructions: `Read and change the live data of the Jazz app at ${endpoint} through its backend. Start with whoami; query answers carry inspectorUrl links to show a person what you found.`,
        });
      }
      case "ping":
        return result({});
      case "tools/list":
        return result({ tools: MCP_TOOLS });
      case "tools/call": {
        const params = (message.params ?? {}) as Json;
        try {
          const value = await callTool(String(params.name), (params.arguments ?? {}) as Json);
          return result({
            content: [{ type: "text", text: JSON.stringify(value, null, 2) }],
          });
        } catch (error) {
          const failed =
            error instanceof InspectError
              ? error
              : new InspectError("failed", error instanceof Error ? error.message : String(error));
          return result({
            content: [
              {
                type: "text",
                text: JSON.stringify({
                  error: failed.code,
                  message: failed.message,
                }),
              },
            ],
            isError: true,
          });
        }
      }
      default:
        return {
          jsonrpc: "2.0",
          id,
          error: { code: -32601, message: `unknown method ${method}` },
        };
    }
  }

  const lines = createInterface({
    input: options.input ?? process.stdin,
    crlfDelay: Infinity,
  });
  const inFlight = new Set<Promise<void>>();
  for await (const line of lines) {
    if (!line.trim()) continue;
    let message: Json;
    try {
      message = JSON.parse(line) as Json;
    } catch {
      write(
        JSON.stringify({
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "parse error" },
        }),
      );
      continue;
    }
    const task = respond(message).then((reply) => {
      if (reply) write(JSON.stringify(reply));
    });
    inFlight.add(task);
    void task.finally(() => inFlight.delete(task));
  }
  await Promise.all(inFlight);
  pending?.cancel();
}
