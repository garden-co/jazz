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
import {
  DEFAULT_ROW_LIMIT,
  InspectError,
  MAX_ROW_LIMIT,
  type InspectAs,
  type InspectRequest,
} from "./protocol.js";
import { WHERE_OPERATORS } from "../where-operators.js";

const SERVER_INFO = { name: "jazz-inspect", version: "1" };
const LATEST_PROTOCOL = "2025-06-18";

const AS = {
  description:
    'Run under this user\'s permissions, to see what they see: the app\'s user id, or { "userId": …, "claims": {…} }. Omit to use the backend\'s authority (every row). Anything else is refused.',
  oneOf: [
    { type: "string", minLength: 1 },
    {
      type: "object",
      properties: { userId: { type: "string", minLength: 1 }, claims: { type: "object" } },
      required: ["userId"],
      additionalProperties: false,
    },
  ],
};
const CLAIMS = {
  type: "object",
  description:
    'With as: the user\'s session claims, as their sign-in would carry them, e.g. {"role":"manager"}. Policies that read claims see none unless you pass them; answers carry a warning when that may matter.',
};
const QUERY = {
  type: "object",
  description:
    'A query: {"table":"todos","where":{"done":false,"title":{"contains":"milk"}},"orderBy":[["title","asc"]],"limit":20}. Query history.<table> for a row\'s versions.',
  properties: {
    table: {
      type: "string",
      description: 'A table from schema, or "history.<table>" for its row history.',
    },
    where: {
      type: "object",
      description: `Column → value (equals), or column → { operator: operand } with operators ${WHERE_OPERATORS.join(", ")} (which apply depends on the column's type; in/notIn take an array, isNull a boolean). Several columns must all match. Values are JSON: timestamps as ISO strings, 64-bit integers as strings, bytes as { "$bytes": "<base64>" }.`,
      additionalProperties: true,
    },
    select: {
      type: "array",
      items: { type: "string" },
      description:
        'Columns to answer with. Default: every column ("*"). Add provenance columns "$createdBy", "$createdAt", "$updatedBy" or "$updatedAt".',
    },
    orderBy: {
      type: "array",
      description: 'Sort keys, first one first: [["title","asc"], ["$createdAt","desc"]].',
      items: {
        type: "array",
        prefixItems: [{ type: "string" }, { type: "string", enum: ["asc", "desc"] }],
        minItems: 2,
        maxItems: 2,
      },
    },
    limit: {
      type: "integer",
      minimum: 1,
      maximum: MAX_ROW_LIMIT,
      description: `Rows to answer with: default ${DEFAULT_ROW_LIMIT}, at most ${MAX_ROW_LIMIT}. The answer says truncated when more matched.`,
    },
    offset: { type: "integer", minimum: 0, description: "Rows to skip, for paging." },
  },
  required: ["table"],
  additionalProperties: false,
};

const VALUES = {
  type: "object",
  description:
    'Column → value, as JSON (timestamps as ISO strings, 64-bit integers as strings, bytes as { "$bytes": "<base64>" }).',
  additionalProperties: true,
};
const WRITE_OP = {
  oneOf: [
    {
      type: "object",
      description: "Insert a row; id is optional (a new one is made).",
      properties: {
        kind: { const: "insert" },
        table: { type: "string" },
        values: VALUES,
        id: { type: "string" },
      },
      required: ["kind", "table", "values"],
      additionalProperties: false,
    },
    {
      type: "object",
      description: "Change some columns of a row.",
      properties: {
        kind: { const: "update" },
        table: { type: "string" },
        id: { type: "string" },
        values: VALUES,
      },
      required: ["kind", "table", "id", "values"],
      additionalProperties: false,
    },
    {
      type: "object",
      description: "Delete a row.",
      properties: {
        kind: { const: "delete" },
        table: { type: "string" },
        id: { type: "string" },
      },
      required: ["kind", "table", "id"],
      additionalProperties: false,
    },
  ],
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
    description:
      'One row by id, with who created and last changed it. A value too large to list shows as {"$large": {kind, size, preview}}: pass column to get that value (from/to for part of a bytes or text value).',
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string" },
        id: { type: "string" },
        column: { type: "string", description: "Answer only this column's value, whole." },
        from: {
          type: "integer",
          minimum: 0,
          description: "With column: where to start (bytes for bytes, UTF-16 code units for text).",
        },
        to: { type: "integer", minimum: 0, description: "With column: where to stop (exclusive)." },
        as: AS,
        claims: CLAIMS,
      },
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
            'Between 1 and 1000 writes, each with kind insert, update or delete: [{"kind":"update","table":"todos","id":"…","values":{"done":true}}, {"kind":"insert","table":"todos","values":{…}}, {"kind":"delete","table":"todos","id":"…"}]',
          items: WRITE_OP,
          minItems: 1,
          maxItems: 1000,
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
      "Why a user can or can't read each row of a query: every row the query matches (with the backend's authority), whether the user may read it, and which clauses of the table's read policy allowed it. Clauses are the arms of the policy's top-level or, numbered from 0 and rendered as text. Pass the user's claims if the policy reads them.",
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

/**
 * A tool's `as` (and `claims`) as the API takes it. `as` is a user id, or
 * `{ userId, claims? }` as the HTTP API spells it; anything else is refused.
 * Never dropped: a call that names a user must not run with the backend's
 * authority instead.
 */
export function asArgument(args: Json): { as?: InspectAs } {
  const { as, claims } = args;
  const refuse = (message: string) => new InspectError("bad_request", message);
  if (as === undefined) {
    if (claims !== undefined) throw refuse("claims goes with as (the user they belong to)");
    return {};
  }
  if (typeof as === "string") {
    if (!as.trim()) throw refuse("as must name a user id");
    // Claims go to the backend as given: it refuses ones that aren't an object.
    return {
      as: claims === undefined ? as : { userId: as, claims: claims as Record<string, unknown> },
    };
  }
  if (as && typeof as === "object" && !Array.isArray(as)) {
    const { userId, claims: inner, ...rest } = as as Record<string, unknown>;
    const unknown = Object.keys(rest);
    if (unknown.length) {
      throw refuse(
        `as has unknown field(s) ${unknown.join(", ")}: give a user id, or { userId, claims }`,
      );
    }
    if (typeof userId !== "string" || !userId.trim()) {
      throw refuse("as.userId must name a user id");
    }
    if (inner !== undefined && claims !== undefined) {
      throw refuse("give the claims once: in as, or next to it");
    }
    const given = inner ?? claims;
    return {
      as: given === undefined ? userId : { userId, claims: given as Record<string, unknown> },
    };
  }
  throw refuse("as must be a user id, or { userId, claims }");
}

function requireAs(as: { as?: InspectAs }): InspectAs {
  if (as.as === undefined)
    throw new InspectError("bad_request", "explain needs as: the user to explain for");
  return as.as;
}

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
      withoutToken: "call request_access to ask a person for one",
    });

  async function callTool(name: string, args: Json): Promise<unknown> {
    const as = asArgument(args);
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
          ...(typeof args.column === "string" ? { column: args.column } : {}),
          ...(typeof args.from === "number" ? { from: args.from } : {}),
          ...(typeof args.to === "number" ? { to: args.to } : {}),
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
          as: requireAs(as),
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
