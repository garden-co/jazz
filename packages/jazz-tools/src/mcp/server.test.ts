import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { DatabaseSync } from "node:sqlite";

import { runServer } from "./server.js";

// ---------------------------------------------------------------------------
// Fixture — minimal SQLite DB + txt so tools actually return data
// ---------------------------------------------------------------------------

let tmpDir: string;
let dbPath: string;
let txtPath: string;

beforeEach(async () => {
  tmpDir = await mkdtemp(join(tmpdir(), "server-test-"));
  dbPath = join(tmpDir, "docs-index.db");
  txtPath = join(tmpDir, "docs-index.txt");

  // Build minimal DB
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE pages (
      title TEXT NOT NULL, slug TEXT PRIMARY KEY,
      description TEXT NOT NULL, body TEXT NOT NULL
    );
    CREATE VIRTUAL TABLE sections_fts USING fts5(
      title, slug UNINDEXED, section_heading, body, tokenize='unicode61'
    );
  `);
  const ip = db.prepare("INSERT INTO pages VALUES (?,?,?,?)");
  const is = db.prepare("INSERT INTO sections_fts VALUES (?,?,?,?)");
  ip.run(
    "Getting Started",
    "getting-started",
    "Learn how to install Jazz.",
    "This page explains installation.\n\n## Installation\n\nRun npm install jazz-tools.",
  );
  is.run("Getting Started", "getting-started", "", "This page explains installation.");
  is.run("Getting Started", "getting-started", "Installation", "Run npm install jazz-tools.");
  db.close();

  // Build minimal txt
  await writeFile(
    txtPath,
    [
      "===PAGE:getting-started===",
      "TITLE:Getting Started",
      "DESCRIPTION:Learn how to install Jazz.",
      "",
      "This page explains installation.\n\n## Installation\n\nRun npm install jazz-tools.",
    ].join("\n"),
    "utf8",
  );
});

afterEach(async () => {
  await rm(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Test helper: write messages to a PassThrough, run server, collect responses
// ---------------------------------------------------------------------------

async function exchange(messages: object[]): Promise<Array<Record<string, unknown>>> {
  const input = new PassThrough();
  const output = new PassThrough();

  const chunks: Buffer[] = [];
  output.on("data", (chunk: Buffer) => chunks.push(chunk));

  for (const msg of messages) {
    input.write(JSON.stringify(msg) + "\n");
  }
  input.end();

  await runServer({ input, output, dbPath, txtPath });

  const text = Buffer.concat(chunks).toString("utf8");
  return text
    .split("\n")
    .filter((l) => l.trim())
    .map((l) => JSON.parse(l));
}

async function exchangeOne(messages: object[]): Promise<Record<string, unknown>> {
  const responses = await exchange(messages);
  expect(responses).toHaveLength(1);
  return responses[0]!;
}

// ---------------------------------------------------------------------------
// Protocol — lifecycle
// ---------------------------------------------------------------------------

describe("initialize", () => {
  it("responds with protocolVersion, capabilities.tools, and serverInfo", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 1, method: "initialize", params: {} }]);
    expect(res.id).toBe(1);
    expect((res.result as any).protocolVersion).toBeDefined();
    expect((res.result as any).capabilities.tools).toBeDefined();
    expect((res.result as any).serverInfo.name).toBeDefined();
  });
});

describe("initialized notification", () => {
  it("produces no response (notification has no id)", async () => {
    const responses = await exchange([
      { jsonrpc: "2.0", id: 1, method: "initialize", params: {} },
      { jsonrpc: "2.0", method: "initialized" }, // notification — no id
    ]);
    // Only the initialize response should be present
    expect(responses).toHaveLength(1);
    expect(responses[0]!.id).toBe(1);
  });
});

describe("ping", () => {
  it("responds with empty result", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 2, method: "ping" }]);
    expect(res.id).toBe(2);
    expect(res.result).toEqual({});
  });
});

describe("unknown method", () => {
  it("returns JSON-RPC error -32601", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 9, method: "no/such/method" }]);
    expect(res.id).toBe(9);
    expect((res.error as any).code).toBe(-32601);
  });
});

describe("malformed JSON", () => {
  it("returns JSON-RPC parse error -32700", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks: Buffer[] = [];
    output.on("data", (c: Buffer) => chunks.push(c));

    input.write("this is not json\n");
    input.end();

    await runServer({ input, output, dbPath, txtPath });

    const [res] = Buffer.concat(chunks)
      .toString("utf8")
      .split("\n")
      .filter(Boolean)
      .map((l) => JSON.parse(l));
    expect((res.error as any).code).toBe(-32700);
  });
});

// ---------------------------------------------------------------------------
// tools/list
// ---------------------------------------------------------------------------

describe("tools/list", () => {
  it("returns an array of three tools", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 3, method: "tools/list" }]);
    const tools = (res.result as any).tools as Array<{ name: string }>;
    expect(tools).toHaveLength(3);
    const names = tools.map((t) => t.name);
    expect(names).toContain("search_docs");
    expect(names).toContain("get_doc");
    expect(names).toContain("list_pages");
  });

  it("each tool has a name, description, and inputSchema", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 3, method: "tools/list" }]);
    const tools = (res.result as any).tools as Array<{
      name: string;
      description: string;
      inputSchema: { type: string; properties: object };
    }>;
    for (const tool of tools) {
      expect(typeof tool.name).toBe("string");
      expect(typeof tool.description).toBe("string");
      expect(tool.inputSchema.type).toBe("object");
      expect(typeof tool.inputSchema.properties).toBe("object");
    }
  });

  it("search_docs has required query param", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 3, method: "tools/list" }]);
    const tools = (res.result as any).tools as Array<any>;
    const searchTool = tools.find((t: any) => t.name === "search_docs");
    expect(searchTool.inputSchema.required).toContain("query");
  });

  it("get_doc has required slug param", async () => {
    const res = await exchangeOne([{ jsonrpc: "2.0", id: 3, method: "tools/list" }]);
    const tools = (res.result as any).tools as Array<any>;
    const getDocTool = tools.find((t: any) => t.name === "get_doc");
    expect(getDocTool.inputSchema.required).toContain("slug");
  });
});

// ---------------------------------------------------------------------------
// tools/call
// ---------------------------------------------------------------------------

describe("tools/call search_docs", () => {
  it("returns content array with text type and ANSI-formatted output", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 4,
        method: "tools/call",
        params: { name: "search_docs", arguments: { query: "installation" } },
      },
    ]);
    const content = (res.result as any).content as Array<{ type: string; text: string }>;
    expect(content[0]!.type).toBe("text");
    // Title and slug appear in output (may be wrapped in ANSI codes)
    expect(content[0]!.text).toContain("Getting Started");
    expect(content[0]!.text).toContain("getting-started");
  });

  it("respects optional limit argument", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 4,
        method: "tools/call",
        params: { name: "search_docs", arguments: { query: "installation", limit: 1 } },
      },
    ]);
    const text = (res.result as any).content[0]!.text as string;
    // With limit 1, only one slug should appear
    const slugOccurrences = text.split("getting-started").length - 1;
    expect(slugOccurrences).toBeLessThanOrEqual(1);
  });
});

describe("tools/call get_doc", () => {
  it("returns ANSI-formatted output with title, description, and body", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 5,
        method: "tools/call",
        params: { name: "get_doc", arguments: { slug: "getting-started" } },
      },
    ]);
    const text = (res.result as any).content[0]!.text as string;
    expect(text).toContain("Getting Started");
    expect(text).toContain("Learn how to install Jazz.");
    expect(text).toContain("npm install jazz-tools");
  });

  it("returns JSON-RPC error when slug param is missing", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 5,
        method: "tools/call",
        params: { name: "get_doc", arguments: {} },
      },
    ]);
    expect((res.error as any).code).toBe(-32602);
  });

  it("returns isError result when slug does not exist", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 5,
        method: "tools/call",
        params: { name: "get_doc", arguments: { slug: "no-such-page" } },
      },
    ]);
    // Must be a successful JSON-RPC result (not a JSON-RPC error)
    // so the model can read the message and recover.
    expect(res.error).toBeUndefined();
    expect((res.result as any).isError).toBe(true);
    const text = (res.result as any).content[0]!.text as string;
    expect(text).toContain("no-such-page");
  });
});

describe("tools/call list_pages", () => {
  it("returns ANSI-formatted output with title, slug, and description per page", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 6,
        method: "tools/call",
        params: { name: "list_pages", arguments: {} },
      },
    ]);
    const text = (res.result as any).content[0]!.text as string;
    expect(text).toContain("Getting Started");
    expect(text).toContain("getting-started");
    expect(text).toContain("Learn how to install Jazz.");
  });
});

describe("tools/call error cases", () => {
  it("unknown tool name returns isError result so the model can recover", async () => {
    const res = await exchangeOne([
      {
        jsonrpc: "2.0",
        id: 7,
        method: "tools/call",
        params: { name: "nonexistent_tool", arguments: {} },
      },
    ]);
    expect(res.error).toBeUndefined();
    expect((res.result as any).isError).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Process lifecycle
// ---------------------------------------------------------------------------

describe("stdin EOF", () => {
  it("runServer promise resolves when input closes", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    input.end();
    await expect(runServer({ input, output, dbPath, txtPath })).resolves.toBeUndefined();
  });
});
