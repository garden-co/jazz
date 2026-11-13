import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { runInspectCommand } from "./cli.js";
import { runMcpServer } from "./mcp.js";

// #5182: without a token, a deployed app's hook refuses the call, and the CLI
// used to pass on only "the app's inspect hook refused this request". The
// caller has no token yet: say so, and how to get one.

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

const refusing = (async () =>
  Response.json(
    { error: "unauthorized", message: "the app's inspect hook refused this request" },
    { status: 401 },
  )) as typeof fetch;

async function configDir() {
  const dir = await mkdtemp(join(tmpdir(), "jazz-inspect-whoami-"));
  cleanups.push(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

describe("a call without a token that the app refuses", () => {
  it("the CLI says there's no token and to log in", async () => {
    let out = "";
    const env = { JAZZ_CONFIG_DIR: await configDir() };
    const code = await runInspectCommand(["whoami", "--app", "https://app.example.com/jazz"], {
      stdout: (text) => (out += text),
      stderr: () => {},
      env,
      fetch: refusing,
    });
    expect(code).toBe(1);
    expect(JSON.parse(out)).toEqual({
      error: "unauthorized",
      message:
        "no Inspector token for https://app.example.com/jazz: run `jazz-tools inspect login --app https://app.example.com/jazz` to ask a person for one (the backend said: the app's inspect hook refused this request)",
    });
  });

  it("the CLI passes a token's refusal on as is", async () => {
    let out = "";
    await runInspectCommand(["whoami", "--app", "https://app.example.com/jazz"], {
      stdout: (text) => (out += text),
      stderr: () => {},
      env: { JAZZ_CONFIG_DIR: await configDir(), JAZZ_INSPECT_TOKEN: "jzi1_expired" },
      fetch: (async () =>
        Response.json(
          { error: "unauthorized", message: "token expired" },
          { status: 401 },
        )) as typeof fetch,
    });
    expect(JSON.parse(out).message).toBe("token expired");
  });

  it("MCP says to call request_access", async () => {
    const input = new PassThrough();
    const lines: any[] = [];
    const served = runMcpServer({
      app: "https://app.example.com",
      env: { JAZZ_CONFIG_DIR: await configDir() },
      input,
      output: (line) => lines.push(JSON.parse(line)),
      fetch: refusing,
    });
    input.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "whoami", arguments: {} } })}\n`,
    );
    input.end();
    await served;
    const result = lines.find((line) => line.id === 1).result;
    expect(result.isError).toBe(true);
    expect(result.content[0].text).toMatch(/no Inspector token for .*call request_access/);
  });
});
