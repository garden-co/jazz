import { execFileSync, spawn } from "node:child_process";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { fetchBrief, renderBrief, renderFetchError } from "./brief.js";

const BRIEF = "Set up Jazz (https://jazz.tools) in this project.";

function respond(body: string, init: ResponseInit = {}): typeof fetch {
  return async () => new Response(body, init);
}

describe("fetchBrief", () => {
  it("returns the trimmed Markdown body", async () => {
    const fetchImpl = respond(`\n${BRIEF}\n\n`, { headers: { "content-type": "text/markdown" } });
    await expect(fetchBrief("https://example.test/brief.md", fetchImpl)).resolves.toBe(BRIEF);
  });

  it("rejects non-2xx responses", async () => {
    const fetchImpl = respond("gone", { status: 404 });
    await expect(fetchBrief("https://example.test", fetchImpl)).rejects.toThrow("HTTP 404");
  });

  it("rejects HTML so a misrouted page is never printed as the brief", async () => {
    const fetchImpl = respond("<html></html>", { headers: { "content-type": "text/html" } });
    await expect(fetchBrief("https://example.test", fetchImpl)).rejects.toThrow(/content-type/);
  });

  it("rejects an empty body", async () => {
    await expect(fetchBrief("https://example.test", respond("  \n"))).rejects.toThrow("empty");
  });
});

describe("renderBrief", () => {
  it("prints only the brief for agents", () => {
    expect(renderBrief(BRIEF, false)).toBe(`${BRIEF}\n`);
  });

  it("prefixes one hint line for humans", () => {
    const out = renderBrief(BRIEF, true);
    expect(out).toMatch(/^Paste this into your coding agent/);
    expect(out.endsWith(`\n\n${BRIEF}\n`)).toBe(true);
  });
});

describe("renderFetchError", () => {
  it("names the URL so the agent can fetch it itself", () => {
    const out = renderFetchError("https://jazz.tools/agent-brief.md", new Error("HTTP 503"));
    expect(out).toContain("HTTP 503");
    expect(out).toContain("Fetch https://jazz.tools/agent-brief.md directly");
  });
});

describe("create-jazz bin", () => {
  const packageDir = path.resolve(import.meta.dirname, "..");
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    execFileSync("pnpm", ["build"], { cwd: packageDir });
    server = createServer((req, res) => {
      if (req.url === "/agent-brief.md") {
        res.writeHead(200, { "content-type": "text/markdown; charset=utf-8" });
        res.end(`${BRIEF}\n`);
      } else {
        res.writeHead(404);
        res.end();
      }
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }, 60_000);

  afterAll(() => server?.close());

  function runBin(url: string, args: string[] = []) {
    return new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve) => {
      const child = spawn(process.execPath, ["bin/create-jazz.js", ...args], {
        cwd: packageDir,
        env: { ...process.env, JAZZ_AGENT_BRIEF_URL: url },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk) => (stdout += chunk));
      child.stderr.on("data", (chunk) => (stderr += chunk));
      child.on("close", (code) => resolve({ code, stdout, stderr }));
    });
  }

  it("prints the fetched brief and ignores arguments", async () => {
    const result = await runBin(`${baseUrl}/agent-brief.md`, ["my-app", "--starter", "x"]);
    expect(result).toEqual({ code: 0, stdout: `${BRIEF}\n`, stderr: "" });
  });

  it("exits non-zero with the URL when the fetch fails", async () => {
    const result = await runBin(`${baseUrl}/missing.md`);
    expect(result.code).toBe(1);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain(`Fetch ${baseUrl}/missing.md directly`);
  });
});
