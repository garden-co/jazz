import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { mkdtempSync, rmSync } from "node:fs";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createAccountManager } from "jazz-tools";

import {
  deploy,
  startLocalJazzServer,
  startTestJwtIssuer,
  type LocalJazzServerHandle,
  type TestJwtIssuerHandle,
} from "jazz-tools/testing";
import permissions from "../permissions.js";
import { app } from "../schema.js";

const EXTERNAL_ISSUER = "https://todo-server.example.test";
const APP_ID = "todo-cli-durable-restart";
const exampleRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const tsxLauncher = join(exampleRoot, "node_modules", ".bin", "tsx");

type RunningChild = {
  process: ChildProcess;
  baseUrl: string;
};

let jwtIssuer: TestJwtIssuerHandle;
let upstream: LocalJazzServerHandle;
async function startUpstream(): Promise<void> {
  jwtIssuer = await startTestJwtIssuer();
  upstream = await startLocalJazzServer({
    appId: APP_ID,
    jwksUrl: jwtIssuer.jwksUrl,
    jwtIssuer: EXTERNAL_ISSUER,
    jwtAudience: jwtIssuer.audience,
  });
  await deploy({
    serverUrl: upstream.url,
    appId: upstream.appId,
    adminSecret: upstream.adminSecret,
    schema: app,
    permissions,
  });
}
async function registerToken(token: string): Promise<void> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId: APP_ID,
    serverUrl: upstream.url,
    env: `todo-cli-${crypto.randomUUID()}`,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  await accounts.registerJWT(token);
}

async function startChild(cwd: string): Promise<RunningChild> {
  const childEnv = {
    ...process.env,
    PORT: "0",
    JAZZ_APP_ID: APP_ID,
    JAZZ_SERVER_URL: upstream.url,
    JAZZ_BACKEND_SECRET: upstream.backendSecret,
    JAZZ_JWKS_URL: jwtIssuer.jwksUrl,
  };
  delete childEnv.DB_PATH;
  const child = spawn(tsxLauncher, [join(exampleRoot, "src", "main.ts")], {
    cwd,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";
  let errorOutput = "";
  const ready = new Promise<string>((resolve, reject) => {
    const onOutput = (chunk: Buffer) => {
      output += chunk.toString();
      const match = output.match(/Todo server listening on (http:\/\/localhost:\d+)/);
      if (match) resolve(match[1]!);
    };
    child.stdout?.on("data", onOutput);
    child.stderr?.on("data", (chunk: Buffer) => {
      errorOutput += chunk.toString();
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      reject(
        new Error(
          `Server exited before readiness (code=${code}, signal=${signal}). Output:\n${output}\n${errorOutput}`,
        ),
      );
    });
  });

  return { process: child, baseUrl: await ready };
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill("SIGINT");
  await once(child, "exit");
}

async function runCli(
  cwd: string,
  args: string[],
  dbPath: string | undefined,
): Promise<{ code: number | null; output: string }> {
  const childEnv = {
    ...process.env,
    PORT: "0",
    JAZZ_APP_ID: APP_ID,
    JAZZ_SERVER_URL: upstream.url,
    JAZZ_BACKEND_SECRET: upstream.backendSecret,
    JAZZ_JWKS_URL: jwtIssuer.jwksUrl,
  };
  if (dbPath === undefined) delete childEnv.DB_PATH;
  else childEnv.DB_PATH = dbPath;
  const child = spawn(tsxLauncher, [join(exampleRoot, "src", "main.ts"), ...args], {
    cwd,
    env: childEnv,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let output = "";
  child.stdout?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  child.stderr?.on("data", (chunk: Buffer) => {
    output += chunk.toString();
  });
  const [code] = await once(child, "exit");
  return { code: code as number | null, output };
}

const invalidCliCases = [
  { name: "unknown options", args: ["--unknown"], dbPath: undefined },
  { name: "missing data path", args: ["--data-path"], dbPath: undefined },
  { name: "empty data path", args: ["--data-path", ""], dbPath: undefined },
  {
    name: "in-memory and data path conflict",
    args: ["--in-memory", "--data-path", "/tmp/todo.db"],
    dbPath: undefined,
  },
  { name: "empty DB_PATH", args: [], dbPath: "" },
] as const;

describe("Todo server CLI", () => {
  beforeAll(async () => {
    await startUpstream();
  });

  afterAll(async () => {
    await upstream?.stop();
    await jwtIssuer?.stop();
  });

  it("persists authenticated todos across fresh CLI processes with the default path", async () => {
    const cwd = mkdtempSync(join(tmpdir(), "jazz-todo-cli-restart-"));
    const token = jwtIssuer.jwtForUser("todo-cli-restart-user", {}, { issuer: EXTERNAL_ISSUER });
    let first: RunningChild | undefined;
    let second: RunningChild | undefined;

    try {
      await registerToken(token);
      first = await startChild(cwd);
      const createResponse = await fetch(`${first.baseUrl}/todos`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ title: "survives process restart" }),
      });
      expect(createResponse.status).toBe(201);
      await stopChild(first.process);

      second = await startChild(cwd);
      const listResponse = await fetch(`${second.baseUrl}/todos`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(listResponse.status).toBe(200);
      const todos = (await listResponse.json()) as Array<{ title: string }>;
      expect(todos.some((todo) => todo.title === "survives process restart")).toBe(true);
    } finally {
      if (first) await stopChild(first.process);
      if (second) await stopChild(second.process);
      rmSync(cwd, { recursive: true, force: true });
    }
  });
  it.each(invalidCliCases)("$name fails before listening", async ({ args, dbPath }) => {
    const cwd = mkdtempSync(join(tmpdir(), "jazz-todo-cli-invalid-"));
    try {
      const result = await runCli(cwd, args, dbPath);
      expect(result.code).not.toBe(0);
      expect(result.output).toContain("Fatal error:");
      expect(result.output).not.toContain("Todo server listening");
    } finally {
      rmSync(cwd, { recursive: true, force: true });
    }
  });
});
