import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { mkdtempSync, rmSync } from "node:fs";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { startTestJwtIssuer, type TestJwtIssuerHandle } from "jazz-tools/testing";

const EXTERNAL_ISSUER = "https://todo-server.example.test";
const APP_ID = "todo-cli-durable-restart";
const exampleRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const tsxLauncher = join(exampleRoot, "node_modules", ".bin", "tsx");

type RunningChild = {
  process: ChildProcess;
  baseUrl: string;
};

let jwtIssuer: TestJwtIssuerHandle;

async function startChild(cwd: string): Promise<RunningChild> {
  const childEnv = {
    ...process.env,
    PORT: "0",
    JAZZ_APP_ID: APP_ID,
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

describe("Todo server CLI", () => {
  beforeAll(async () => {
    jwtIssuer = await startTestJwtIssuer();
  });

  afterAll(async () => {
    await jwtIssuer?.stop();
  });

  it("persists authenticated todos across fresh CLI processes with the default path", async () => {
    const cwd = mkdtempSync(join(tmpdir(), "jazz-todo-cli-restart-"));
    const token = jwtIssuer.jwtForUser("todo-cli-restart-user", {}, { issuer: EXTERNAL_ISSUER });
    let first: RunningChild | undefined;
    let second: RunningChild | undefined;

    try {
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
});
