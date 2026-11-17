// The Rust todo servers (todo-server-rs and its docs copy) as backend
// clients of a TypeScript main backend: each runs against a stock
// `createJazz` serving its own jazz.app.json and permissions, with a scoped
// machine token, and its REST API's writes reach the main backend.
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SCOPED_SUB, startMainBackend, type MainBackend } from "../main-backend.ts";
import { cargoBin, examples } from "./cargo.ts";

const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!().catch(() => undefined);
});

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address() as { port: number };
      probe.close(() => resolve(port));
    });
  });
}

async function until<T>(
  what: string,
  probe: () => Promise<T | undefined>,
  ms = 30_000,
): Promise<T> {
  const deadline = Date.now() + ms;
  for (;;) {
    const found = await probe().catch(() => undefined);
    if (found !== undefined) return found;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

async function runServer(bin: string, backend: MainBackend): Promise<string> {
  const port = await freePort();
  const dir = mkdtempSync(join(tmpdir(), "todo-server-"));
  writeFileSync(join(dir, "token"), backend.mint({ sub: SCOPED_SUB }));
  let output = "";
  const child: ChildProcess = spawn(bin, [], {
    env: {
      ...process.env,
      JAZZ_SERVER_URL: backend.serverUrl,
      JAZZ_APP_ID: backend.appId,
      JAZZ_TOKEN_FILE: join(dir, "token"),
      TODO_DATA_DIR: join(dir, "data"),
      TODO_PORT: String(port),
      RUST_LOG: process.env.RUST_LOG ?? "warn",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout!.on("data", (chunk) => (output += chunk));
  child.stderr!.on("data", (chunk) => (output += chunk));
  const exited = new Promise((resolve) => child.once("exit", resolve));
  cleanups.push(async () => {
    child.kill("SIGTERM");
    await exited;
  });
  const url = `http://127.0.0.1:${port}`;
  await until(`the server on ${url}\n${output}`, async () => {
    if (child.exitCode !== null) throw new Error(`exited ${child.exitCode}: ${output}`);
    const response = await fetch(`${url}/health`);
    return response.ok ? true : undefined;
  });
  return url;
}

describe.each([
  ["todo-server", "todo-server-rs"],
  ["todo-server-docs", "docs/todo-server-rs"],
])("%s as a scoped backend client", (pkg, dir) => {
  it("writes through its REST API to the TypeScript main backend", async () => {
    const bin = cargoBin(pkg, pkg);
    const backend = await startMainBackend(join(examples, dir));
    cleanups.push(() => backend.close());
    const url = await runServer(bin, backend);

    const created = await fetch(`${url}/todos`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ title: "Written in Rust" }),
    });
    expect(created.status, await created.clone().text()).toBeLessThan(300);
    const todo = (await created.json()) as { id: string; title: string };
    expect(todo.title).toBe("Written in Rust");

    // The main backend has it.
    const row = await until("the todo on the main backend", async () => {
      const [found] = await backend.jazz.db.all(backend.app.todos.where({ id: todo.id }));
      return found;
    });
    expect(row).toMatchObject({ title: "Written in Rust", done: false });
  });

  it("sees the main backend's writes through its REST API", async () => {
    const bin = cargoBin(pkg, pkg);
    const backend = await startMainBackend(join(examples, dir));
    cleanups.push(() => backend.close());
    const url = await runServer(bin, backend);
    // A todo the main backend writes reaches the Rust server's API.
    const write = backend.jazz.db.insert(backend.app.todos, {
      title: "Written in TypeScript",
      done: false,
    });
    await write.wait({ tier: "global" });
    const fromTs = write.value;
    const listed = await until("the TypeScript todo in the Rust API", async () => {
      const todos = (await (await fetch(`${url}/todos`)).json()) as Array<{ id: string }>;
      return todos.some((t) => t.id === fromTs.id) ? todos : undefined;
    });
    expect(listed.map((t) => t.id)).toEqual(expect.arrayContaining([fromTs.id]));
  });
});
