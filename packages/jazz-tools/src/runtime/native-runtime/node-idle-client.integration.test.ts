import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { schema as s } from "../../index.js";
import { deploy, startLocalJazzServer, startTestJazz } from "../../testing/index.js";

// A Node client connected to a server, with nothing to sync, must go idle.
// Its server pump used to re-arm itself after every core tick, so an idle
// client armed the 16ms server-pump timer ~60 times a second forever.

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

afterEach(() => {
  vi.restoreAllMocks();
});

describe("an idle Node client", () => {
  it("stops pumping once a connected client has nothing to sync", async () => {
    const app = s.defineApp({ notes: s.table({ label: s.string() }, {}) }, { migrations: "none" });
    const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    const session = await startTestJazz({ app, permissions: {}, core: server });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {},
      });
      const db = session.jazz.db;
      await db.insert(app.notes, { label: "first" }).wait({ tier: "global" });

      // The runtime calls the global `setTimeout` at each use.
      const timers = vi.spyOn(globalThis, "setTimeout");
      const serverPumpTimers = () => timers.mock.calls.filter(([, delay]) => delay === 16).length;
      const expectIdle = async (when: string) => {
        await sleep(1_000); // let the last write's follow-up traffic settle
        const before = serverPumpTimers();
        await sleep(2_000);
        const armed = serverPumpTimers() - before;
        // Before the fix: ~120 in two idle seconds.
        expect(armed, `${when}: 16ms server-pump timers armed while idle`).toBeLessThanOrEqual(3);
      };

      await expectIdle("after the first write settled");

      // Real work still reaches the server, and the client goes idle again.
      await db.insert(app.notes, { label: "second" }).wait({ tier: "global" });
      expect(await db.all(app.notes, { tier: "remote" })).toHaveLength(2);
      await expectIdle("after the second write settled");
    } finally {
      await session.stop();
      await server.stop();
    }
  }, 30_000);

  // The link's outbound wake must not keep Node alive: a process whose
  // client has closed exits, as it did before the wake existed.
  it("lets a Node process exit once its connected client has closed", async () => {
    const app = s.defineApp({ notes: s.table({ label: s.string() }, {}) }, { migrations: "none" });
    const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        schema: app,
        permissions: {},
      });
      const built = (path: string) => new URL(`../../../dist/${path}`, import.meta.url).href;
      const core = { url: server.url, appId: server.appId, adminSecret: server.adminSecret };
      const child = spawn(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          [
            `import { schema as s } from ${JSON.stringify(built("index.js"))};`,
            `import { startTestJazz } from ${JSON.stringify(built("testing/index.js"))};`,
            `const app = s.defineApp({ notes: s.table({ label: s.string() }, {}) }, { migrations: "none" });`,
            `const core = { ...${JSON.stringify(core)}, stop: async () => {} };`,
            `const session = await startTestJazz({ app, permissions: {}, core });`,
            `await session.jazz.db.insert(app.notes, { label: "from the child" }).wait({ tier: "global" });`,
            `await session.stop();`,
            'process.stdout.write("closed\\n");',
          ].join("\n"),
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let output = "";
      let errors = "";
      let closedAt: number | undefined;
      child.stdout.on("data", (chunk: Buffer) => {
        output += chunk.toString();
        if (closedAt === undefined && output.includes("closed")) closedAt = Date.now();
      });
      child.stderr.on("data", (chunk: Buffer) => (errors += chunk.toString()));
      const exit = await new Promise<{ code: number | null; at: number }>((resolve) => {
        const watchdog = setTimeout(() => child.kill("SIGKILL"), 30_000);
        child.once("exit", (code) => {
          clearTimeout(watchdog);
          resolve({ code, at: Date.now() });
        });
      });
      expect(closedAt, `the child closed its client (stderr: ${errors})`).toBeDefined();
      expect(exit.code, `the child exited by itself (stderr: ${errors})`).toBe(0);
      // Before the fix it stayed alive until killed.
      expect(exit.at - closedAt!).toBeLessThan(5_000);
    } finally {
      await server.stop();
    }
  }, 60_000);
});
