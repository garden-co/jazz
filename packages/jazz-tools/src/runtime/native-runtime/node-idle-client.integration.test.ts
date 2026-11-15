import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { deploy, startLocalJazzServer, startTestJazz, testApp } from "../../testing/index.js";

// A Node client connected to a server, with nothing to sync, must go idle.
// Its server pump used to re-arm itself after every core tick, so an idle
// client armed the 16ms server-pump timer ~60 times a second forever.

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

afterEach(() => {
  vi.restoreAllMocks();
});

describe("an idle Node client", () => {
  it("stops pumping once a connected client has nothing to sync", async () => {
    const app = testApp({ tables: { notes: { columns: { label: "string" } } } });
    const permissions = { format: 1, tables: {} };
    const server = await startLocalJazzServer({ appId: randomUUID(), inMemory: true });
    const session = await startTestJazz({ app, permissions, core: server });
    try {
      await deploy({
        serverUrl: server.url,
        appId: server.appId,
        adminSecret: server.adminSecret,
        app,
        permissions,
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
});
