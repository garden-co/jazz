import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

describe("finite concurrent local write convergence", () => {
  for (const withGSet of [false, true]) {
    const app = s.defineApp({
      records: s.table(
        {
          title: s.string(),
          archived: s.boolean(),
          revision: s.float(),
          count: s.int().merge("counter"),
          tags: withGSet ? s.array(s.string()).merge("g-set") : s.array(s.string()),
        },
        {},
      ),
    });
    const permissions = s.definePermissions(app, ({ policy }) => policy.records.allowRead.always());
    for (const offline of [false, true]) {
      it(`settles a fresh editor with ${withGSet ? "GSet and Counter" : "Counter"} after concurrent ${offline ? "offline" : "online"} write chains`, async () => {
        const directory = await mkdtemp(join(tmpdir(), "jazz-concurrent-merge-test-"));
        const settings = {
          appId: randomUUID(),
          dataDir: join(directory, "server"),
          adminSecret: randomUUID(),
          backendSecret: randomUUID(),
          allowLocalFirstAuth: true,
        };
        let server = await startLocalJazzServer({ ...settings, schema: app, permissions });
        const sessions: Awaited<ReturnType<typeof createJazzSession>>[] = [];
        async function open(name: string) {
          const session = await createJazzSession({
            appId: settings.appId,
            app,
            permissions,
            serverUrl: server.url,
            driver: { type: "persistent", dataPath: join(directory, name) },
            initial: { backendSecret: settings.backendSecret },
          });
          sessions.push(session);
          const snapshot = session.getSnapshot();
          if (!snapshot.client) throw snapshot.error ?? new Error(snapshot.status);
          return snapshot.client.db;
        }
        try {
          await deploy({ ...settings, schema: app, permissions, serverUrl: server.url });
          const writer = await open("writer");
          const id = randomUUID();
          await writer
            .upsert(app.records, id, {
              title: "seed",
              archived: false,
              revision: 0,
              count: 0,
              tags: ["seed"],
            })
            .wait({ tier: "global" });
          const observer = await open("observer");
          await observer.all(app.records, { tier: "global" });
          const port = server.port;
          if (offline) {
            await server.stop();
            await delay(300);
          }
          async function write(db: typeof writer, title: string) {
            let tail;
            for (let revision = 1; revision <= 100; revision++) {
              tail = db.upsert(app.records, id, {
                title,
                archived: false,
                revision,
                ...(offline ? { count: revision } : {}),
                tags: [title],
              });
              await tail.wait({ tier: "local" });
            }
            return () => tail!.wait({ tier: "global" });
          }
          const tails = await Promise.all([write(writer, "left"), write(observer, "right")]);
          if (offline) {
            server = await startLocalJazzServer({ ...settings, port });
            await Promise.all([writer.reconnect(), observer.reconnect()]);
          }
          await Promise.all(tails.map((settle) => settle()));
          await delay(1000);
          const editor = await open("editor");
          expect(await editor.all(app.records, { tier: "global" })).toMatchObject([
            { revision: 100, archived: false },
          ]);
          await editor.update(app.records, id, { archived: true }).wait({ tier: "global" });
          expect(await editor.all(app.records, { tier: "global" })).toMatchObject([
            { revision: 100, archived: true },
          ]);
          const [settled] = await editor.all(app.records, { tier: "global" });
          expect(settled.count).toBe(offline ? 200 : 0);
          if (withGSet)
            expect(settled.tags).toEqual(expect.arrayContaining(["seed", "left", "right"]));
          // A second ordinary child also settles; no application writes remain.
          await editor.update(app.records, id, { title: "settled" }).wait({ tier: "global" });
          await expect
            .poll(() => writer.all(app.records, { tier: "global" }))
            .toMatchObject([{ title: "settled", revision: 100, archived: true }]);
        } finally {
          for (const session of sessions.reverse()) {
            await session.getSnapshot().client?.shutdown({ waitForSync: false });
            await session.close();
          }
          await server.stop();
          await rm(directory, { recursive: true, force: true });
        }
      }, 30_000);
    }
  }
});
