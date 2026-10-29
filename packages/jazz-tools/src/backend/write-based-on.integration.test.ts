import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createJazzSession } from "./index.js";

const app = s.defineApp({
  stops: s.table({ notes: s.string().merge("text") }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => policy.stops.allowRead.always());

async function withTwoEditors(run: (ada: Db, bo: Db, id: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "jazz-based-on-test-"));
  const settings = {
    appId: randomUUID(),
    dataDir: join(directory, "server"),
    adminSecret: randomUUID(),
    backendSecret: randomUUID(),
    allowLocalFirstAuth: true,
  };
  const server = await startLocalJazzServer({ ...settings, schema: app, permissions });
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
    const ada = await open("ada");
    const id = randomUUID();
    await ada.upsert(app.stops, id, { notes: "Load-in 2pm. Doors 8pm." }).wait({ tier: "global" });
    const bo = await open("bo");
    await run(ada, bo, id);
  } finally {
    for (const session of sessions.reverse()) {
      await session.getSnapshot().client?.shutdown({ waitForSync: false });
      await session.close();
    }
    await server.stop();
    await rm(directory, { recursive: true, force: true });
  }
}

type Db = Parameters<Parameters<typeof withTwoEditors>[0]>[0];

describe("writes based on the row a form opened", () => {
  it("keep edits that arrived while the form was open", async () => {
    await withTwoEditors(async (ada, bo, id) => {
      const [opened] = await ada.all(app.stops.select("notes", "$version"), { tier: "remote" });
      expect(typeof opened!.$version).toBe("bigint");
      await bo.all(app.stops, { tier: "remote" });
      await bo
        .update(app.stops, id, { notes: "Load-in 2pm. Soundcheck 6pm. Doors 8pm." })
        .wait({ tier: "global" });
      await expect
        .poll(async () => (await ada.all(app.stops, { tier: "remote" }))[0]?.notes)
        .toBe("Load-in 2pm. Soundcheck 6pm. Doors 8pm.");

      await ada
        .update(app.stops, id, { notes: "Load-in 1pm. Doors 9pm." }, { basedOn: opened })
        .wait({ tier: "global" });

      await expect
        .poll(async () => (await bo.all(app.stops, { tier: "remote" }))[0]?.notes)
        .toBe("Load-in 1pm. Soundcheck 6pm. Doors 9pm.");
    });
  }, 30_000);

  it("replace edits the form never showed when no base is named", async () => {
    await withTwoEditors(async (ada, bo, id) => {
      await bo.all(app.stops, { tier: "remote" });
      await bo
        .update(app.stops, id, { notes: "Load-in 2pm. Soundcheck 6pm. Doors 8pm." })
        .wait({ tier: "global" });
      await expect
        .poll(async () => (await ada.all(app.stops, { tier: "remote" }))[0]?.notes)
        .toBe("Load-in 2pm. Soundcheck 6pm. Doors 8pm.");

      await ada
        .update(app.stops, id, { notes: "Load-in 1pm. Doors 9pm." })
        .wait({ tier: "global" });

      await expect
        .poll(async () => (await bo.all(app.stops, { tier: "remote" }))[0]?.notes)
        .toBe("Load-in 1pm. Doors 9pm.");
    });
  }, 30_000);

  it("leaves $version out of ordinary rows and rejects rows read without it", async () => {
    await withTwoEditors(async (ada, _bo, id) => {
      const [row] = await ada.all(app.stops, { tier: "remote" });
      expect(row).not.toHaveProperty("$version");
      expect(() => ada.update(app.stops, id, { notes: "x" }, { basedOn: row as never })).toThrow(
        'basedOn needs a row read with "$version" selected.',
      );
    });
  }, 30_000);
});
