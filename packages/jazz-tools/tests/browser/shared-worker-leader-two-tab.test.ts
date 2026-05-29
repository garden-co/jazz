import { describe, expect, it } from "vitest";
import { createDb } from "../../src/runtime/db.js";
import { schema as s } from "../../src/index.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./remote-browser-db.js";
import { leaderSupported } from "./fixtures/leader-support.js";

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { todos } = app;

describe.skipIf(!leaderSupported)("shared-worker-leader two tabs", () => {
  it("Tab A insert is visible to Tab B through the leader-minted MessagePort", async () => {
    const appId = `leader-two-tab-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `two-tab-${Date.now()}`;
    const remoteId = `remote-${Math.random().toString(36).slice(2, 6)}`;

    const localDb = await createDb({
      appId,
      driver: { type: "persistent", dbName },
    });
    await createRemoteBrowserDb({
      id: remoteId,
      appId,
      dbName,
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
    });

    const title = `from-a-${Date.now()}`;
    await localDb.insert(todos, { title, done: false }).wait({ tier: "local" });

    const rows = await waitForRemoteBrowserDbTitle({ id: remoteId, title, timeoutMs: 20000 });
    expect(rows.some((r) => r.title === title)).toBe(true);

    await closeRemoteBrowserDb(remoteId);
    await localDb.shutdown();
  }, 30000);
});
