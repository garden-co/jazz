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

describe.skipIf(!leaderSupported)("shared-worker-leader tab close", () => {
  it("closing one of three tabs leaves the other two unaffected (no re-election)", async () => {
    const appId = `leader-close-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `tab-close-${Date.now()}`;
    const remoteB = `remote-b-${Math.random().toString(36).slice(2, 6)}`;
    const remoteC = `remote-c-${Math.random().toString(36).slice(2, 6)}`;
    const schemaJson = JSON.stringify(app.wasmSchema);

    const localDb = await createDb({
      appId,
      driver: { type: "persistent", dbName },
    });
    await createRemoteBrowserDb({ id: remoteB, appId, dbName, table: "todos", schemaJson });
    await createRemoteBrowserDb({ id: remoteC, appId, dbName, table: "todos", schemaJson });

    // Close tab C. In leader mode the leader is the SharedWorker itself, so
    // this must NOT trigger any re-election — B keeps following uninterrupted.
    await closeRemoteBrowserDb(remoteC);

    const title = `after-close-${Date.now()}`;
    await localDb.insert(todos, { title, done: false }).wait({ tier: "local" });

    const rows = await waitForRemoteBrowserDbTitle({ id: remoteB, title, timeoutMs: 20000 });
    expect(rows.some((r) => r.title === title)).toBe(true);

    await closeRemoteBrowserDb(remoteB);
    await localDb.shutdown();
  }, 30000);
});
