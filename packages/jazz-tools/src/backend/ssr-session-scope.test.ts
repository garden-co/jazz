import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzContext } from "./create-jazz-context.js";
import { createSnapshotBuilder } from "./ssr.js";

const app = s.defineApp({
  notes: s.table({
    title: s.string(),
    owner: s.string(),
  }),
});

// A note is readable only by the principal named in its `owner` column.
const permissions = definePermissions(app, ({ policy, session }) => [
  policy.notes.allowRead.where({ owner: session.user_id }),
]);

async function createScopedContext() {
  const appId = randomUUID();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-ssr-scope-"));
  const context = createJazzContext({
    appId,
    app,
    permissions,
    driver: { type: "persistent", dataPath: join(dataRoot, "runtime.db") },
    env: "test",
    userBranch: "main",
    tier: "edge",
  });
  onTestFinished(async () => {
    context.flush();
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });
  return { context, appId };
}

describe("SSR snapshot — session scoping", () => {
  it("prefetches only the scoped session's readable rows, never leaking another principal's data", async () => {
    const { context, appId } = await createScopedContext();

    const seed = context.db(app);
    seed.insert(app.notes, { title: "alice-note", owner: "alice" });
    seed.insert(app.notes, { title: "bob-note", owner: "bob" });

    const aliceDb = context.forSession({ user_id: "alice", claims: {}, authMode: "external" }, app);

    const builder = createSnapshotBuilder({ appId, schema: app, principalId: "alice" });
    await builder.prefetch(aliceDb, app.notes);
    const snapshot = builder.dehydrate();

    const titles = (snapshot.entries[0]?.result as Array<{ title: string }>).map((r) => r.title);
    expect(titles).toEqual(["alice-note"]);
    expect(titles).not.toContain("bob-note");
    expect(snapshot.principalId).toBe("alice");
  });

  it("derives the snapshot principalId from the prefetch Db when none is configured", async () => {
    const { context, appId } = await createScopedContext();

    const seed = context.db(app);
    seed.insert(app.notes, { title: "alice-note", owner: "alice" });

    const aliceDb = context.forSession({ user_id: "alice", claims: {}, authMode: "external" }, app);

    // No `principalId` passed — it must be derived from `aliceDb`'s session so it
    // can't drift from the data's actual scope.
    const builder = createSnapshotBuilder({ appId, schema: app });
    await builder.prefetch(aliceDb, app.notes);
    const snapshot = builder.dehydrate();

    expect(snapshot.principalId).toBe("alice");
  });
});
