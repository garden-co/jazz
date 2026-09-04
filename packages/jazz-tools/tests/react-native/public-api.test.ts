import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createDb, type Db } from "../../src/react-native/create-db.js";
import { serializeSchemaSource } from "../../src/drivers/schema-wire.js";
import { schema } from "../../src/schema-namespace.js";
import { host } from "./native-platform.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

it("runs public CRUD, query, subscription and foreground propagation through the real RN owner, then closes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-api-"));
  let db: Db | undefined;
  let reader: Db | undefined;
  let unsubscribe: (() => void) | undefined;
  let failed = false;
  try {
    const issuer = "https://auth.example";
    const user = "rn-api-test";
    const capability = host.admit(
      JSON.stringify({
        scope: {
          app_namespace: "rn-api-test",
          storage_namespace: "default",
          auth_scope: JSON.stringify([issuer, user]),
        },
        sqlite_path: join(directory, "relay.sqlite"),
        schema_json: serializeSchemaSource(app.wasmSchema),
        identity: {
          node: "01010101-0101-0101-0101-010101010101",
          author: JSON.stringify([issuer, user]),
        },
        claims: {},
      }),
    );
    const config = {
      appId: "rn-api-test",
      nativeRelay: { capability },
      cookieSession: { issuer, user_id: user, claims: {}, authMode: "external" as const },
    };
    db = await createDb(config);
    // Two foreground roots attached to one actual local relay, no server or
    // simulated database. Empty local reads remain valid until marker delivery.
    reader = await createDb(config);
    const writer = db;
    const observer = reader;
    const snapshots: { id: string; title: string; done: boolean }[][] = [];
    const open = app.todos.where({ done: false }).orderBy("title");
    unsubscribe = observer.subscribe(open, (rows) => snapshots.push(rows));
    const created = await writer
      .insert(app.todos, { title: "first", done: false })
      .wait({ tier: "local" });
    await expect.poll(async () => writer.all(open, { tier: "local" })).toEqual([created]);
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([created]);
    await expect.poll(() => snapshots.at(-1)).toEqual([created]);
    await writer.update(app.todos, created.id, { title: "updated" }).wait({ tier: "local" });
    await expect
      .poll(async () => observer.one(app.todos.where({ id: created.id }), { tier: "local" }))
      .toMatchObject({ title: "updated" });
    await expect.poll(() => snapshots.at(-1)).toEqual([{ ...created, title: "updated" }]);
    await writer.delete(app.todos, created.id).wait({ tier: "local" });
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([]);
    await expect.poll(() => snapshots.at(-1)).toEqual([]);
    const survivor = await writer
      .insert(app.todos, { title: "survives sibling close", done: false })
      .wait({ tier: "local" });
    await expect.poll(async () => observer.all(open, { tier: "local" })).toEqual([survivor]);
    await expect.poll(() => snapshots.at(-1)).toEqual([survivor]);
    unsubscribe();
    unsubscribe = undefined;
    const cancelledSnapshotCount = snapshots.length;
    await writer
      .update(app.todos, survivor.id, { title: "after cancellation" })
      .wait({ tier: "local" });
    const afterCancellation = { ...survivor, title: "after cancellation" };
    await expect
      .poll(async () => observer.all(open, { tier: "local" }))
      .toEqual([afterCancellation]);
    expect(snapshots).toHaveLength(cancelledSnapshotCount);
    await Promise.all([writer.shutdown(), writer.shutdown()]);
    // Shared NativeRuntimeAdapter semantics return no rows after shutdown.
    expect(await writer.all(open, { tier: "local" })).toEqual([]);
    expect(await observer.all(open, { tier: "local" })).toEqual([afterCancellation]);
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    const cleanupErrors: unknown[] = [];
    for (const cleanup of [
      () => unsubscribe?.(),
      () => db?.shutdown(),
      () => reader?.shutdown(),
      () => {
        host.close();
      },
      () => rm(directory, { recursive: true, force: true }),
    ]) {
      try {
        await cleanup();
      } catch (error) {
        cleanupErrors.push(error);
      }
    }
    if (!failed && cleanupErrors.length)
      throw new AggregateError(cleanupErrors, "RN fixture cleanup failed");
  }
});
