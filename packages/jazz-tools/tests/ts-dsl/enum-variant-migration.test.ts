// #5056: a migration may add enum variants. Rows travel by variant name, so
// a client still on the older schema reads a row holding the new variant
// under its name, while its own writes stay checked against its own
// variants. A deploy admits the new version without making it the core's
// current schema: writes, filters, sorts and policies naming the new
// variant must work through the backend all the same.
//
//   old (v1: todo, done) ──insert "todo"──► backend (v2) ──► core ──► current reads "todo"
//   current (v2: todo, done, blocked) ──insert "blocked"──► ... ──► old reads "blocked"
//   old ──insert "blocked"──✗ (not one of v1's variants)
import { afterEach, describe, expect, it } from "vitest";
import { schema as s, migration as m } from "../../src/index.js";
import { computeSchemaHash } from "../../src/dev/catalogue.js";
import { deploy, startLocalJazzServer, startTestJazz } from "../../src/testing/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { appArtifact, appPermissions } from "../../src/testing/app-artifact.js";
import { shipArtifact } from "../../src/runtime/app-artifact.js";
import type { Db } from "../../src/runtime/db.js";

const schemaV1 = {
  tasks: s.table(
    {
      title: s.string(),
      status: s.enum("todo", "done"),
      labels: s.array(s.enum("bug")).optional(),
    },
    {},
  ),
};
// A new variant goes at the end of the list, so earlier rows sort as before.
const schemaV2 = {
  tasks: s.table(
    {
      title: s.string(),
      status: s.enum("todo", "done", "blocked"),
      labels: s.array(s.enum("bug", "ui")).optional(),
    },
    {},
  ),
};
const v1 = s.defineApp(schemaV1, { migrations: "none" });
const v2 = s.defineApp(schemaV2, { migrations: "none" });
const allowAllV1 = s.definePermissions(v1, ({ policy }) => {
  policy.tasks.allowRead.always();
  policy.tasks.allowInsert.always();
  policy.tasks.allowUpdate.always();
});
const allowAllV2 = s.definePermissions(v2, ({ policy }) => {
  policy.tasks.allowRead.always();
  policy.tasks.allowInsert.always();
  policy.tasks.allowUpdate.always();
});
// v2's permissions name the variant it adds.
const onlyBlocked = s.definePermissions(v2, ({ policy }) => {
  policy.tasks.allowRead.where({ status: "blocked" });
  policy.tasks.allowInsert.always();
});

describe("adding an enum variant in a migration", () => {
  let server: Awaited<ReturnType<typeof startTestJazz>> | undefined;
  let dbs: Db[] = [];

  /** v1, then v2 with the stub `migrations create` writes (no steps), deployed. */
  async function start(permissionsV2: typeof onlyBlocked) {
    const core = await startLocalJazzServer({ inMemory: true });
    const deployment = { serverUrl: core.url, appId: core.appId, adminSecret: core.adminSecret };
    const deployed = await deploy({ ...deployment, schema: v1, permissions: allowAllV1 });
    const migration = m.defineMigration({
      fromHash: deployed.schema.hash,
      toHash: await computeSchemaHash(v2.wasmSchema),
      from: schemaV1,
      to: schemaV2,
    });
    await deploy({ ...deployment, schema: v2, permissions: permissionsV2, migration });
    // v2 ships its migration in its jazz.app.json, before its backend boots.
    shipArtifact(v2.wasmSchema, {
      artifact: await appArtifact(v1, {
        steps: [{ to: v2, migration }],
      }),
    });
    server = await startTestJazz({
      app: v2,
      permissions: await appPermissions(v2, permissionsV2),
      core,
    });
  }

  async function open() {
    const db = await createDb({
      ...(await localAccountConfig(server!.appId, server!.url)),
      driver: { type: "persistent" },
    });
    dbs.push(db);
    return db;
  }

  afterEach(async () => {
    try {
      for (const db of dbs) await db.shutdown();
    } finally {
      dbs = [];
      await server?.stop();
      await server?.core.stop();
      server = undefined;
    }
  });

  it("lets older rows read on the new schema and the new variant read on the old one", async () => {
    await start(allowAllV2);
    const old = await open();
    const current = await open();
    const earlier = await old
      .insert(v1.tasks, { title: "earlier", status: "todo", labels: ["bug"] })
      .wait({ tier: "global" });
    await expect
      .poll(() => current.all(v2.tasks.where({ id: earlier.id })))
      .toEqual([{ id: earlier.id, title: "earlier", status: "todo", labels: ["bug"] }]);

    const later = await current
      .insert(v2.tasks, { title: "later", status: "blocked", labels: ["ui", "bug"] })
      .wait({ tier: "global" });
    const laterRow = { id: later.id, title: "later", status: "blocked", labels: ["ui", "bug"] };
    // The older client sees the new variant's name as it is.
    await expect.poll(() => old.all(v1.tasks.where({ id: later.id }))).toEqual([laterRow]);

    // A client that never wrote it filters and sorts by the new variant.
    const fresh = await open();
    await expect.poll(() => fresh.all(v2.tasks.where({ status: "blocked" }))).toEqual([laterRow]);
    // v2 declares `blocked` last, so it sorts after `todo`, and before it
    // descending.
    await expect
      .poll(async () => (await fresh.all(v2.tasks.orderBy("status", "asc"))).map((t) => t.title))
      .toEqual(["earlier", "later"]);
    expect((await fresh.all(v2.tasks.orderBy("status", "desc"))).map((t) => t.title)).toEqual([
      "later",
      "earlier",
    ]);
  });

  it("still checks an older client's writes against its own variants", async () => {
    await start(allowAllV2);
    const old = await open();
    expect(() =>
      old.insert(v1.tasks, { title: "nope", status: "blocked" as "todo", labels: null }),
    ).toThrow(/blocked/);
    expect(() =>
      old.insert(v1.tasks, { title: "nope", status: "todo", labels: ["ui" as "bug"] }),
    ).toThrow(/ui/);
  });

  it("evaluates a policy that names the new variant", async () => {
    await start(onlyBlocked);
    const writer = await open();
    await writer.insert(v2.tasks, { title: "hidden", status: "todo" }).wait({ tier: "global" });
    const shown = await writer
      .insert(v2.tasks, { title: "shown", status: "blocked" })
      .wait({ tier: "global" });
    const reader = await open();
    await expect
      .poll(() => reader.all(v2.tasks))
      .toEqual([{ id: shown.id, title: "shown", status: "blocked", labels: null }]);
    // A client still on v1 is governed by the same bundle: the policy names
    // a variant v1 doesn't declare, and it reads the row by that name.
    const old = await open();
    await expect
      .poll(() => old.all(v1.tasks))
      .toEqual([{ id: shown.id, title: "shown", status: "blocked", labels: null }]);
  });
});
