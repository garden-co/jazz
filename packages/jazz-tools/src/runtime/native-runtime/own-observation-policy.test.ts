import { expect, it } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { definePermissions } from "../../permissions/index.js";
import { createDb } from "../default-create-db.js";
import { localAccountConfig } from "../testing/account-fixtures.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  trustedReservedSessionToken,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
} from "../client-session.js";
import { getDbInternalSession } from "../db-internal-session.js";
import { startTestJazz } from "../../testing/index.js";
import { translateQuery } from "../query-adapter.js";
import { transformRow } from "../row-transformer.js";

it("keeps real account policy, pending rows and caller claims outside another owner's observations", async () => {
  const app = s.defineApp(
    {
      observation_rows: s.table({ ownerAccountId: s.uuid(), title: s.string() }, {}),
    },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.observation_rows.allowRead.where({ ownerAccountId: session.user.account });
    policy.observation_rows.allowInsert.where({ ownerAccountId: session.user.account });
  });
  const server = await startTestJazz({
    app,
    permissions,
    coreOptions: { allowLocalFirstAuth: true, inMemory: true },
  });
  const owners: Awaited<ReturnType<typeof createDb>>[] = [];
  const actualSession = (db: (typeof owners)[number]) => {
    const session = getDbInternalSession(db);
    if (!session) throw new Error("Missing real fixture account session");
    return {
      account_id: session.account_id,
      issuer: session.issuer,
      user_id: session.user_id,
      authMode: session.authMode,
      claims: session.claims,
      [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: trustedReservedSessionToken(session),
    };
  };

  const observe = async (db: (typeof owners)[number], query: typeof app.observation_rows) => {
    const runtime = db.currentRuntime() as NativeRuntimeAdapter;
    const snapshots = await runtime.observeLocalBatch(
      [translateQuery(query._build(), query._schema)],
      JSON.stringify(actualSession(db)),
    );
    return snapshots.map((snapshot) => ({
      ...snapshot,
      rows: snapshot.rows.map((row) => transformRow(row, query._schema, query._table)),
    }));
  };
  try {
    const aliceConfig = await localAccountConfig(server.appId, server.url);
    const bobConfig = await localAccountConfig(server.appId, server.url);
    const alice = await createDb(aliceConfig);
    owners.push(alice);
    const bob = await createDb(bobConfig);
    owners.push(bob);
    await Promise.all(owners.map((db) => db.all(app.observation_rows, { tier: "remote" })));
    const insert = async (db: typeof alice, ownerAccountId: string, title: string) => {
      const tx = db.beginSerializableTransaction();
      const row = tx.insert(app.observation_rows, { ownerAccountId, title });
      const write = tx.commit();
      await write.wait({ tier: "global" });
      return row;
    };
    const aliceRow = await insert(alice, aliceConfig.account.id, "Alice private row");
    const bobRow = await insert(bob, bobConfig.account.id, "Bob private row");
    const aliceQuery = app.observation_rows.where({ id: aliceRow.id });
    const bobQuery = app.observation_rows.where({ id: bobRow.id });
    expect((await observe(alice, aliceQuery))[0]!.rows).toEqual([aliceRow]);
    // The original direct policy-bound path still supplies no own-client bypass.
    const aliceRuntime = alice.currentRuntime() as NativeRuntimeAdapter;
    expect(
      (
        await aliceRuntime.observeLocalBatch([
          translateQuery(aliceQuery._build(), aliceQuery._schema),
        ])
      )[0]!.rows,
    ).toEqual([]);
    const shared = await aliceRuntime.observeLocalBatch(
      [
        translateQuery(aliceQuery._build(), aliceQuery._schema),
        translateQuery(aliceQuery._build(), aliceQuery._schema),
      ],
      JSON.stringify(actualSession(alice)),
    );
    expect(shared[0]!.through).toBe(shared[1]!.through);
    expect(shared.map((x) => x.evidence)).toEqual([shared[0]!.evidence, shared[0]!.evidence]);
    expect((await observe(bob, bobQuery))[0]!.rows).toEqual([bobRow]);
    expect(await bob.all(aliceQuery, { tier: "remote" })).toEqual([]);
    expect((await observe(bob, aliceQuery))[0]!.rows).toEqual([]);

    const bobRuntime = bob.currentRuntime() as NativeRuntimeAdapter;
    const nativeQuery = JSON.stringify({ table: "observation_rows" });
    await expect(
      bobRuntime.observeLocalBatch([nativeQuery], JSON.stringify(actualSession(alice))),
    ).rejects.toThrow(/owner identity/);
    const changedClaims = actualSession(bob);
    changedClaims.claims = { ...changedClaims.claims, scope: "unadmitted" };
    await expect(
      bobRuntime.observeLocalBatch([nativeQuery], JSON.stringify(changedClaims)),
    ).rejects.toThrow(/installed owner claims/);
    expect((await observe(bob, bobQuery))[0]!.rows).toEqual([bobRow]);

    const rejected = bob.beginSerializableTransaction();
    const unapproved = rejected.insert(app.observation_rows, {
      ownerAccountId: aliceConfig.account.id,
      title: "Not authorised by Bob",
    });
    const refusal = rejected.commit();
    const refused = expect(refusal.wait({ tier: "global" })).rejects.toMatchObject({
      code: "permission_denied",
    });
    expect(
      (await observe(bob, app.observation_rows.where({ id: unapproved.id })))[0]!.rows,
    ).toEqual([]);
    await refused;
    expect(
      (await observe(bob, app.observation_rows.where({ id: unapproved.id })))[0]!.rows,
    ).toEqual([]);
    await bob.shutdown();
    await expect(
      bobRuntime.observeLocalBatch([nativeQuery], JSON.stringify(actualSession(bob))),
    ).rejects.toThrow(/closed/);
  } finally {
    await Promise.all(owners.map((db) => db.shutdown()));
    await server.stop();
  }
}, 30_000);
