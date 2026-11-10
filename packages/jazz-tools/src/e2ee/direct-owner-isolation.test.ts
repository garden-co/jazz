import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "../runtime/default-create-db.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import {
  trustedReservedSessionToken,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
} from "../runtime/client-session.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { frontE2eeCore } from "./fixtures/app.js";

it("keeps real account policy, pending rows and caller claims outside another owner's observations", async () => {
  const app = s.defineApp(
    {
      __e2ee_isolated_roots: s.table({ ownerAccountId: s.uuid(), title: s.string() }, {}),
    },
    { migrations: "none" },
  );
  const permissions = definePermissions(app, ({ policy, session }) => {
    policy.__e2ee_isolated_roots.allowRead.where({ ownerAccountId: session.user.account });
    policy.__e2ee_isolated_roots.allowInsert.where({ ownerAccountId: session.user.account });
  });
  let server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  const owners: Awaited<ReturnType<typeof createDb>>[] = [];
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    server = await frontE2eeCore(server, { schema: app, permissions });
    const aliceConfig = await localAccountConfig(server.appId, server.url);
    const bobConfig = await localAccountConfig(server.appId, server.url);
    const alice = await createDb(aliceConfig);
    owners.push(alice);
    const bob = await createDb(bobConfig);
    owners.push(bob);
    await Promise.all(owners.map((db) => db.all(app.__e2ee_isolated_roots, { tier: "remote" })));
    const insert = async (db: typeof alice, ownerAccountId: string, title: string) => {
      const tx = db.beginSerializableTransaction();
      const row = tx.insert(app.__e2ee_isolated_roots, { ownerAccountId, title });
      const write = tx.commit();
      await write.wait({ tier: "global" });
      return row;
    };
    const aliceRow = await insert(alice, aliceConfig.account.id, "Alice private row");
    const bobRow = await insert(bob, bobConfig.account.id, "Bob private row");
    const aliceQuery = app.__e2ee_isolated_roots.where({ id: aliceRow.id });
    const bobQuery = app.__e2ee_isolated_roots.where({ id: bobRow.id });
    expect((await alice.observeE2eeHistory([aliceQuery]))[0]!.rows).toEqual([aliceRow]);
    expect((await bob.observeE2eeHistory([bobQuery]))[0]!.rows).toEqual([bobRow]);
    expect(await bob.all(aliceQuery, { tier: "remote" })).toEqual([]);
    expect((await bob.observeE2eeHistory([aliceQuery]))[0]!.rows).toEqual([]);

    const actualSession = (db: typeof alice) => {
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
    const bobRuntime = bob.currentRuntime() as NativeRuntimeAdapter;
    const nativeQuery = JSON.stringify({ table: "__e2ee_isolated_roots" });
    await expect(
      bobRuntime.observeLocalBatch([nativeQuery], JSON.stringify(actualSession(alice))),
    ).rejects.toThrow(/owner identity/);
    const changedClaims = actualSession(bob);
    changedClaims.claims = { ...changedClaims.claims, scope: "unadmitted" };
    await expect(
      bobRuntime.observeLocalBatch([nativeQuery], JSON.stringify(changedClaims)),
    ).rejects.toThrow(/installed owner claims/);
    expect((await bob.observeE2eeHistory([bobQuery]))[0]!.rows).toEqual([bobRow]);

    const rejected = bob.beginSerializableTransaction();
    const unapproved = rejected.insert(app.__e2ee_isolated_roots, {
      ownerAccountId: aliceConfig.account.id,
      title: "Not authorised by Bob",
    });
    const refusal = rejected.commit();
    const refused = expect(refusal.wait({ tier: "global" })).rejects.toMatchObject({
      code: "permission_denied",
    });
    expect(
      (await bob.observeE2eeHistory([app.__e2ee_isolated_roots.where({ id: unapproved.id })]))[0]!
        .rows,
    ).toEqual([]);
    await refused;
    expect(
      (await bob.observeE2eeHistory([app.__e2ee_isolated_roots.where({ id: unapproved.id })]))[0]!
        .rows,
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
