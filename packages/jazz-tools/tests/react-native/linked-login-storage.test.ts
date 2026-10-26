import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { createAccountManager } from "../../src/react-native/create-account-manager.js";
import { createDb, type Db } from "../../src/react-native/create-db.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../../src/testing/index.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

const app = schema.defineApp({ notes: schema.table({ text: schema.string() }, {}) });
const permissions = schema.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
});

function accountStore() {
  let value: string | null = null;
  return {
    async read() {
      return value;
    },
    async update(transform: (previous: string | null) => string) {
      value = transform(value);
    },
  };
}

it("keeps both linked logins' offline writes private and resumes each under its original author", async () => {
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-linked-logins-"));
  const host = createPlatformHost(directory);
  installPlatformHost(host);
  const databases: Db[] = [];
  try {
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const manager = await createAccountManager({
      appId: server.appId,
      serverUrl: server.url,
      store: accountStore(),
    });
    const aliceToken = await issuer.jwtForUser("linked-alice");
    const bobToken = await issuer.jwtForUser("linked-bob");
    const first = await manager.registerJWT(aliceToken);
    const linked = await manager.linkJWT(bobToken);
    expect(linked.id).toBe(first.id);
    const alice = await manager.loginJWT(aliceToken);
    const secondManager = await createAccountManager({
      appId: server.appId,
      serverUrl: server.url,
      store: accountStore(),
    });
    const bob = await secondManager.loginJWT(bobToken);
    expect(bob.id).toBe(alice.id);
    const open = async (account: typeof alice, online = false) => {
      const db = await createDb({
        appId: server.appId,
        account,
        ...(online ? { serverUrl: server.url } : {}),
      });
      databases.push(db);
      return db;
    };

    const old = await open(alice);
    const aliceRow = await old.insert(app.notes, { text: "alice pending" }).wait({ tier: "local" });
    await old.shutdown();

    const current = await open(bob);
    expect(await current.all(app.notes)).toEqual([]);
    const bobRow = await current.insert(app.notes, { text: "bob pending" }).wait({ tier: "local" });
    expect((await current.all(app.notes)).map((row) => row.text)).toEqual(["bob pending"]);
    await current.shutdown();

    const resumed = await open(alice);
    await expect
      .poll(async () => (await resumed.all(app.notes)).map((row) => row.text))
      .toEqual(["alice pending"]);
    await resumed.shutdown();

    const onlineAlice = await open(alice, true);
    await expect
      .poll(async () =>
        (await onlineAlice.all(app.notes, { tier: "remote" })).map((row) => row.text),
      )
      .toEqual(["alice pending"]);
    expect(
      await onlineAlice.one(app.notes.select("$createdBy").where({ id: aliceRow.id })),
    ).toMatchObject({
      $createdBy: { account: alice.id, identity: alice.identity },
    });
    await onlineAlice.shutdown({ waitForSync: true });

    const onlineBob = await open(bob, true);
    await expect
      .poll(async () =>
        (await onlineBob.all(app.notes.orderBy("text"), { tier: "remote" })).map((row) => row.text),
      )
      .toEqual(["alice pending", "bob pending"]);
    expect(
      await onlineBob.one(app.notes.select("$createdBy").where({ id: bobRow.id })),
    ).toMatchObject({
      $createdBy: { account: bob.id, identity: bob.identity },
    });
    await onlineBob.shutdown({ waitForSync: true });
  } finally {
    for (const db of databases.reverse()) await db.shutdown();
    host.close();
    await rm(directory, { recursive: true, force: true });
    await server.stop();
    await issuer.stop();
  }
}, 30_000);
