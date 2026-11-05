import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it, vi } from "vitest";
import { schema } from "../../src/schema-namespace.js";
import { createAccountManager } from "../../src/react-native/create-account-manager.js";
import type { AccountHandle } from "../../src/accounts/state.js";
import { createJazzSession } from "../../src/react-native/create-jazz-session.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../../src/testing/index.js";
import { createPlatformHost, installPlatformHost } from "./native-platform.js";

const app = schema.defineApp({ notes: schema.table({ text: schema.string() }, {}) });
const permissions = schema.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
});
type Session = Awaited<ReturnType<typeof createJazzSession>>;

async function withRetainedLogin(
  run: (fixture: {
    open(): Promise<Session>;
    aliceToken: string;
    bobToken: string;
    alice: AccountHandle;
    bob: AccountHandle;
  }) => Promise<void>,
) {
  const issuer = await startTestJwtIssuer();
  const server = await startLocalJazzServer({
    inMemory: true,
    jwksUrl: issuer.jwksUrl,
    jwtIssuer: issuer.issuer,
    jwtAudience: issuer.audience,
  });
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-retained-login-"));
  const host = createPlatformHost(directory);
  installPlatformHost(host);
  // The Linux bridge runs in Node. Supply the real RN platform identity so
  // public read defaults exercise native local-first behaviour.
  vi.stubGlobal("navigator", { product: "ReactNative" });
  const sessions: Session[] = [];
  let stored: string | null = null;
  const config = {
    appId: server.appId,
    serverUrl: server.url,
    store: {
      async read() {
        return stored;
      },
      async update(transform: (previous: string | null) => string) {
        stored = transform(stored);
      },
    },
  };
  try {
    await deploy({ ...config, adminSecret: server.adminSecret, schema: app, permissions });
    const manager = await createAccountManager(config);
    const aliceToken = await issuer.jwtForUser("retained-alice");
    const bobToken = await issuer.jwtForUser("retained-bob");
    await manager.registerJWT(aliceToken);
    const bob = await manager.linkJWT(bobToken);
    const alice = await manager.loginJWT(aliceToken);
    await manager.settleSelection();
    expect(bob.id).toBe(alice.id);
    expect(stored).not.toContain(aliceToken);
    expect(stored).not.toContain(bobToken);
    await run({
      async open() {
        const session = await createJazzSession(config);
        sessions.push(session);
        return session;
      },
      aliceToken,
      bobToken,
      alice,
      bob,
    });
  } finally {
    for (const session of sessions.reverse()) await session.close();
    host.close();
    await rm(directory, { recursive: true, force: true });
    await server.stop();
    await issuer.stop();
    vi.unstubAllGlobals();
  }
}

it("restores Alice before a token and activates the same client with its pending writes", async () => {
  await withRetainedLogin(async ({ open, aliceToken, alice }) => {
    const first = await open();
    expect(first.getSnapshot()).toMatchObject({ status: "ready", needsLogin: true });
    const original = first.getSnapshot().client!;
    const row = await original.db
      .insert(app.notes, { text: "before provider" })
      .wait({ tier: "local" });
    expect((await original.db.all(app.notes)).map((item) => item.text)).toEqual([
      "before provider",
    ]);
    await first.close();

    const restored = await open();
    expect(restored.getSnapshot()).toMatchObject({ status: "ready", needsLogin: true });
    const client = restored.getSnapshot().client!;
    expect(restored.getSnapshot().account?.identity).toEqual(alice.identity);
    await expect
      .poll(async () => (await client.db.all(app.notes)).map((item) => item.text))
      .toEqual(["before provider"]);
    await restored.loginJWT(aliceToken);
    expect(restored.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    expect(restored.getSnapshot().client).toBe(client);
    await expect
      .poll(() =>
        client.db.one(app.notes.select("$createdBy").where({ id: row.id }), { tier: "remote" }),
      )
      .toMatchObject({ $createdBy: { account: alice.id, identity: alice.identity } });
  });
}, 30_000);

it("switches provisional Alice to linked Bob and syncs Alice's queue only when Alice returns", async () => {
  await withRetainedLogin(async ({ open, aliceToken, bobToken, alice, bob }) => {
    const session = await open();
    const original = session.getSnapshot().client!;
    const pending = await original.db
      .insert(app.notes, { text: "Alice pending" })
      .wait({ tier: "local" });
    await session.loginJWT(bobToken);
    expect(session.getSnapshot()).toMatchObject({ status: "ready", needsLogin: false });
    const current = session.getSnapshot().client!;
    expect(current).not.toBe(original);
    expect(session.getSnapshot().account?.identity).toEqual(bob.identity);
    expect(await current.db.all(app.notes, { tier: "remote" })).toEqual([]);
    await expect(original.db.all(app.notes)).rejects.toThrow("shutting down or closed");
    const bobRow = await current.db
      .insert(app.notes, { text: "Bob accepted" })
      .wait({ tier: "global" });

    await session.loginJWT(aliceToken);
    const returned = session.getSnapshot().client!;
    expect(returned).not.toBe(current);
    await expect
      .poll(async () =>
        (await returned.db.all(app.notes.orderBy("text"), { tier: "remote" })).map(
          (item) => item.text,
        ),
      )
      .toEqual(["Alice pending", "Bob accepted"]);
    expect(
      await returned.db.one(app.notes.select("$createdBy").where({ id: pending.id })),
    ).toMatchObject({ $createdBy: { account: alice.id, identity: alice.identity } });
    expect(
      await returned.db.one(app.notes.select("$createdBy").where({ id: bobRow.id })),
    ).toMatchObject({ $createdBy: { account: bob.id, identity: bob.identity } });
  });
}, 30_000);
