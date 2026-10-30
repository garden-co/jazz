import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { createDb, schema as s, type Db } from "../index.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { observeLocalWrites } from "../runtime/local-write-observer.js";
import { createJazzAppOwner, type JazzApp } from "../session/app.js";
import { createJazzSessionOwner } from "../session/state.js";
import { accountRegistryUrl } from "./context.js";
import { prepareAccountManager } from "./persistence.js";

const app = s.defineApp({ carts: s.table({ item: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.carts.allowRead.always();
  policy.carts.allowInsert.always();
  policy.carts.allowUpdate.never();
  policy.carts.allowDelete.never();
});

/** An in-memory stand-in for one browser's account selection storage. */
function browserStorage() {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform: (value: string | null) => string) {
      stored = transform(stored);
    },
  };
}

function accountManager(appId: string, serverUrl: string, store = browserStorage()) {
  return prepareAccountManager({
    appId,
    registry: accountRegistryUrl(serverUrl, appId),
    // The browser host's setting: reopen the selected external account at once.
    retainAccountAssignment: true,
    mintToken: () => {
      throw new Error("external accounts mint no local-first token");
    },
    store,
  });
}

describe("a manual-mode app after a reload (#4061)", () => {
  it("syncs a write made before login once it logs in because needsLogin is true", async () => {
    const issuer = await startTestJwtIssuer();
    const appId = randomUUID();
    const server = await startLocalJazzServer({
      appId,
      jwksUrl: issuer.jwksUrl,
      jwtIssuer: issuer.issuer,
      jwtAudience: issuer.audience,
    });
    const open: { shutdown(): Promise<void> }[] = [];
    let shopper: JazzApp<{ db: Db; shutdown(): Promise<void> }> | undefined;
    try {
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret: server.adminSecret,
        schema: resolveSchemaSource(app),
        permissions,
      });

      // First visit: the shopper signs in; this browser retains the assignment.
      const storage = browserStorage();
      const firstVisit = await accountManager(appId, server.url, storage);
      await firstVisit.loginOrRegisterJWT(issuer.jwtForUser("shopper"));
      await firstVisit.settleSelection();

      // Reload: a manual-mode app (no managed auth) reopens the account at once.
      const accounts = await accountManager(appId, server.url, storage);
      shopper = createJazzAppOwner({}, () =>
        createJazzSessionOwner({
          accounts,
          async openClient(account) {
            const db = await createDb({
              appId,
              account,
              serverUrl: server.url,
              driver: { type: "memory" },
            });
            return { db, shutdown: () => db.shutdown() };
          },
        }),
      );
      await vi.waitFor(() => expect(shopper!.getSnapshot().status).toBe("ready"));
      const reopened = shopper.getSnapshot();
      // Jamazon's trap: the identity already reads as signed in.
      expect(reopened.account?.identity.issuer).toBe(issuer.issuer);
      expect(reopened.needsLogin).toBe(true);

      // The shopper fills their cart before the app has logged in again.
      const db = reopened.client!.db;
      const writes = vi.fn();
      const stopObserving = observeLocalWrites(db, writes);
      db.insert(app.carts, { item: "teapot" });
      expect(writes).toHaveBeenCalledOnce();
      stopObserving();

      // Another device of the same store reads the server's view.
      const otherAccounts = await accountManager(appId, server.url);
      const other = await createDb({
        appId,
        account: await otherAccounts.loginOrRegisterJWT(issuer.jwtForUser("other")),
        serverUrl: server.url,
        driver: { type: "memory" },
      });
      open.push(other);
      let elsewhere: string[] = [];
      const stop = other.subscribe(app.carts, (rows) => {
        elsewhere = rows.map((row) => row.item);
      });
      await new Promise((resolve) => setTimeout(resolve, 1_000));
      expect(elsewhere).not.toContain("teapot");

      // The fix the app makes: log in again whenever needsLogin is true,
      // rather than inferring sign-in from the account's identity.
      if (shopper.getSnapshot().needsLogin)
        await shopper.sessionActions.loginOrRegisterJWT(issuer.jwtForUser("shopper"));
      expect(shopper.getSnapshot()).toMatchObject({
        status: "ready",
        account: reopened.account,
        client: reopened.client,
        needsLogin: false,
      });

      await vi.waitFor(() => expect(elsewhere).toContain("teapot"), { timeout: 15_000 });
      stop();
    } finally {
      await shopper?.dispose().catch(() => {});
      for (const db of open.reverse()) await db.shutdown().catch(() => {});
      await server.stop();
      await issuer.stop();
    }
  }, 60_000);
});
