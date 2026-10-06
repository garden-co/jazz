import { randomUUID } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { createDb, schema as s } from "../index.js";
import { deploy, startLocalJazzServer, startTestJwtIssuer } from "../testing/index.js";
import { resolveSchemaSource } from "../schema-source.js";
import { accountRegistryUrl } from "./context.js";
import { isProvisionalAccount } from "./enrollment.js";
import { prepareAccountManager } from "./persistence.js";
import { settleAccountSelection } from "./selection-durability.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.notes.allowRead.always();
  policy.notes.allowInsert.always();
  policy.notes.allowUpdate.never();
  policy.notes.allowDelete.never();
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

describe("a retained external account after a reload", () => {
  for (const operation of ["loginJWT", "loginOrRegisterJWT"] as const) {
    it(`receives another client's changes once ${operation} revalidates it in place`, async () => {
      const issuer = await startTestJwtIssuer();
      const appId = randomUUID();
      const server = await startLocalJazzServer({
        appId,
        jwksUrl: issuer.jwksUrl,
        jwtIssuer: issuer.issuer,
        jwtAudience: issuer.audience,
      });
      const open: { shutdown(): Promise<void> }[] = [];
      try {
        await deploy({
          serverUrl: server.url,
          appId,
          adminSecret: server.adminSecret,
          schema: resolveSchemaSource(app),
          permissions,
        });

        // First visit: Alice signs in; this browser retains her assignment.
        const storage = browserStorage();
        const firstVisit = await accountManager(appId, server.url, storage);
        await firstVisit.loginOrRegisterJWT(issuer.jwtForUser("alice"));
        await settleAccountSelection(firstVisit);

        // Reload: the retained account opens before the provider answers.
        const accounts = await accountManager(appId, server.url, storage);
        const retained = accounts.getLoggedIn()!;
        expect(isProvisionalAccount(retained)).toBe(true);
        const db = await createDb({
          appId,
          account: retained,
          serverUrl: server.url,
          driver: { type: "memory" },
        });
        open.push(db);
        let seen: string[] = [];
        const stop = db.subscribe(app.notes, (rows) => {
          seen = rows.map((row) => row.text);
        });

        // On a page the provider answers well after the client has opened.
        await new Promise((resolve) => setTimeout(resolve, 500));

        // The provider confirms the same identity: the open context keeps
        // running and adopts the credential instead of being reopened.
        const confirmed = await accounts[operation](issuer.jwtForUser("alice"));
        expect(confirmed).toBe(retained);
        expect(isProvisionalAccount(retained)).toBe(false);

        // Another client (Bob, elsewhere) writes; the reloaded tab sees it.
        const bobAccounts = await accountManager(appId, server.url);
        const bob = await createDb({
          appId,
          account: await bobAccounts.loginOrRegisterJWT(issuer.jwtForUser("bob")),
          serverUrl: server.url,
          driver: { type: "memory" },
        });
        open.push(bob);
        await bob.insert(app.notes, { text: "from bob" }).wait({ tier: "global" });

        await vi.waitFor(() => expect(seen).toContain("from bob"), { timeout: 15_000 });
        stop();
      } finally {
        for (const db of open.reverse()) await db.shutdown().catch(() => {});
        await server.stop();
        await issuer.stop();
      }
    }, 60_000);
  }
});
