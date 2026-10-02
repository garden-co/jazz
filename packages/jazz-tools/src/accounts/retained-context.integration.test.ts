import { expect, it, vi } from "vitest";
import { createDb, schema } from "../index.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";
import { createAccountManager } from "./create-account-manager.js";
import { isProvisionalAccount } from "./enrollment.js";

const alice = { issuer: "https://issuer.example", subject: "alice" };
const aliceAccount = "00000000-0000-4000-8000-00000000000a";

function jwt(claims: Record<string, unknown> = {}) {
  const exp = Math.floor(Date.now() / 1000) + 3600;
  return `e30.${btoa(JSON.stringify({ iss: alice.issuer, sub: alice.subject, exp, ...claims }))}.sig`;
}

it("opens a retained account's local data before its credential, then adopts the provider JWT", async () => {
  const appId = "retained-account-context";
  let stored: string | null = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [],
    selected: null,
    assignment: { account: aliceAccount, ...alice },
  });
  const fetch = vi.fn(
    async () => new Response(JSON.stringify({ account: aliceAccount, identity: alice })),
  );
  vi.stubGlobal("fetch", fetch);
  const accounts = await createAccountManager({
    appId,
    serverUrl: "http://127.0.0.1:1",
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  const account = accounts.getLoggedIn()!;
  expect(isProvisionalAccount(account)).toBe(true);
  const app = schema.defineApp({ notes: schema.table({ text: schema.string() }, {}) });
  const db = await createDb({ appId, account, driver: { type: "memory" } });
  try {
    expect(db.getConfig().jwtToken).toBeUndefined();
    expect(getDbInternalSession(db)).toMatchObject({
      account_id: aliceAccount,
      issuer: alice.issuer,
      user_id: alice.subject,
      authMode: "external",
    });
    const { value: row } = await db.insert(app.notes, { text: "before revalidation" });
    expect(await db.one(app.notes.select("$createdBy").where({ id: row.id }))).toMatchObject({
      $createdBy: { account: aliceAccount, identity: alice },
    });

    const token = jwt({ role: "editor" });
    await expect(accounts.revalidateJWT("loginJWT", { getToken: async () => token })).resolves.toBe(
      account,
    );
    await vi.waitFor(() => expect(db.getConfig().jwtToken).toBe(token));
    expect(db.getAuthState()).toMatchObject({
      authMode: "external",
      session: {
        user: { account: aliceAccount, identity: alice },
        claims: expect.objectContaining({ role: "editor" }),
      },
    });
    expect(await db.all(app.notes)).toEqual([
      expect.objectContaining({ id: row.id, text: "before revalidation" }),
    ]);
  } finally {
    await db.shutdown();
    vi.unstubAllGlobals();
  }
});
