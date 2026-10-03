import { expect, it, vi } from "vitest";
import { createDb, schema } from "../index.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";
import { accountRegistryUrl } from "./context.js";
import { prepareAccountManager } from "./persistence.js";
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
  // The browser host retains assignments; this is its account manager over
  // an in-memory store.
  const accounts = await prepareAccountManager({
    appId,
    registry: accountRegistryUrl("http://127.0.0.1:1", appId),
    retainAccountAssignment: true,
    mintToken: () => {
      throw new Error("a retained external account mints no local-first token");
    },
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

it("does not loop renewing a retained account's missing credential before revalidation", async () => {
  const { Db } = await import("../runtime/db.js");
  const appId = "retained-account-backoff";
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
  // The browser host retains assignments; this is its account manager over
  // an in-memory store.
  const accounts = await prepareAccountManager({
    appId,
    registry: accountRegistryUrl("http://127.0.0.1:1", appId),
    retainAccountAssignment: true,
    mintToken: () => {
      throw new Error("a retained external account mints no local-first token");
    },
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
  const authListeners: ((state: { error?: string }) => void)[] = [];
  const onAuthChanged = Db.prototype.onAuthChanged;
  vi.spyOn(Db.prototype, "onAuthChanged").mockImplementation(function (this: never, listener) {
    authListeners.push(listener as never);
    return onAuthChanged.call(this, listener);
  });
  const refreshAccountAuth = vi.spyOn(Db.prototype, "refreshAccountAuth");
  const db = await createDb({ appId, account, driver: { type: "memory" } });
  vi.useFakeTimers();
  try {
    // A tokenless link reports a missing credential; nothing can renew it yet.
    for (let i = 0; i < 20; i++) {
      for (const listener of authListeners) listener({ error: "missing" });
      await vi.advanceTimersByTimeAsync(60_000);
    }
    expect(refreshAccountAuth).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();

    vi.useRealTimers();
    await accounts.revalidateJWT("loginJWT", { getToken: async () => jwt() });
    await vi.waitFor(() => expect(refreshAccountAuth).toHaveBeenCalledOnce());
    expect(fetch).toHaveBeenCalledOnce();
  } finally {
    vi.useRealTimers();
    vi.restoreAllMocks();
    await db.shutdown();
    vi.unstubAllGlobals();
  }
});
