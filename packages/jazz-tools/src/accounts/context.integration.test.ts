import { describe, expect, it, vi } from "vitest";
import { createDb, schema } from "../index.js";
import { getDbInternalSession } from "../runtime/db-internal-session.js";
import type { AuthFailureReason } from "../runtime/auth-state.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";

function failAuth(db: object, reason: AuthFailureReason): void {
  (db as { markUnauthenticated(reason: AuthFailureReason): void }).markUnauthenticated(reason);
}

describe("account context authority", () => {
  it("opens locally without activating its registry transport and rejects another app or authority", async () => {
    const { serverUrl: _serverUrl, ...config } = await localAccountConfig("local-only-account");
    const app = schema.defineApp({ notes: schema.table({ text: schema.string() }, {}) });
    const db = await createDb(config);
    try {
      const { value: row } = await db.insert(app.notes, { text: "offline" });
      expect(await db.all(app.notes)).toEqual([
        expect.objectContaining({ id: row.id, text: "offline" }),
      ]);
      expect(await db.one(app.notes.select("$createdBy").where({ id: row.id }))).toMatchObject({
        $createdBy: { account: config.account.id, identity: config.account.identity },
      });
      const author = { account: config.account.id, identity: config.account.identity };
      const otherAuthor = {
        ...author,
        account:
          author.account === "00000000-0000-4000-8000-000000000001"
            ? "00000000-0000-4000-8000-000000000002"
            : "00000000-0000-4000-8000-000000000001",
      };
      expect(await db.all(app.notes.where({ $createdBy: author }))).toHaveLength(1);
      expect(await db.all(app.notes.where({ "$createdBy.account": author.account }))).toHaveLength(
        1,
      );
      expect(
        await db.all(app.notes.where({ "$createdBy.identity": author.identity })),
      ).toHaveLength(1);
      expect(await db.all(app.notes.where({ $createdBy: { ne: author } }))).toEqual([]);
      const accountlessAuthor = { ...author, account: null };
      await expect(async () =>
        db.all(
          // @ts-expect-error Durable row-author conditions require an account.
          app.notes.where({ $createdBy: { ne: accountlessAuthor } }),
        ),
      ).rejects.toThrow('Invalid structured author condition for "$createdBy"');
      expect(await db.all(app.notes.where({ $createdBy: { ne: otherAuthor } }))).toHaveLength(1);
      expect(
        await db.all(
          app.notes.where({
            $createdBy: { ...author, identity: { ...author.identity, subject: "someone-else" } },
          }),
        ),
      ).toEqual([]);
      expect(
        await db.all(
          app.union([
            app.notes.where({ $createdBy: author }),
            app.notes.where({ $createdBy: otherAuthor }),
          ]),
        ),
      ).toHaveLength(1);
      await expect(createDb({ ...config, appId: "another-app" })).rejects.toMatchObject({
        code: "account_application_mismatch",
      });
      await expect(
        createDb({ ...config, serverUrl: "https://other-core.example" }),
      ).rejects.toMatchObject({ code: "account_application_mismatch" });
    } finally {
      await db.shutdown();
    }
  });

  it("keeps a local-first account's identity across repeated auth refreshes", async () => {
    const config = await localAccountConfig("local-first-repeated-refresh");
    const db = await createDb(config);
    try {
      for (let refresh = 0; refresh < 3; refresh++) {
        await db.refreshAccountAuth(config.account);
        expect(getDbInternalSession(db)).toMatchObject({
          issuer: "urn:jazz:local-first",
          user_id: config.account.identity.subject,
        });
      }
      expect(db.getAuthState()).toMatchObject({
        authMode: "local-first",
        session: { user: { identity: config.account.identity } },
      });
    } finally {
      await db.shutdown();
    }
  });

  it("renews a local-first account's token when the server reports it expired", async () => {
    const config = await localAccountConfig("local-first-expired-renewal");
    const db = await createDb(config);
    try {
      await db.refreshAccountAuth(config.account);
      const refresh = vi.spyOn(db, "refreshAccountAuth");
      failAuth(db, "expired");
      expect(db.getAuthState().error).toBe("expired");
      await vi.waitFor(() => expect(db.getAuthState().error).toBeUndefined());
      expect(refresh).toHaveBeenCalledOnce();
    } finally {
      await db.shutdown();
    }
  });

  it("retries a failed account auth refresh instead of abandoning renewal", async () => {
    const config = await localAccountConfig("local-first-refresh-retry");
    const db = await createDb(config);
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const refresh = vi
        .spyOn(db, "refreshAccountAuth")
        .mockRejectedValueOnce(new Error("registry unavailable"));
      failAuth(db, "expired");
      await vi.waitFor(() => expect(db.getAuthState().error).toBeUndefined(), { timeout: 5_000 });
      expect(refresh).toHaveBeenCalledTimes(2);
      expect(logged).toHaveBeenCalledWith(
        "Account auth refresh failed",
        expect.objectContaining({ message: "registry unavailable" }),
      );
    } finally {
      logged.mockRestore();
      await db.shutdown();
    }
  });

  it("spaces out account renewals a server keeps rejecting as expired", async () => {
    const config = await localAccountConfig("local-first-renewal-backoff");
    const db = await createDb(config);
    try {
      const refresh = vi.spyOn(db, "refreshAccountAuth");
      failAuth(db, "expired");
      await vi.waitFor(() => expect(db.getAuthState().error).toBeUndefined());
      expect(refresh).toHaveBeenCalledOnce();

      // The renewed token is rejected again, as by a server whose clock is
      // ahead: the next renewal waits instead of reconnecting at once.
      failAuth(db, "expired");
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(refresh).toHaveBeenCalledOnce();
      await vi.waitFor(() => expect(refresh).toHaveBeenCalledTimes(2), { timeout: 3_000 });
      await vi.waitFor(() => expect(db.getAuthState().error).toBeUndefined());
    } finally {
      await db.shutdown();
    }
  });
});
