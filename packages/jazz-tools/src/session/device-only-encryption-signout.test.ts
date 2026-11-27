import { expect, it, vi } from "vitest";
import { AccountManager } from "../accounts/state.js";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import { memoryPrivateStore } from "../encryption/private-store.js";
import { encryptionStoreScope } from "../encryption/store-scope.js";
import { createDb } from "../runtime/default-create-db.js";
import { createRegisteredJazzClient } from "../web/registered-jazz-client.js";
import { replaceJazzSessionClient } from "./state.js";
import { openBrowserSessionCore } from "./create-jazz-session.js";

it.each(["deleted", "held", "key wipe failed", "wipe disabled"] as const)(
  "wipes the device-only account's keys only after its persistent replica is deleted (%s)",
  async (outcome) => {
    let accounts: string | null = null;
    const store = memoryPrivateStore();
    const session = await openBrowserSessionCore({
      serverUrl: null,
      scope: "device-only",
      env: "test",
      clearOnSignOut: outcome !== "wipe disabled",
      store: {
        read: async () => accounts,
        update: async (transform) => {
          accounts = transform(accounts);
        },
      },
      encryption: { app, store },
      client: async () => ({
        // Run the local-first runtime in Node and supply the browser worker's delete result.
        createClient: (config) => createDb({ ...config, driver: { type: "memory" } }),
        deleteLocalFirstStore: async () => (outcome === "held" ? "held" : "deleted"),
      }),
    });
    try {
      const { account, client } = session.getSnapshot();
      const keys = encryptionStoreScope("device-only", "test", account!.id);
      expect(await store.list(keys)).toContain("device");
      // The in-memory fixture has no replica files to remove.
      vi.spyOn(client!.db, "deleteClientStorage").mockResolvedValue();
      const failure = new Error("key store unavailable");
      if (outcome === "key wipe failed") vi.spyOn(store, "delete").mockRejectedValue(failure);
      await session.logout();
      if (outcome === "key wipe failed") {
        expect(session.getSnapshot()).toMatchObject({
          status: "signed-out",
          error: { cause: failure },
        });
      } else {
        expect(session.getSnapshot().error).toBeUndefined();
      }
      const remaining = await store.list(keys);
      if (outcome === "deleted") expect(remaining).toEqual([]);
      else expect(remaining).toContain("device");
    } finally {
      await session.close();
    }
  },
);

it.each(["before teardown", "after teardown", "configuration change"] as const)(
  "handles encryption across a logout failure or client replacement (%s)",
  async (transition) => {
    let accounts: string | null = null;
    const config = { scope: "device-only", env: "test", driver: { type: "memory" } } as const;
    const session = await openBrowserSessionCore({
      ...config,
      serverUrl: null,
      store: {
        read: async () => accounts,
        update: async (transform) => {
          accounts = transform(accounts);
        },
      },
      encryption: { app, store: memoryPrivateStore() },
      client: async () => ({ createClient: createDb }),
    });
    try {
      const before = session.getSnapshot();
      const client = before.client!;
      const oldHandle = client.db.encryption;
      const oldDevice = await oldHandle.deviceId();

      if (transition === "before teardown") {
        const failure = new Error("could not finish syncing");
        const shutdown = vi.spyOn(client, "shutdown").mockRejectedValueOnce(failure);
        try {
          await expect(session.logout()).rejects.toBe(failure);
        } finally {
          shutdown.mockRestore();
        }
        const after = session.getSnapshot();
        expect(after.status).toBe("ready");
        expect(after.client?.db.encryption).toBe(oldHandle);
        await expect(oldHandle.deviceId()).resolves.toBe(oldDevice);
        return;
      }

      if (transition === "after teardown") {
        const failure = new Error("account logout failed");
        const logout = vi.spyOn(AccountManager.prototype, "logout").mockImplementationOnce(() => {
          throw failure;
        });
        try {
          await expect(session.logout()).rejects.toBe(failure);
        } finally {
          logout.mockRestore();
        }
        expect(session.getSnapshot()).toMatchObject({ status: "signed-out", error: failure });
        expect(session.getSnapshot().client).toBeUndefined();
      } else {
        await replaceJazzSessionClient(session, (_client, account) =>
          createRegisteredJazzClient(
            "web",
            { ...config, account, encryption: { app, store: memoryPrivateStore() } },
            createDb,
          ),
        );
        const after = session.getSnapshot();
        expect(after.status).toBe("ready");
        expect(after.account).toBe(before.account);
        const newHandle = after.client!.db.encryption;
        expect(newHandle).not.toBe(oldHandle);
        expect(await newHandle.deviceId()).not.toBe(oldDevice);
      }

      // A configuration replacement closes the previous runtime asynchronously.
      await expect
        .poll(async () => oldHandle.deviceId().catch((error: unknown) => error))
        .toMatchObject({ code: "closed" });
      expect(() => oldHandle.onReport(() => {})()).toThrow(
        expect.objectContaining({ code: "closed" }),
      );
    } finally {
      await session.close();
    }
  },
);
