import { expect, it, vi } from "vitest";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";
import { memoryPrivateStore } from "../encryption/private-store.js";
import { encryptionStoreScope } from "../encryption/store-scope.js";
import { createDb } from "../runtime/default-create-db.js";
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
