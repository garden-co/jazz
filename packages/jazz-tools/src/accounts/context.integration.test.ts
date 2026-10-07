import { describe, expect, it, vi } from "vitest";
import { createDb, schema } from "../index.js";
import { localAccountConfig } from "../runtime/testing/account-fixtures.js";
import { createAccountDbWithRuntimeSource } from "./context.js";
import { Db } from "../runtime/db.js";
import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";

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

  it.each([
    { cleanupFails: false, graceful: false },
    { cleanupFails: true, graceful: false },
    { cleanupFails: false, graceful: true },
    { cleanupFails: true, graceful: true },
  ])(
    "disposes the live Db after attachment fails (cleanup fails: $cleanupFails, graceful: $graceful)",
    async ({ cleanupFails, graceful }) => {
      const { serverUrl: _serverUrl, ...config } = await localAccountConfig(
        `failed-e2ee-attachment-${crypto.randomUUID()}`,
      );
      const app = schema.defineApp({ notes: schema.table({ text: schema.string() }, {}) });
      const source = new DefaultRuntimeSource();
      let opened: Db | undefined;
      let gracefulShutdown: Promise<void> | undefined;
      let syncSignal: AbortSignal | undefined;
      source.waitForPendingWrites = (signal) => {
        syncSignal = signal;
        return new Promise<void>((_resolve, reject) => {
          signal?.addEventListener("abort", () => reject(new Error("Sync cancelled")), {
            once: true,
          });
        });
      };
      const disposeSource = source.shutdown.bind(source);
      if (cleanupFails) {
        source.shutdown = async () => {
          await disposeSource();
          throw new Error("Source cleanup failed");
        };
      }
      // Capture the real Db and materialize its runtime before attachment fails.
      // A shutdown-call assertion alone would miss a cancelled graceful wait
      // that leaves the Db usable.
      const createDirect = Db.createWithDirectConnection;
      const creation = vi
        .spyOn(Db, "createWithDirectConnection")
        .mockImplementationOnce(async (resolved, runtime) => {
          opened = await createDirect(resolved, runtime);
          await opened.all(app.notes, { tier: "local" });
          if (graceful) {
            gracefulShutdown = opened.shutdown({ waitForSync: true });
            gracefulShutdown.catch(() => {});
          }
          return opened;
        });
      try {
        await expect(
          createAccountDbWithRuntimeSource(
            {
              ...config,
              e2ee: {
                app: { wasmSchema: app.wasmSchema },
                store: {
                  async read() {
                    return null;
                  },
                  async update() {
                    throw new Error("Invalid attachment must not prepare device keys");
                  },
                },
              },
            },
            source,
          ),
        ).rejects.toThrow("E2EE application is missing managed table");
        expect(opened).toBeDefined();
        if (graceful) {
          expect(syncSignal?.aborted).toBe(true);
          await expect(gracefulShutdown).rejects.toThrow("Graceful shutdown");
        }
        await expect(opened!.all(app.notes, { tier: "local" })).rejects.toThrow(
          /shutting down or closed/,
        );
      } finally {
        creation.mockRestore();
        opened?.abortGracefulShutdown();
        await gracefulShutdown?.catch(() => {});
        await opened?.shutdown().catch(() => {});
      }
    },
  );
});
