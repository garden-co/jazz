import { afterEach, describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { deploy } from "../../src/dev/catalogue.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  largeValueRewriteApp as app,
  largeValueRewritePermissions,
} from "./large-value-rewrite-schema.js";
import { getJazzServerInfo, type JazzServerInfo } from "./testing-server.js";
import {
  TestCleanup,
  createBrowserTestDb,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";

interface BackendCommands {
  largeValueRewriteBackendOpen(info: JazzServerInfo): Promise<void>;
  largeValueRewriteBackendSeed(
    appId: string,
    conversationId: string,
    attachmentBytes: number,
  ): Promise<string>;
  largeValueRewriteBackendAppend(
    appId: string,
    turnId: string,
    words: string[],
    delayMs: number,
  ): Promise<string>;
  largeValueRewriteBackendClose(appId: string): Promise<void>;
}
const backend = commands as unknown as BackendCommands;
const cleanup = new TestCleanup();
let backendAppId: string | undefined;
afterEach(async () => {
  await withTimeout(cleanup.cleanup(), 20_000, "cleanup").catch(() => undefined);
  if (backendAppId) await backend.largeValueRewriteBackendClose(backendAppId);
  backendAppId = undefined;
});

// #3816: MusicAgent's page lists a conversation's attachments without their
// bytes while the backend streams a reply into the same conversation. With a
// large attachment, the browser stopped receiving updates partway through
// the reply, although a reload showed the finished reply.
describe("browser subscriptions while the backend rewrites a row", () => {
  it.each([
    { attachmentBytes: 4 * 1024, label: "small" },
    { attachmentBytes: 353 * 1024, label: "large" },
  ])(
    "deliver the $label attachment's metadata and the final reply",
    async ({ attachmentBytes }) => {
      const info = await getJazzServerInfo(uniqueDbName("large-value-rewrite"));
      await deploy({
        ...info,
        schema: app.wasmSchema,
        permissions: largeValueRewritePermissions,
      });
      backendAppId = info.appId;
      await withTimeout(backend.largeValueRewriteBackendOpen(info), 20_000, "backend open");

      // A persistent browser database, so the page reads through the
      // SharedWorker as the app does.
      const db = cleanup.track(
        await createBrowserTestDb({
          appId: info.appId,
          serverUrl: info.serverUrl,
          secret: generateAuthSecret(),
          logLevel: "trace",
          driver: {
            type: "persistent",
            dbName: uniqueDbName("large-value-rewrite"),
          },
        }),
      );
      const conversation = await db
        .insert(app.conversations, { title: "Release show" })
        .wait({ tier: "global" });
      const turnId = await backend.largeValueRewriteBackendSeed(
        info.appId,
        conversation.id,
        attachmentBytes,
      );

      let filenames: string[] = [];
      let body: string | undefined;
      cleanup.trackSubscription(
        db.subscribe(
          app.attachments.where({ conversation_id: conversation.id }).select("filename"),
          (rows) => {
            filenames = rows.map((row) => row.filename);
          },
        ),
      );
      cleanup.trackSubscription(
        db.subscribe(app.turns.where({ conversation_id: conversation.id }), (rows) => {
          body = rows.find((row) => row.id === turnId)?.body;
        }),
      );

      const words = Array.from({ length: 60 }, (_, index) => `word${index} `);
      const final = await withTimeout(
        backend.largeValueRewriteBackendAppend(info.appId, turnId, words, 20),
        60_000,
        "backend appends",
      );
      expect(final).toBe(words.join(""));

      await waitForCondition(
        async () => body === final && filenames.length === 1,
        20_000,
        "the attachment's metadata and the final reply",
      ).catch((error: Error) => {
        throw new Error(
          `${error.message}; saw ${filenames.length} attachments and ${body?.length ?? "no"} of ${final.length} reply characters`,
        );
      });
      expect(filenames).toEqual(["rough-mix.wav"]);
    },
    120_000,
  );
});
