/**
 * Browser receipt for large values travelling through the durable
 * tab -> SharedWorker -> server relay.  This deliberately uses the public
 * browser API and a real server/IndexedDB; the tab owns only an in-memory
 * runtime while the worker owns the durable replica.
 */

import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { schema as s } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, sleep, uniqueDbName, waitForQuery, withTimeout } from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = s.defineApp({
  values: s.table({
    name: s.string(),
    text: s.string().optional(),
    bytes: s.bytes().optional(),
    control: s.boolean(),
  }),
});

const permissions = s.definePermissions(app, ({ policy }) => [
  policy.values.allowRead.always(),
  policy.values.allowInsert.always(),
  policy.values.allowUpdate.always(),
  policy.values.allowDelete.always(),
]);

// This deliberately keeps the ordinary write API: `body` crosses Groove's
// automatic 64 KiB promotion boundary during one user transaction which also
// authors a referenced project. It is distinct from the streaming receipts
// below, which each create one row in their own transaction.
const transactionApp = s.defineApp({
  projects: s.table({
    name: s.string(),
  }),
  documents: s
    .table({
      branch: s.string(),
      title: s.string(),
      projectId: s.ref("projects"),
      body: s.string(),
    })
    .branchBy("branch"),
});

const transactionPermissions = s.definePermissions(transactionApp, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.documents.allowRead.always(),
  policy.documents.allowInsert.always(),
  policy.documents.allowUpdate.always(),
  policy.documents.allowDelete.always(),
]);

describe("browser persistent-worker large-value relay", () => {
  const cleanup = new TestCleanup();

  afterEach(async () => {
    await withTimeout(cleanup.cleanup(), 10_000, "browser relay cleanup did not finish");
  });

  /// A tab streams multi-chunk Text and Bytea through its durable SharedWorker
  /// to the server; a following ordinary mutation proves the connection remains
  /// usable after both uploads.
  ///
  /// writer tab -> persistent SharedWorker -> server -> reader tab
  it("syncs streamed text and bytes without poisoning the relay", async () => {
    const server = await withTimeout(
      getJazzServerInfo(uniqueDbName("large-value-worker-relay")),
      10_000,
      "test server did not become available",
    );
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app.wasmSchema,
      permissions,
    });

    const secret = generateAuthSecret();
    const writer = await withTimeout(
      openSyncedBrowserDb("large-value-worker-relay-writer", secret, server),
      10_000,
      "writer tab did not attach to its persistent worker",
    );
    const reader = await withTimeout(
      openSyncedBrowserDb("large-value-worker-relay-reader", secret, server),
      10_000,
      "reader tab did not attach to its persistent worker",
    );

    const text = ["browser worker ".repeat(6_000), "relay text ".repeat(8_000)].join("");
    const bytes = new Uint8Array(180_000);
    for (let index = 0; index < bytes.length; index += 1) bytes[index] = index % 251;

    const textWrite = await writer.insertStreaming(app.values, {
      name: "streamed-text",
      text: streamOf(text.slice(0, 72_000), text.slice(72_000)),
      control: false,
    });
    await withTimeout(
      textWrite.wait({ tier: "global" }),
      15_000,
      "streamed Text did not reach the server through the persistent worker",
    );

    const byteWrite = await writer.insertStreaming(app.values, {
      name: "streamed-bytes",
      bytes: streamOf(bytes.slice(0, 70_000), bytes.slice(70_000, 140_000), bytes.slice(140_000)),
      control: false,
    });
    await withTimeout(
      byteWrite.wait({ tier: "global" }),
      15_000,
      "streamed Bytea did not reach the server through the persistent worker",
    );

    await withTimeout(
      writer.insert(app.values, { name: "control", control: true }).wait({ tier: "global" }),
      15_000,
      "ordinary control write was blocked after streamed uploads",
    );

    // Control: the same server-side receipt is readable by a direct-memory
    // client. If the persistent reader regresses, this separates a nested
    // tab -> worker relay failure from upload/server availability.
    const directReader = await withTimeout(
      openSyncedBrowserDb("large-value-worker-relay-direct-reader", secret, server, false),
      10_000,
      "direct-memory reader did not attach to the server",
    );
    const directValues = await waitForQuery(
      directReader,
      app.values,
      (rows) => rows.length === 3 && rows.some((row) => row.name === "control"),
      "direct-memory reader did not receive the persistent-worker receipt",
      20_000,
      "edge",
    );
    expect(directValues.find((row) => row.name === "streamed-text")?.text).toBe(text);
    expect(directValues.find((row) => row.name === "streamed-bytes")?.bytes).toEqual(bytes);

    const values = await waitForQuery(
      reader,
      app.values,
      (rows) => rows.length === 3 && rows.some((row) => row.name === "control"),
      "persistent reader did not receive a direct-memory-verified streamed receipt",
      20_000,
      "edge",
    );
    const receivedText = values.find((row) => row.name === "streamed-text");
    const receivedBytes = values.find((row) => row.name === "streamed-bytes");
    expect(receivedText?.text).toBe(text);
    expect(receivedBytes?.bytes).toEqual(bytes);
  }, 90_000);

  /**
   * A foreground tab sends this as exactly one CommitUnit after its worker has
   * staged the promoted `body` tree. The relay must not reconstruct a second,
   * incompatible payload for the same transaction while forwarding it to the
   * server.
   *
   * tab transaction(project + large document) -> SharedWorker -> server
   */
  it("settles an ordinary multi-row transaction with an automatic large value", async () => {
    const server = await withTimeout(
      getJazzServerInfo(uniqueDbName("large-value-transaction-relay")),
      10_000,
      "test server did not become available",
    );
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: transactionApp.wasmSchema,
      permissions: transactionPermissions,
    });

    const secret = generateAuthSecret();
    const writerDbName = uniqueDbName("large-value-transaction-writer");
    const writer = await withTimeout(
      openSyncedBrowserDb("large-value-transaction-writer", secret, server, true, writerDbName),
      10_000,
      "writer tab did not attach to its persistent worker",
    );

    // 240 KB crosses the inline boundary and needs multiple tree chunks.
    // Keep this identical to the historical browser-corpus producer.
    const body = "large value ".repeat(20_000);
    const initial = await writer.transaction((tx) => {
      const project = tx.insert(transactionApp.projects, { name: "relay project" });
      const document = tx.insert(
        transactionApp.documents,
        {
          branch: "main",
          title: "relay document",
          projectId: project.id,
          body,
        },
        { branch: "main" },
      );
      return { project, document };
    });

    await withTimeout(
      initial.wait({ tier: "global" }),
      20_000,
      "ordinary multi-row large-value transaction conflicted through the persistent worker",
    );

    const documents = await withTimeout(
      writer.all(transactionApp.documents, { branch: "main", tier: "edge" }),
      20_000,
      "writer could not read its globally settled large document",
    );
    expect(documents).toEqual([expect.objectContaining({ title: "relay document", body })]);

    // Reopen the persistent worker from IndexedDB, rather than merely reading
    // the warm worker cache. This is the path that previously encountered the
    // oversized settled-program-fact key after an apparently successful write.
    await writer.shutdown();
    cleanup.untrack(writer);
    await sleep(100);
    const reopened = await withTimeout(
      openSyncedBrowserDb("large-value-transaction-reopen", secret, server, true, writerDbName),
      10_000,
      "reopened writer did not attach to persisted IndexedDB storage",
    );
    const reopenedDocuments = await withTimeout(
      reopened.all(transactionApp.documents, { branch: "main", tier: "edge" }),
      20_000,
      "reopened writer could not hydrate the settled large document",
    );
    expect(reopenedDocuments).toEqual([expect.objectContaining({ title: "relay document", body })]);
  }, 90_000);

  async function openSyncedBrowserDb(
    label: string,
    secret: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
    persistent = true,
    dbName = uniqueDbName(label),
  ): Promise<Db> {
    return cleanup.track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        secret,
        // The focused harness forwards the bounded redacted SharedWorker
        // flight recorder to Vitest, making a receipt failure inspectable.
        logLevel: "trace",
        driver: persistent ? { type: "persistent", dbName } : { type: "memory" },
      }),
    );
  }
});

function streamOf(...chunks: Array<string | Uint8Array>): ReadableStream<string | Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}
