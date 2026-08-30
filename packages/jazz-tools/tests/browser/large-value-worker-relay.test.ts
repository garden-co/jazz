/**
 * Browser receipt for large values travelling through the durable
 * tab -> SharedWorker -> server relay.  This deliberately uses the public
 * browser API and a real server/IndexedDB; the tab owns only an in-memory
 * runtime while the worker owns the durable replica.
 */

import { afterEach, describe, expect, it } from "vitest";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { schema as s } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, uniqueDbName, waitForQuery, withTimeout } from "./support.js";
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

  async function openSyncedBrowserDb(
    label: string,
    secret: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
    persistent = true,
  ): Promise<Db> {
    return cleanup.track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        secret,
        // The focused harness forwards the bounded redacted SharedWorker
        // flight recorder to Vitest, making a receipt failure inspectable.
        logLevel: "trace",
        driver: persistent
          ? { type: "persistent", dbName: uniqueDbName(label) }
          : { type: "memory" },
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
