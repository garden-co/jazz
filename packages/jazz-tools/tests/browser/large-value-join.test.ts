/**
 * Browser receipt for #3830: one large value must not hold back the small
 * rows around it when a second persistent client joins.
 *
 * A listing that selects only names never reads the large column, so it must
 * not wait for that column's chunks. Before the fix, the joining client's
 * durable SharedWorker and its tab both rebuilt every large value their
 * retained subscriptions published, then dropped the column the `select`
 * excludes. Rebuilding meant fetching all ~24 MB through the relay first, and
 * the whole result batch waited for it: the joining client saw none of the
 * rows, not even the small ones, for well over a minute.
 */

import { afterEach, describe, expect, it } from "vitest";
import type { Db } from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { schema as s } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import {
  TestCleanup,
  createBrowserTestDb as createDb,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = s.defineApp({
  files: s.table({ name: s.string(), contents: s.bytes() }, {}),
});

const permissions = s.definePermissions(app, ({ policy }) => [
  policy.files.allowRead.always(),
  policy.files.allowInsert.always(),
  policy.files.allowUpdate.always(),
]);

const MB = 1024 * 1024;

describe("browser persistent join next to a large value", () => {
  const cleanup = new TestCleanup();

  afterEach(async () => {
    await withTimeout(cleanup.cleanup(), 20_000, "large-value join cleanup did not finish");
  });

  /// alice streams a ~24 MB file and writes small files beside it. bob, a
  /// different user on a fresh persistent replica, opens a listing of names.
  /// He must see every name promptly, and later small writes must keep
  /// arriving, without first downloading the large contents.
  ///
  /// alice tab ─insertStreaming(24 MB) + inserts─► alice worker ─► server
  ///                                                                  │
  /// bob tab ◄─select(id, name)─ bob worker ◄───────────────────────┘
  it("lists names beside a 24 MB value without downloading it", async () => {
    const server = await withTimeout(
      getJazzServerInfo(uniqueDbName("large-value-join")),
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

    const alice = await withTimeout(
      open("large-value-join-alice", server),
      10_000,
      "alice's tab did not attach to her persistent worker",
    );
    const big = await alice.insertStreaming(app.files, {
      name: "big.bin",
      contents: filledStream(24 * MB),
    });
    const small = alice.insert(app.files, { name: "small.txt", contents: new Uint8Array([1]) });
    await withTimeout(
      big.wait({ tier: "global" }),
      60_000,
      "alice's 24 MB upload did not reach the server",
    );
    await withTimeout(
      small.wait({ tier: "global" }),
      10_000,
      "alice's small write did not reach the server",
    );

    const bob = await withTimeout(
      open("large-value-join-bob", server),
      10_000,
      "bob's tab did not attach to his persistent worker",
    );
    let listed: string[] = [];
    const unsubscribe = bob.subscribe(app.files.select("id", "name"), (rows) => {
      listed = rows.map((row) => row.name).sort();
    });

    try {
      await waitForCondition(
        async () => listed.includes("big.bin") && listed.includes("small.txt"),
        20_000,
        "bob's listing did not show both names beside the large value",
      );
      expect(listed).toEqual(["big.bin", "small.txt"]);

      // A later small write still reaches the open listing promptly.
      await withTimeout(
        alice
          .insert(app.files, { name: "later.txt", contents: new Uint8Array([2]) })
          .wait({ tier: "global" }),
        10_000,
        "alice's later small write did not reach the server",
      );
      await waitForCondition(
        async () => listed.includes("later.txt"),
        10_000,
        "bob's listing did not show alice's later write",
      );
      expect(listed).toEqual(["big.bin", "later.txt", "small.txt"]);
    } finally {
      unsubscribe();
    }
  }, 150_000);

  async function open(
    label: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
  ): Promise<Db> {
    return cleanup.track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        secret: generateAuthSecret(),
        logLevel: "trace",
        driver: { type: "persistent", dbName: uniqueDbName(label) },
      }),
    );
  }
});

/// Streams `total` bytes in 64 KiB pieces, so the writer never holds the
/// whole value at once.
function filledStream(total: number): ReadableStream<Uint8Array> {
  let sent = 0;
  return new ReadableStream({
    pull(controller) {
      if (sent >= total) {
        controller.close();
        return;
      }
      const size = Math.min(64 * 1024, total - sent);
      sent += size;
      controller.enqueue(new Uint8Array(size).fill(sent % 251));
    },
  });
}
