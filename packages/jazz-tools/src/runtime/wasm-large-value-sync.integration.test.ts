import { randomUUID } from "node:crypto";
import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createDb } from "./default-create-db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

const app = s.defineApp({
  docs: s.table({ label: s.string(), body: s.string() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => {
  policy.docs.allowRead.always();
  policy.docs.allowInsert.always();
  policy.docs.allowUpdate.always();
});

// The memory driver uses WASM in Node; the sync server uses NAPI.
it.each([
  { operation: "insert", bytes: 64 * 1024 },
  { operation: "insert", bytes: 64 * 1024 + 1 },
  { operation: "insert", bytes: 512 * 1024 },
  { operation: "update", bytes: 512 * 1024 },
  { operation: "mergeable", bytes: 512 * 1024 },
  { operation: "exclusive", bytes: 512 * 1024 },
] as const)(
  "$operation syncs two pending values of $bytes bytes",
  async ({ operation, bytes }) => {
    const server = await startLocalJazzServer({
      appId: randomUUID(),
      inMemory: true,
      allowLocalFirstAuth: true,
    });
    onTestFinished(() => server.stop());
    await deploy({
      appId: server.appId,
      serverUrl: server.url,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    const alice = await createDb(await localAccountConfig(server.appId, server.url));
    onTestFinished(() => alice.shutdown());
    await alice.insert(app.docs, { label: "warmup", body: "ready" }).wait({ tier: "global" });

    const values = ["a", "b"].map((letter) => ({ label: letter, body: letter.repeat(bytes) }));
    const seeds =
      operation === "update"
        ? await Promise.all(
            values.map(({ label }) =>
              alice.insert(app.docs, { label, body: "seed" }).wait({ tier: "global" }),
            ),
          )
        : [];
    // Admit both writes before waiting: awaiting the first conceals the deadlock.
    const writes = values.map((value, index) => {
      if (operation === "update") return alice.update(app.docs, seeds[index]!.id, value);
      if (operation === "mergeable" || operation === "exclusive") {
        const tx =
          operation === "exclusive" ? alice.beginExclusiveTransaction() : alice.beginTransaction();
        tx.insert(app.docs, value);
        return tx.commit();
      }
      return alice.insert(app.docs, value);
    });
    await Promise.all(writes.map((write) => write.wait({ tier: "global" })));
    const rows = await alice.all(app.docs, { tier: "remote" });
    expect(rows).toHaveLength(3);
    for (const value of values) {
      expect(rows.find((row) => row.label === value.label)?.body).toBe(value.body);
    }
  },
  15_000,
);
