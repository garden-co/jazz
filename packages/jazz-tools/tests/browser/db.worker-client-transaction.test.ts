import { expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createAccountDbWithRuntimeSource } from "../../src/accounts/context.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { WorkerClientRuntimeSource } from "../../src/runtime/worker-client-runtime-source.js";

const app = s.defineApp({ records: s.table({ label: s.string(), payload: s.bytes() }, {}) });
it.each(["memory", "persistent"] as const)(
  "staged reads retain both resident and newly inserted rows on %s storage",
  async (type) => {
    const account = await localAccountConfig(crypto.randomUUID());
    const db = await createAccountDbWithRuntimeSource(
      {
        ...account,
        driver: type === "memory" ? { type } : { type, dbName: `worker-tx-${crypto.randomUUID()}` },
      },
      new WorkerClientRuntimeSource(),
    );
    try {
      await db
        .insert(app.records, { label: "resident", payload: new Uint8Array([1]) })
        .wait({ tier: "local" });
      const transaction = db.beginTransaction();
      transaction.insert(app.records, { label: "staged", payload: new Uint8Array([2]) });
      try {
        expect(
          (await transaction.all(app.records, { tier: "local" })).map((row) => row.label).sort(),
        ).toEqual(["resident", "staged"]);
        expect((await db.all(app.records, { tier: "local" })).map((row) => row.label)).toEqual([
          "resident",
        ]);
      } finally {
        await transaction.rollback();
      }
    } finally {
      await db.shutdown();
    }
  },
);

it("shutdown persists an ordinary worker mutation without a separate application wait", async () => {
  const account = await localAccountConfig(crypto.randomUUID());
  const config = {
    ...account,
    driver: { type: "persistent" as const, dbName: `worker-flush-${crypto.randomUUID()}` },
  };
  let db = await createAccountDbWithRuntimeSource(config, new WorkerClientRuntimeSource());
  const payload = Uint8Array.from({ length: 96 * 1024 }, (_, index) => index % 251);
  try {
    // Open first so shutdown tests an admitted mutation, not lazy client construction.
    expect(await db.all(app.records, { tier: "local" })).toEqual([]);
    const write = db.insert(app.records, { label: "durable", payload });
    await db.shutdown();
    db = await createAccountDbWithRuntimeSource(config, new WorkerClientRuntimeSource());
    expect(await db.all(app.records, { tier: "local" })).toEqual([write.value]);
  } finally {
    await db.shutdown();
  }
});

it.each(["relay", "ordinary"] as const)(
  "keeps an older foreground's pending row in a reopened %s transaction snapshot",
  async (role) => {
    const { DefaultRuntimeSource } = await import("../../src/runtime/default-runtime-source.js");
    const account = await localAccountConfig(crypto.randomUUID());
    const config = {
      ...account,
      driver: { type: "persistent" as const, dbName: `worker-old-snapshot-${crypto.randomUUID()}` },
    };
    let db = await createAccountDbWithRuntimeSource(config, new DefaultRuntimeSource());
    const write = db.insert(app.records, { label: "old foreground", payload: new Uint8Array([9]) });
    try {
      await write.wait({ tier: "local" });
      await db.shutdown();
      db = await createAccountDbWithRuntimeSource(
        config,
        role === "relay" ? new DefaultRuntimeSource() : new WorkerClientRuntimeSource(),
      );
      expect(await db.all(app.records, { tier: "local" })).toEqual([write.value]);
      const transaction = db.beginTransaction();
      try {
        expect(
          (await transaction.all(app.records, { tier: "local" })).map((row) => row.label),
        ).toEqual(["old foreground"]);
      } finally {
        await transaction.rollback();
      }
    } finally {
      await db.shutdown();
    }
  },
);
