import { expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import { beginDbTransactionAfter } from "./db.js";
import type { JazzClient, TransactionalRuntime } from "./client.js";
import type { RuntimeClientContext } from "./runtime-source.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

class PreparationSource extends DefaultRuntimeSource {
  client!: JazzClient;
  override createClient(context: RuntimeClientContext): JazzClient {
    this.client = super.createClient(context);
    return this.client;
  }
}

it.each(["mergeable", "serializable"] as const)(
  "keeps generated IDs native for immediate %s transaction inserts without dependencies",
  async (kind) => {
    const app = s.defineApp({
      plain: s.table({ title: s.string() }, {}),
      transformed: s.table(
        {
          title: s.string().transform({
            from: (value) => value.toUpperCase(),
            to: (value: string) => value.toLowerCase(),
          }),
        },
        {},
      ),
    });
    const source = new PreparationSource();
    const db = await createAccountDbWithRuntimeSource(
      await localAccountConfig(`transform-insert-${crypto.randomUUID()}`),
      source,
    );
    try {
      await db.one(app.plain);
      const tx =
        kind === "serializable" ? db.beginSerializableTransaction() : db.beginTransaction();
      const insert = vi.spyOn(source.client, "insertInternal");
      const plain = tx.insert(app.plain, { title: "plain" });
      const transformed = tx.insert(app.transformed, { title: "Mixed" });
      expect(plain).toEqual({ id: expect.any(String), title: "plain" });
      expect(transformed).toEqual({ id: expect.any(String), title: "MIXED" });
      expect(insert.mock.calls.map((call) => call[2]?.id)).toEqual([undefined, undefined]);
      await tx.commit().wait({ tier: "local" });
      await expect(db.one(app.plain.where({ id: plain.id }))).resolves.toEqual(plain);
      await expect(db.one(app.transformed.where({ id: transformed.id }))).resolves.toEqual(
        transformed,
      );
    } finally {
      vi.restoreAllMocks();
      await db.shutdown();
    }
  },
);

it("retains transaction admission when an earlier transform lookup delays opening", async () => {
  const app = s.defineApp({
    prices: s.table(
      {
        amount: s.float().transform({
          dependsOn: ["scale"],
          from: (value, context: Readonly<{ scale: number }>) => value / context.scale,
          to: (value: number, context: Readonly<{ scale: number }>) => value * context.scale,
        }),
        scale: s.float().default(100),
      },
      {},
    ),
  });
  const source = new PreparationSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`transform-admission-${crypto.randomUUID()}`),
    source,
  );
  let releaseLookup!: () => void;
  let releaseAdmission!: () => void;
  const lookup = new Promise<void>((resolve) => {
    releaseLookup = resolve;
  });
  const admission = new Promise<void>((resolve) => {
    releaseAdmission = resolve;
  });
  let completion: Promise<unknown> | undefined;
  try {
    const { value: row } = db.insert(app.prices, { amount: 1 });
    const originalQuery = source.client.queryInternal.bind(source.client);
    vi.spyOn(source.client, "queryInternal").mockImplementationOnce(async (...args) => {
      await lookup;
      return originalQuery(...args);
    });
    const pending = db.update(app.prices, row.id, { amount: 2 });
    const opening = vi.spyOn(
      source.client.getRuntime() as TransactionalRuntime,
      "beginTransaction",
    );
    const prerequisite = vi.fn(() => admission);
    const tx = beginDbTransactionAfter(db, prerequisite);
    tx.update(app.prices, row.id, { amount: 3, scale: 100 });
    completion = tx.commit().wait();
    completion.catch(() => {});
    releaseLookup();
    await pending.wait({ tier: "local" });
    await vi.waitFor(() => expect(prerequisite).toHaveBeenCalledTimes(1));
    expect(opening).not.toHaveBeenCalled();
    await expect(db.one(app.prices.where({ id: row.id }))).resolves.toMatchObject({ amount: 2 });
    releaseAdmission();
    await completion;
    await expect(db.one(app.prices.where({ id: row.id }))).resolves.toMatchObject({ amount: 3 });
  } finally {
    releaseLookup();
    releaseAdmission();
    await completion?.catch(() => {});
    vi.restoreAllMocks();
    await db.shutdown();
  }
});
