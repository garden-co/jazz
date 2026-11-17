import { expect, it, vi } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { JazzClient } from "./client.js";
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
    const app = testApp(
      {
        tables: {
          plain: { columns: { title: "string" } },
          transformed: { columns: { title: { type: "string", transform: "upper" } } },
        },
        transforms: { upper: { stored: "string", view: "string" } },
      },
      {
        transforms: {
          upper: {
            from: (value: string) => value.toUpperCase(),
            to: (value: string) => value.toLowerCase(),
          },
        },
      },
    );
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
