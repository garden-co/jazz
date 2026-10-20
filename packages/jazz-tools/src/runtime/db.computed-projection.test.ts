import { expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createAccountDbWithRuntimeSource } from "../accounts/context.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { JazzClient } from "./client.js";
import type { RuntimeClientContext } from "./runtime-source.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

class ProjectionSource extends DefaultRuntimeSource {
  client!: JazzClient;

  override createClient(context: RuntimeClientContext): JazzClient {
    this.client = super.createClient(context);
    return this.client;
  }
}

it("fetches only the stored inputs of callbacks evaluated by a computed read", async () => {
  const app = s.defineApp({
    rows: s.computed(
      s.computed(
        s.table(
          {
            amount: s.int().transform({
              dependsOn: ["text"],
              from: (value, context: Readonly<{ text: string }>) => value + context.text.length,
              to: (value: number) => value,
            }),
            text: s.string().transform({
              dependsOn: ["large"],
              from: (value, context: Readonly<{ large: Uint8Array }>) =>
                `${value}:${context.large.length}`,
              to: (value: string) => value,
            }),
            large: s.bytes(),
          },
          {},
        ),
        { derived: { dependsOn: ["amount"], compute: (row) => row.amount } },
      ),
      { nested: { dependsOn: ["derived"], compute: (row) => `value:${row.derived}` } },
    ),
  });
  const source = new ProjectionSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`computed-projection-${crypto.randomUUID()}`),
    source,
  );
  try {
    const { value: row } = db.insert(app.rows, {
      amount: 1,
      text: "raw",
      large: new Uint8Array([1, 2, 3]),
    });
    const query = vi.spyOn(source.client, "queryInternal");
    const selectedColumns = () => {
      const request = JSON.parse(query.mock.calls.at(-1)![0]) as {
        select_columns: { kind: string; column: string }[];
      };
      return request.select_columns.map((column) => column.column).sort();
    };

    await expect(db.one(app.rows.where({ id: row.id }).select("nested"))).resolves.toEqual({
      id: row.id,
      nested: "value:4",
    });
    expect(selectedColumns()).toEqual(["amount", "text"]);

    await expect(db.one(app.rows.where({ id: row.id }).select("nested", "text"))).resolves.toEqual({
      id: row.id,
      nested: "value:4",
      text: "raw:3",
    });
    expect(selectedColumns()).toEqual(["amount", "large", "text"]);
  } finally {
    vi.restoreAllMocks();
    await db.shutdown();
  }
});
