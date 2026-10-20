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

it.each(["constant", "identity"] as const)(
  "uses an ID-only physical projection when selecting only the %s computed field",
  async (column) => {
    const app = s.defineApp({
      rows: s.computed(s.table({ large: s.bytes() }, {}), {
        constant: { dependsOn: [], compute: () => "constant" },
        identity: { dependsOn: ["id"], compute: (row) => row.id },
      }),
      parents: s.table({ targetId: s.uuid() }, { target: s.rel("rows", "targetId") }),
    });
    const source = new ProjectionSource();
    const db = await createAccountDbWithRuntimeSource(
      await localAccountConfig(`computed-id-projection-${crypto.randomUUID()}`),
      source,
    );
    try {
      const { value: row } = db.insert(app.rows, { large: new Uint8Array([1, 2, 3]) });
      const { value: parent } = db.insert(app.parents, { targetId: row.id });
      const query = vi.spyOn(source.client, "queryInternal");
      const request = () =>
        JSON.parse(query.mock.calls.at(-1)![0]) as {
          select_columns?: { kind: string; column: string }[];
          array_subqueries: { select_columns: string[] | null }[];
        };
      const expected = { id: row.id, [column]: column === "constant" ? "constant" : row.id };

      await expect(db.one(app.rows.where({ id: row.id }).select(column))).resolves.toEqual(
        expected,
      );
      expect(request().select_columns).toEqual([{ kind: "full", column: "id" }]);

      await expect(
        db.one(
          app.parents.where({ id: parent.id }).include({
            target: app.rows.select(column),
          }),
        ),
      ).resolves.toMatchObject({ target: expected });
      expect(request().array_subqueries[0]?.select_columns).toEqual(["id"]);

      await expect(
        db.one(app.parents.where({ id: parent.id }).hopTo("target").select(column)),
      ).resolves.toEqual(expected);
      expect(request().select_columns).toEqual([{ kind: "full", column: "id" }]);
    } finally {
      vi.restoreAllMocks();
      await db.shutdown();
    }
  },
);

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
