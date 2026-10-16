import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import type { Db } from "../../src/runtime/db.js";
import { uniqueDbName } from "./factories.js";

const definition = {
  products: s.table(
    {
      price: s.float().transform({
        dependsOn: ["unit"],
        from: (value, context: Readonly<{ unit: string }>) =>
          context.unit === "cents" ? value / 100 : value,
        to: (value: number, context: Readonly<{ unit: string }>) =>
          context.unit === "cents" ? value * 100 : value,
      }),
      unit: s.string().default("cents"),
      label: s.string().default("product"),
    },
    {},
  ),
};
const app = s.defineApp(definition);

describe("dependency-aware transforms", () => {
  let db: Db | undefined;
  afterEach(async () => db?.shutdown());
  async function open() {
    db = await createDb({
      ...(await localAccountConfig("dependent-transforms")),
      driver: { type: "persistent", dbName: uniqueDbName("dependent-transforms") },
    });
    return db;
  }

  it("uses creation defaults and hides dependencies on selected reads", async () => {
    const db = await open();
    const table = app.products;
    const inserted = db.insert(table, { price: 1.25 });
    expect(inserted.value.price).toBe(1.25);
    expect(inserted.value.unit).toBe("cents");
    await expect(db.one(table.where({ id: inserted.value.id }).select("price"))).resolves.toEqual({
      id: inserted.value.id,
      price: 1.25,
    });
  });

  it("fetches missing update context and preserves unawaited transaction ordering", async () => {
    const db = await open();
    const table = app.products;
    const { value: row } = db.insert(table, { price: 1, unit: "cents" });
    await db.transaction(async (tx) => {
      tx.update(table, row.id, { price: 2 });
      tx.update(table, row.id, { label: "changed" });
      expect(await tx.one(table.where({ id: row.id }))).toMatchObject({
        price: 2,
        label: "changed",
      });
    });
    await db.update(table, row.id, { unit: "dollars" });
    await expect(db.one(table.where({ id: row.id }))).resolves.toMatchObject({ price: 200 });
  });

  it("rejects unknown, self and cyclic dependencies when defining a table", () => {
    const transform = (dependsOn: string[]) =>
      s.float().transform({
        dependsOn,
        from: (v) => v,
        to: (v: number) => v,
      });
    expect(() => s.table({ price: transform(["missing"]) }, {})).toThrow(/unknown.*missing/i);
    expect(() => s.table({ price: transform(["price"]) }, {})).toThrow(/self|itself/i);
    expect(() => s.table({ a: transform(["b"]), b: transform(["a"]) }, {})).toThrow(/cycl/i);
  });
});
