import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { schema as s } from "../../src/index.js";
import type { Db } from "../../src/runtime/db.js";

const definition = {
  baskets: s.table({ product: s.uuid() }, { item: s.rel("products", "product") }),
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

export function testDependentTransforms(create: () => Promise<Db>) {
  describe("dependency-aware transforms", () => {
    let db: Db | undefined;
    afterEach(async () => db?.shutdown());
    async function open() {
      db = await create();
      return db;
    }

    it("uses creation defaults and hides dependencies on selected reads", async () => {
      const db = await open();
      const table = app.products;
      const inserted = db.insert(table, { price: 1.25 });
      expect(inserted.value.price).toBe(1.25);
      expect(inserted.value.unit).toBe("cents");
      await expect(
        db.one(table.where({ id: inserted.value.id }).select("*")),
      ).resolves.toMatchObject({ price: 1.25, unit: "cents" });
      await expect(db.one(table.where({ id: inserted.value.id }).select("price"))).resolves.toEqual(
        {
          id: inserted.value.id,
          price: 1.25,
        },
      );
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

    it("orders plain-table writes and transaction snapshots after dependency preparation", async () => {
      const db = await open();
      const { value: product } = db.insert(app.products, { price: 1 });
      const pending = db.update(app.products, product.id, { price: 2 });
      const basket = db.insert(app.baskets, { product: product.id });
      expect(basket.value.product).toBe(product.id);
      const tx = db.beginSerializableTransaction();
      tx.update(app.baskets, basket.value.id, { product: product.id });
      await expect(tx.one(app.products.where({ id: product.id }))).resolves.toMatchObject({
        price: 2,
      });
      await tx.commit().wait();
      await pending.wait({ tier: "local" });
      await expect(db.one(app.baskets.where({ id: basket.value.id }))).resolves.toEqual({
        id: basket.value.id,
        product: product.id,
      });
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

    it("prepares hidden dependencies in included rows and relationship hops", async () => {
      const db = await open();
      const { value: product } = db.insert(app.products, { price: 3.5 });
      const { value: basket } = db.insert(app.baskets, { product: product.id });
      const included = await db.one(
        app.baskets.where({ id: basket.id }).include({ item: app.products.select("price") }),
      );
      expect(included?.item).toEqual({ id: product.id, price: 3.5 });
      await expect(
        db.one(app.baskets.where({ id: basket.id }).hopTo("item").select("price")),
      ).resolves.toEqual({ id: product.id, price: 3.5 });
    });

    it("delivers dependency-only subscription updates with the original selection", async () => {
      const db = await open();
      const { value: row } = db.insert(app.products, { price: 2 });
      let stop = () => {};
      const updated = new Promise<unknown>((resolve) => {
        stop = db.subscribe(app.products.where({ id: row.id }).select("price"), (rows) => {
          if (rows[0]?.price === 200) resolve(rows[0]);
        });
      });
      db.update(app.products, row.id, { unit: "dollars" });
      await expect(updated).resolves.toEqual({ id: row.id, price: 200 });
      stop();
    });

    it("uses defaults only on creation, supports both upsert branches and restore", async () => {
      const db = await open();
      const id = "00000000-0000-4000-8000-000000000212";
      await db.upsert(app.products, id, { price: 2 }).wait({ tier: "local" });
      await expect(db.one(app.products.where({ id }))).resolves.toMatchObject({
        price: 2,
        unit: "cents",
      });
      await db.upsert(app.products, id, { price: 3, unit: "dollars" }).wait({ tier: "local" });
      await db.upsert(app.products, id, { price: 4 }).wait({ tier: "local" });
      await expect(db.one(app.products.where({ id }))).resolves.toMatchObject({
        price: 4,
        unit: "dollars",
      });
      await db.delete(app.products, id).wait({ tier: "local" });
      expect(db.restore(app.products, id, { price: 5 }).value).toMatchObject({
        price: 5,
        unit: "cents",
      });
      await expect(db.one(app.products.where({ id }))).resolves.toMatchObject({
        price: 5,
        unit: "cents",
      });
    });

    it("keeps immediate insert results while earlier automatic preparation is pending", async () => {
      const db = await open();
      const { value: first } = db.insert(app.products, { price: 1 });
      const update = db.update(app.products, first.id, { price: 2 });
      const inserted = db.insert(app.products, { price: 3 });
      expect(inserted.value).toMatchObject({ price: 3, unit: "cents" });
      await update.wait({ tier: "local" });
      await expect(db.one(app.products.where({ id: inserted.value.id }))).resolves.toMatchObject({
        price: 3,
      });
    });

    it("keeps immediate insert results when a conversion starts dependency preparation", async () => {
      let duringConversion = () => {};
      const models = s.defineApp({
        ...definition,
        events: s.table(
          {
            value: s.float().transform({
              from: (value) => value,
              to: (value: number) => {
                duringConversion();
                return value;
              },
            }),
          },
          {},
        ),
      });
      const db = await open();
      const { value: product } = db.insert(models.products, { price: 1 });
      duringConversion = () => {
        duringConversion = () => {};
        db.update(models.products, product.id, { price: 2 });
      };
      const inserted = db.insert(models.events, { value: 3 });
      expect(inserted.value).toEqual({ id: expect.any(String), value: 3 });
      await inserted.wait({ tier: "local" });
      await expect(db.one(models.events.where({ id: inserted.value.id }))).resolves.toEqual(
        inserted.value,
      );
      await expect(db.one(models.products.where({ id: product.id }))).resolves.toMatchObject({
        price: 2,
      });
    });

    it("cancels late preparation on rollback and waits for preparation on commit", async () => {
      const db = await open();
      const { value: row } = db.insert(app.products, { price: 1 });
      const rolledBack = db.beginTransaction();
      rolledBack.update(app.products, row.id, { price: 2 });
      await rolledBack.rollback();
      await expect(db.one(app.products.where({ id: row.id }))).resolves.toMatchObject({ price: 1 });
      const committed = db.beginTransaction();
      committed.update(app.products, row.id, { price: 3 });
      const inserted = committed.insert(app.products, { price: 4 });
      expect(inserted.price).toBe(4);
      const committing = committed.commit();
      expect(() => committed.update(app.products, row.id, { price: 99 })).toThrow(/commit/);
      await committing.wait({ tier: "local" });
      await expect(db.one(app.products.where({ id: row.id }))).resolves.toMatchObject({ price: 3 });
      await expect(db.one(app.products.where({ id: inserted.id }))).resolves.toMatchObject({
        price: 4,
      });
    });

    it("uses stored read contexts and app-facing write contexts independently of declaration order", async () => {
      const db = await open();
      const contexts: unknown[] = [];
      const models = s.defineApp({
        measurements: s.table(
          {
            amount: s.float().transform({
              dependsOn: ["scale"],
              from(value, context: Readonly<{ scale: number }>) {
                contexts.push(context);
                return value / context.scale;
              },
              to(value: number, context: Readonly<{ scale: string }>) {
                expect(Object.isFrozen(context)).toBe(true);
                expect(Object.keys(context)).toEqual(["scale"]);
                return value * Number(context.scale.replace("scale:", ""));
              },
            }),
            scale: s
              .int()
              .default(100)
              .transform({
                from: (value) => `scale:${value}`,
                to: (value: string) => Number(value.replace("scale:", "")),
              }),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.measurements, { amount: 2 });
      expect(row).toMatchObject({ amount: 2, scale: "scale:100" });
      await db
        .update(models.measurements, row.id, { amount: 3, scale: "scale:10" })
        .wait({ tier: "local" });
      await expect(
        db.one(models.measurements.where({ amount: 30 }).select("amount")),
      ).resolves.toEqual({ id: row.id, amount: 3 });
      expect(contexts).toContainEqual({ scale: 10 });
      await db.update(models.measurements, row.id, { amount: 4 }).wait({ tier: "local" });
      await expect(db.one(models.measurements.where({ amount: 40 }))).resolves.toMatchObject({
        amount: 4,
      });
    });

    it("passes full dependency values while preserving public partial selections", async () => {
      const db = await open();
      const models = s.defineApp({
        documents: s.table(
          {
            preview: s.string().transform({
              dependsOn: ["text"],
              from: (value, context: Readonly<{ text: string }>) => `${value}:${context.text}`,
              to: (value: string, _context: Readonly<{ text: string }>) => value,
            }),
            text: s.string(),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.documents, { preview: "abcdef", text: "uvwxyz" });
      await expect(
        db.one(
          models.documents.where({ id: row.id }).select({
            preview: { fromUtf8: 1, toUtf8: 3 },
            text: { fromUtf8: 2, toUtf8: 4 },
          }),
        ),
      ).resolves.toEqual({ id: row.id, preview: "bc:uvwxyz", text: "wx" });
    });

    it("preserves explicit null and supplies reserved ids in creation contexts", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s.table(
          {
            amount: s.int().transform({
              dependsOn: ["factor"],
              from: (value, context: Readonly<{ factor: number | null }>) =>
                value / (context.factor ?? 1),
              to: (value: number, context: Readonly<{ factor: number | null }>) =>
                value * (context.factor ?? 1),
            }),
            factor: s.int().optional().default(100),
            identity: s.string().transform({
              dependsOn: ["id"],
              from: (_value, context: Readonly<{ id: string }>) => context.id,
              to: (value: string, _context: Readonly<{ id: string }>) => value,
            }),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, { amount: 2, factor: null, identity: "seed" });
      expect(row).toMatchObject({ amount: 2, factor: null, identity: row.id });
      await db.update(models.rows, row.id, { amount: 3 }).wait({ tier: "local" });
      await expect(db.one(models.rows.where({ amount: 3 }))).resolves.toMatchObject({
        amount: 3,
        factor: null,
      });
    });

    it("propagates callback failures, fails an explicit transaction and recovers independent writes", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s.table(
          {
            value: s.int().transform({
              dependsOn: ["unit"],
              from: (value, _context: Readonly<{ unit: string }>) => value,
              to(value: number, _context: Readonly<{ unit: string }>) {
                if (value < 0) throw new Error("negative conversion");
                return value;
              },
            }),
            unit: s.string(),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, { value: 1, unit: "x" });
      const failed = db.update(models.rows, row.id, { value: -1 });
      const following = db.update(models.rows, row.id, { value: 2 });
      await expect(failed.wait({ tier: "local" })).rejects.toThrow("negative conversion");
      await following.wait({ tier: "local" });
      const tx = db.beginTransaction();
      tx.update(models.rows, row.id, { value: -1 });
      tx.update(models.rows, row.id, { unit: "must not stage" });
      await expect(tx.commit().wait({ tier: "local" })).rejects.toThrow("negative conversion");
      await tx.rollback();
      await expect(db.one(models.rows.where({ id: row.id }))).resolves.toMatchObject({
        value: 2,
        unit: "x",
      });
      const synchronous = db.beginTransaction();
      expect(() => synchronous.insert(models.rows, { value: -1, unit: "x" })).toThrow(
        "negative conversion",
      );
      expect(() => synchronous.update(models.rows, row.id, { value: 3, unit: "x" })).toThrow(
        "negative conversion",
      );
      await expect(synchronous.commit().wait({ tier: "local" })).rejects.toThrow(
        "negative conversion",
      );
      await synchronous.rollback();
    });

    it("rejects mixed streaming or partial dependencies but permits dependency-only edits", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s.table(
          {
            price: s.int().transform({
              dependsOn: ["text"],
              from: (value, context: Readonly<{ text: string }>) => value + context.text.length,
              to: (value: number, context: Readonly<{ text: string }>) =>
                value - context.text.length,
            }),
            text: s.string(),
            other: s.string(),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, { price: 10, text: "abc", other: "unchanged" });
      const source = () =>
        (async function* () {
          yield "hello";
        })();
      await expect(
        db.updateStreaming(models.rows, row.id, { price: 20, text: source() }),
      ).rejects.toThrow(/whole-value replacement/);
      const diffs = {
        text: { within: { from: 0, to: 3 }, splices: [{ at: 3, delete: 0, insert: "!" }] },
      };
      expect(() => db.update(models.rows, row.id, { price: 20 }, { applyDiffs: diffs })).toThrow(
        /whole-value replacement/,
      );
      await db.update(models.rows, row.id, {}, { applyDiffs: diffs }).wait({ tier: "local" });
      await expect(db.one(models.rows.where({ id: row.id }))).resolves.toMatchObject({
        price: 11,
        text: "abc!",
      });
      await db.updateStreaming(models.rows, row.id, { text: source() });
      await expect(db.one(models.rows.where({ id: row.id }))).resolves.toMatchObject({
        price: 12,
        text: "hello",
      });
      await db.updateStreaming(models.rows, row.id, { price: 20, other: source() });
      await expect(db.one(models.rows.where({ id: row.id }))).resolves.toMatchObject({
        price: 20,
        other: "hello",
      });
    });

    it("uses complete JSON and bytes dependencies with partial public selections", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s.table(
          {
            text: s.string().transform({
              dependsOn: ["json", "bytes"],
              from: (value, context: Readonly<{ json: { title: string }; bytes: Uint8Array }>) =>
                `${value}:${context.json.title}:${context.bytes.length}`,
              to: (
                value: string,
                _context: Readonly<{ json: { title: string }; bytes: Uint8Array }>,
              ) => value,
            }),
            json: s.json(z.object({ title: z.string() })),
            bytes: s.bytes(),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, {
        text: "abcdef",
        json: { title: "full" },
        bytes: new Uint8Array([1, 2, 3, 4]),
      });
      await expect(
        db.one(
          models.rows.where({ id: row.id }).select({
            text: { fromUtf8: 1, toUtf8: 3 },
            json: { at: "/title" },
            bytes: { from: 1, to: 3 },
          }),
        ),
      ).resolves.toEqual({
        id: row.id,
        text: "bc:full:4",
        json: "full",
        bytes: new Uint8Array([2, 3]),
      });
    });

    it("resolves transitive write contexts without running hidden read transforms", async () => {
      const db = await open();
      let factorReads = 0;
      const models = s.defineApp({
        rows: s.table(
          {
            amount: s.int().transform({
              dependsOn: ["factor"],
              from: (value, context: Readonly<{ factor: number }>) => `${value}:${context.factor}`,
              to: (value: string, context: Readonly<{ factor: number }>) =>
                Number(value) * context.factor,
            }),
            factor: s
              .int()
              .default(10)
              .transform({
                dependsOn: ["base"],
                from(value, context: Readonly<{ base: number }>) {
                  factorReads++;
                  return value * context.base;
                },
                to: (value: number, context: Readonly<{ base: number }>) => value / context.base,
              }),
            base: s.int().default(2),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, { amount: "2" });
      expect(row.amount).toBe("40:10");
      factorReads = 0;
      await expect(db.one(models.rows.where({ id: row.id }).select("amount"))).resolves.toEqual({
        id: row.id,
        amount: "40:10",
      });
      expect(factorReads).toBe(0);
      await db.update(models.rows, row.id, { amount: "3", base: 4 }).wait({ tier: "local" });
      await expect(db.one(models.rows.where({ factor: 10 }))).resolves.toMatchObject({
        amount: "120:10",
        factor: 40,
        base: 4,
      });
    });

    it("fails closed even when a callback throws a falsy value", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s.table(
          {
            value: s.int().transform({
              dependsOn: ["unit"],
              from: (value, _context: Readonly<{ unit: string }>) => value,
              to(value: number, _context: Readonly<{ unit: string }>) {
                if (value < 0) throw undefined;
                return value;
              },
            }),
            unit: s.string(),
          },
          {},
        ),
      });
      const { value: row } = db.insert(models.rows, { value: 1, unit: "x" });
      const tx = db.beginTransaction();
      tx.update(models.rows, row.id, { value: -1 });
      const committed = tx.commit();
      await expect(committed.txId).rejects.toBeUndefined();
      await tx.rollback();
      await expect(db.one(models.rows.where({ id: row.id }))).resolves.toMatchObject({ value: 1 });
    });

    it("fetches context from the mutation's branch view", async () => {
      const db = await open();
      const models = s.defineApp({
        rows: s
          .table(
            {
              price: definition.products.columns.price,
              unit: s.string().default("cents"),
              branch: s.string(),
            },
            {},
          )
          .branchBy("branch"),
      });
      const { value: row } = db.insert(
        models.rows,
        { price: 1, branch: "main" },
        { branch: "main" },
      );
      await db
        .update(models.rows, row.id, { unit: "dollars" }, { branch: "draft", base: "main" })
        .wait({ tier: "local" });
      await db
        .update(models.rows, row.id, { price: 2 }, { branch: "draft", base: "main" })
        .wait({ tier: "local" });
      await expect(
        db.one(models.rows.where({ id: row.id }), { branch: "draft", base: "main" }),
      ).resolves.toMatchObject({ price: 2, unit: "dollars" });
      await expect(
        db.one(models.rows.where({ id: row.id }), { branch: "main" }),
      ).resolves.toMatchObject({ price: 1, unit: "cents" });
    });

    it("rejects unavailable dependencies without creating or changing a row", async () => {
      const db = await open();
      const id = "00000000-0000-4000-8000-000000000299";
      await expect(
        db.update(app.products, id, { price: 2 }).wait({ tier: "local" }),
      ).rejects.toThrow(/unavailable/);
      await expect(db.one(app.products.where({ id }))).resolves.toBeNull();
    });

    it("does not use defaults for unavailable upsert creation context", async () => {
      const db = await open();
      const id = "00000000-0000-4000-8000-000000000299";
      const models = s.defineApp({
        rows: s.table({ price: definition.products.columns.price, unit: s.string() }, {}),
      });
      await expect(
        db.upsert(models.rows, id, { price: 2 }).wait({ tier: "local" }),
      ).rejects.toThrow(/unavailable/);
      await expect(db.one(models.rows.where({ id }))).resolves.toBeNull();
    });
  });
}
