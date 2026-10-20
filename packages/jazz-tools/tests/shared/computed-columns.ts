import { afterEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { schema as s } from "../../src/index.js";
import type { Db } from "../../src/runtime/db.js";

const stored = s.table({ score: s.int().default(5), text: s.string().default("hello") }, {});
const items = s.table(stored.columns, {
  ...stored.relations,
  priority: s.computed((row): "low" | "high" => (row.score >= 8 ? "high" : "low")),
  description: s.computed((row) => `${row.id}:${row.text.length}`),
});
const definition = {
  items,
  baskets: s.table({ itemId: s.uuid() }, { item: s.rel("items", "itemId") }),
};
const app = s.defineApp(definition);
const raw = s.defineApp({ ...definition, items: stored });

export function testComputedColumns(create: () => Promise<Db>) {
  describe("virtual computed columns", () => {
    let db: Db;
    afterEach(async () => db?.shutdown());
    async function open() {
      db = await create();
      return db;
    }

    it("derives immediate results and selected reads without storing extra columns", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      expect(row).toEqual({
        id: row.id,
        score: 5,
        text: "hello",
        priority: "low",
        description: `${row.id}:5`,
      });
      expect(app.wasmSchema).toEqual(raw.wasmSchema);
      await expect(db.one(app.items.where({ id: row.id }).select("priority"))).resolves.toEqual({
        id: row.id,
        priority: "low",
      });
      await expect(db.one(app.items.where({ id: row.id }).select("score"))).resolves.toEqual({
        id: row.id,
        score: 5,
      });
      await expect(db.one(raw.items.where({ id: row.id }))).resolves.toEqual({
        id: row.id,
        score: 5,
        text: "hello",
      });
      await db.update(app.items, row.id, { score: 10 }).wait({ tier: "local" });
      await expect(db.one(app.items.where({ id: row.id }).select("priority"))).resolves.toEqual({
        id: row.id,
        priority: "high",
      });
    });

    it("recomputes subscriptions when only a hidden dependency changes", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      const events: unknown[] = [];
      let stop = () => {};
      const updated = new Promise<unknown>((resolve) => {
        stop = db.subscribe(app.items.where({ id: row.id }).select("priority"), (rows) => {
          events.push(rows[0]);
          if (rows[0]?.priority === "high") resolve(rows[0]);
        });
      });
      try {
        await db.update(app.items, row.id, { score: 9 }).wait({ tier: "local" });
        await expect(updated).resolves.toEqual({ id: row.id, priority: "high" });
        expect(
          events.every((event) => !event || Object.keys(event).sort().join(",") === "id,priority"),
        ).toBe(true);
      } finally {
        stop();
      }
    });

    it("prepares computed fields in includes and relationship hops", async () => {
      const db = await open();
      const { value: item } = db.insert(app.items, { score: 10 });
      const { value: basket } = db.insert(app.baskets, { itemId: item.id });
      const result = await db.one(
        app.baskets.where({ id: basket.id }).include({ item: app.items.select("priority") }),
      );
      expect(result?.item).toEqual({ id: item.id, priority: "high" });
      await expect(
        db.one(app.baskets.where({ id: basket.id }).hopTo("item").select("priority")),
      ).resolves.toEqual({ id: item.id, priority: "high" });
    });

    it("validates stored output columns against the destination after relationship hops", async () => {
      const models = s.defineApp({
        sources: s.table(
          { targetId: s.uuid() },
          {
            target: s.rel("targets", "targetId"),
            rank: s.computed(() => 0),
            title: s.computed(() => "source"),
          },
        ),
        targets: s.table(
          { rank: s.int(), title: s.string() },
          {
            sources: s.reverse("sources", "target"),
            label: s.computed((row) => row.title.toUpperCase()),
          },
        ),
      });
      const db = await open();
      const { value: target } = db.insert(models.targets, { rank: 7, title: "target" });
      const { value: source } = db.insert(models.sources, { targetId: target.id });
      const hopped = models.sources.where({ id: source.id }).hopTo("target");

      expect(() => hopped.where({ rank: 7 })).not.toThrow();
      await expect(db.one(hopped.orderBy("rank").select("rank"))).resolves.toEqual({
        id: target.id,
        rank: 7,
      });
      await expect(db.one(hopped.select({ title: { from: 0, to: 3 } }))).resolves.toEqual({
        id: target.id,
        title: "tar",
      });
      const roundTrip = hopped.hopTo("sources").hopTo("target");
      await expect(db.one(roundTrip.orderBy("rank").select("rank"))).resolves.toEqual({
        id: target.id,
        rank: 7,
      });
      expect(() => hopped.orderBy("label" as never)).toThrow(/cannot be ordered/);
      expect(() => hopped.where({ label: "TARGET" } as never)).toThrow(/cannot be filtered/);
      expect(() => hopped.select({ label: { from: 0, to: 3 } } as never)).toThrow(
        /cannot have a stored partial selection/,
      );
    });

    it.each(["constructor", "toString", "valueOf"] as const)(
      "reads and writes a stored %s dependency without using inherited transforms",
      async (column) => {
        const models = s.defineApp({
          rows: s.table(
            { constructor: s.string(), toString: s.string(), valueOf: s.string() },
            { computedValue: s.computed((row) => row[column].toUpperCase()) },
          ),
        });
        const db = await open();
        const { value: row } = db.insert(models.rows, {
          constructor: "constructor",
          toString: "toString",
          valueOf: "valueOf",
        });
        expect(row.computedValue).toBe(column.toUpperCase());
        await expect(db.one(models.rows.select("computedValue"))).resolves.toEqual({
          id: row.id,
          computedValue: column.toUpperCase(),
        });
        await db
          .update(models.rows, row.id, {
            constructor: "updated",
            toString: "updated",
            valueOf: "updated",
          })
          .wait({ tier: "local" });
        await expect(db.one(models.rows.select("computedValue"))).resolves.toEqual({
          id: row.id,
          computedValue: "UPDATED",
        });
      },
    );

    it("uses app-facing transformed dependencies and stored contexts independently", async () => {
      const base = s.table(
        {
          amount: s.int().transform({
            dependsOn: ["factor"],
            from: (value, context: { readonly factor: number }) => value / context.factor,
            to: (value: number, context: { readonly factor: string }) =>
              value * Number(context.factor.slice(1)),
          }),
          factor: s
            .int()
            .default(100)
            .transform({
              from: (value) => `x${value}`,
              to: (value: string) => Number(value.slice(1)),
            }),
        },
        {},
      );
      const columns = s.table(base.columns, {
        ...base.relations,
        summary: s.computed((row) => `${row.amount}:${row.factor}`),
      });
      const models = s.defineApp({
        rows: s.table(columns.columns, {
          ...columns.relations,
          nested: s.computed((row) => `[${row.summary}]`),
        }),
      });
      const db = await open();
      const { value: row } = db.insert(models.rows, { amount: 2, factor: "x100" });
      expect(row.nested).toBe("[2:x100]");
      await expect(db.one(models.rows.where({ id: row.id }).select("nested"))).resolves.toEqual({
        id: row.id,
        nested: "[2:x100]",
      });
      const unchanged = s.defineApp({ rows: base });
      await expect(db.one(unchanged.rows.where({ id: row.id }))).resolves.toEqual({
        id: row.id,
        amount: 2,
        factor: "x100",
      });
    });

    it("gives computations full values while retaining partial public values", async () => {
      const models = s.defineApp({
        rows: s.table(
          {
            text: s.string().transform({
              from: (value) => value.toUpperCase(),
              to: (value: string) => value.toLowerCase(),
            }),
            json: s.json(z.object({ title: z.string(), extra: z.string() })),
            bytes: s.bytes(),
          },
          { summary: s.computed((row) => `${row.text}:${row.json.extra}:${row.bytes.length}`) },
        ),
      });
      const db = await open();
      const { value: row } = db.insert(models.rows, {
        text: "abcdef",
        json: { title: "title", extra: "full" },
        bytes: new Uint8Array([1, 2, 3, 4]),
      });
      await expect(
        db.one(
          models.rows.where({ id: row.id }).select(
            {
              text: { fromUtf8: 1, toUtf8: 3 },
              json: { at: "/title" },
              bytes: { from: 1, to: 3 },
            },
            "summary",
          ),
        ),
      ).resolves.toEqual({
        id: row.id,
        text: "BC",
        json: "title",
        bytes: new Uint8Array([2, 3]),
        summary: "ABCDEF:full:4",
      });
    });

    it("sees transaction writes and preserves upsert and restore results", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      await db.transaction(async (tx) => {
        tx.update(app.items, row.id, { score: 10 });
        await expect(tx.one(app.items.where({ id: row.id }).select("priority"))).resolves.toEqual({
          id: row.id,
          priority: "high",
        });
        const inserted = tx.insert(app.items, { score: 9 });
        expect(inserted.priority).toBe("high");
      });
      const id = "00000000-0000-4000-8000-000000000499";
      await db.upsert(app.items, id, { score: 9 }).wait({ tier: "local" });
      await expect(db.one(app.items.where({ id }))).resolves.toMatchObject({ priority: "high" });
      await db.upsert(app.items, id, { score: 1 }).wait({ tier: "local" });
      await expect(db.one(app.items.where({ id }))).resolves.toMatchObject({ priority: "low" });
      await db.delete(app.items, id).wait({ tier: "local" });
      expect(db.restore(app.items, id, { score: 10 }).value.priority).toBe("high");
    });

    it("allows dependency-only streams and partial edits", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      await db
        .update(
          app.items,
          row.id,
          {},
          {
            applyDiffs: {
              text: { within: { from: 0, to: 5 }, splices: [{ at: 5, delete: 0, insert: "!" }] },
            },
          },
        )
        .wait({ tier: "local" });
      await expect(db.one(app.items.where({ id: row.id }).select("description"))).resolves.toEqual({
        id: row.id,
        description: `${row.id}:6`,
      });
      async function* source() {
        yield "longer text";
      }
      await db.updateStreaming(app.items, row.id, { text: source() });
      await expect(db.one(app.items.where({ id: row.id }).select("description"))).resolves.toEqual({
        id: row.id,
        description: `${row.id}:11`,
      });
    });

    it("handles constant and id-only fields without returning unrelated columns", async () => {
      const models = s.defineApp({
        rows: s.table(
          { text: s.string() },
          { constant: s.computed(() => 42), identity: s.computed((row) => row.id) },
        ),
      });
      const db = await open();
      const { value: row } = db.insert(models.rows, { text: "hidden" });
      await expect(
        db.one(models.rows.where({ id: row.id }).select("constant", "identity")),
      ).resolves.toEqual({ id: row.id, constant: 42, identity: row.id });
    });

    it("rejects writes and stored queries for computed fields even through unsafe inputs", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      expect(() => db.insert(app.items, { priority: undefined } as never)).toThrow(/read.only/);
      expect(() => db.update(app.items, row.id, { priority: "high" } as never)).toThrow(
        /read.only/,
      );
      expect(() => app.items.where({ priority: "high" } as never)).toThrow(/cannot be filtered/);
      expect(() => app.items.orderBy("priority" as never)).toThrow(/cannot be ordered/);
      await expect(db.one(raw.items.where({ id: row.id }))).resolves.toMatchObject({ score: 5 });
    });

    it("propagates callback failures without breaking independent reads", async () => {
      const db = await open();
      const broken = s.defineApp({
        rows: s.table(stored.columns, {
          ...stored.relations,
          failure: s.computed(() => {
            throw new Error("computed failure");
          }),
        }),
      });
      const safe = s.defineApp({ rows: stored });
      const { value: row } = db.insert(safe.rows, {});
      await expect(db.one(broken.rows.where({ id: row.id }).select("failure"))).rejects.toThrow(
        "computed failure",
      );
      await expect(db.one(safe.rows.where({ id: row.id }))).resolves.toMatchObject({ score: 5 });
    });

    it("discovers conditional dependencies and observes their later updates", async () => {
      const models = s.defineApp({
        rows: s.table(
          { useLeft: s.boolean(), left: s.int(), right: s.int() },
          { value: s.computed((row) => (row.useLeft ? row.left : row.right)) },
        ),
      });
      const db = await open();
      const { value: row } = db.insert(models.rows, { useLeft: true, left: 10, right: 20 });
      let next: ((value: unknown) => void) | undefined;
      const errors: unknown[] = [];
      const events: unknown[] = [];
      const waitFor = (value: number) =>
        new Promise<void>((resolve) => {
          next = (event) => {
            if ((event as { value: number }).value === value) resolve();
          };
        });
      const initial = waitFor(10);
      const stop = db.subscribe(models.rows.where({ id: row.id }).select("value"), {
        onUpdate: (rows) => {
          events.push(rows[0]);
          next?.(rows[0]);
        },
        onError: (error) => errors.push(error),
      });
      try {
        await initial;
        const switched = waitFor(20);
        await db.update(models.rows, row.id, { useLeft: false }).wait({ tier: "local" });
        await switched;
        const updated = waitFor(30);
        await db.update(models.rows, row.id, { right: 30 }).wait({ tier: "local" });
        await updated;
        expect(events.at(-1)).toEqual({ id: row.id, value: 30 });
        expect(
          events.every(
            (event) =>
              Object.keys(event as object)
                .sort()
                .join() === "id,value",
          ),
        ).toBe(true);
        expect(errors).toEqual([]);
      } finally {
        stop();
      }
    });

    it("rejects unknown dependencies, cycles, mutations and asynchronous callbacks at evaluation", async () => {
      const db = await open();
      const safe = s.defineApp({ rows: stored });
      const { value: row } = db.insert(safe.rows, {});
      const cases = [
        {
          callback: (value: Readonly<Record<string, unknown>>) => value.missing,
          error: /unknown.*missing/i,
        },
        { callback: (value: Readonly<Record<string, unknown>>) => value.invalid, error: /cyclic/i },
        {
          callback: (value: Readonly<Record<string, unknown>>) => {
            (value as Record<string, unknown>).score = 0;
          },
          error: /read.only/i,
        },
        { callback: () => Promise.resolve(1), error: /synchronously/ },
      ];
      for (const { callback, error } of cases) {
        const broken = s.defineApp({
          rows: s.table(stored.columns, { invalid: s.computed(callback) }),
        });
        await expect(db.one(broken.rows.where({ id: row.id }).select("invalid"))).rejects.toThrow(
          error,
        );
      }
      await expect(db.one(safe.rows.where({ id: row.id }))).resolves.toMatchObject({ score: 5 });
    });
  });
}
