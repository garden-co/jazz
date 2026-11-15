import { afterEach, describe, expect, it } from "vitest";
import type { Db } from "../../src/runtime/db.js";
import { app } from "./apps/computed-items/jazz.generated.js";
import { app as raw } from "./apps/computed-items-stored/jazz.generated.js";
import { app as safe } from "./apps/computed-rows-stored/jazz.generated.js";
import { app as hopOutputs } from "./apps/computed-hop-outputs/jazz.generated.js";
import { app as gatherOutputs } from "./apps/computed-gather-outputs/jazz.generated.js";
import { app as prototypeConstructor } from "./apps/computed-prototype-constructor/jazz.generated.js";
import { app as prototypeToString } from "./apps/computed-prototype-tostring/jazz.generated.js";
import { app as prototypeValueOf } from "./apps/computed-prototype-valueof/jazz.generated.js";
import { app as fullValues } from "./apps/computed-full-values/jazz.generated.js";
import { app as constantIdentity } from "./apps/computed-constant-identity/jazz.generated.js";
import { app as branched } from "./apps/computed-branch/jazz.generated.js";
import { app as failing } from "./apps/computed-failure/jazz.generated.js";
import { app as parseCaught } from "./apps/computed-parse-caught/jazz.generated.js";
import { app as parseWrapped } from "./apps/computed-parse-wrapped/jazz.generated.js";
import { app as dataStored } from "./apps/computed-data-stored/jazz.generated.js";
import { app as caughtTransformErrors } from "./apps/computed-caught-transform-errors/jazz.generated.js";
import { app as asyncCallbacks } from "./apps/computed-async/jazz.generated.js";
import { app as conditional } from "./apps/computed-conditional/jazz.generated.js";
import { app as nestedRows } from "./apps/computed-nested-rows/jazz.generated.js";
import { app as nestedSubscription } from "./apps/computed-nested-subscription/jazz.generated.js";
import { app as siblings } from "./apps/computed-siblings/jazz.generated.js";
import { app as rowEnumeration } from "./apps/computed-row-enumeration/jazz.generated.js";
import { app as invalidUnknown } from "./apps/computed-invalid-unknown/jazz.generated.js";
import { app as invalidCyclic } from "./apps/computed-invalid-cyclic/jazz.generated.js";
import { app as invalidMutation } from "./apps/computed-invalid-mutation/jazz.generated.js";
import { app as invalidAsync } from "./apps/computed-invalid-async/jazz.generated.js";

// Each case's app is committed under ./apps/computed-*/ (jazz.app.json, with
// its fields in jazz.computed.ts). `computed-items-stored` and
// `computed-rows-stored` are the same tables without computed fields.

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
      const models = hopOutputs;
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

    it("uses the gathered output table for stored and computed query guards", async () => {
      const models = gatherOutputs;
      const db = await open();
      const { value: target } = db.insert(models.targets, { rank: 7, nextId: null });
      const { value: source } = db.insert(models.sources, { targetId: target.id });
      const hopped = models.sources.where({ id: source.id }).hopTo("target");
      const gathered = hopped.gather({
        step: ({ current }) => models.targets.where({ id: current }).hopTo("next"),
        maxDepth: 1,
      });
      await expect(
        db.one(gathered.where({ rank: 7 }).orderBy("rank").select("doubled")),
      ).resolves.toEqual({ id: target.id, doubled: 14 });
      expect(() => gathered.where({ doubled: 14 } as never)).toThrow(/cannot be filtered/);
      expect(() => gathered.orderBy("doubled" as never)).toThrow(/cannot be ordered/);
    });

    it.each([
      ["constructor", prototypeConstructor],
      ["toString", prototypeToString],
      ["valueOf", prototypeValueOf],
    ] as const)(
      "reads and writes a stored %s dependency without using inherited transforms",
      async (column, models) => {
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

    it("gives computations full values while retaining partial public values", async () => {
      const models = fullValues;
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
      const models = constantIdentity;
      const db = await open();
      const { value: row } = db.insert(models.rows, { text: "hidden" });
      await expect(
        db.one(models.rows.where({ id: row.id }).select("constant", "identity")),
      ).resolves.toEqual({ id: row.id, constant: 42, identity: row.id });
    });

    it("retains the selected branch across dependency discovery and preserves null", async () => {
      const models = branched;
      const db = await open();
      const { value: row } = db.insert(
        models.rows,
        { score: null, branch: "main" },
        { branch: "main" },
      );
      await db
        .update(models.rows, row.id, { score: 20 }, { branch: "draft", base: "main" })
        .wait({ tier: "local" });
      const selected = models.rows.where({ id: row.id }).select("value");
      await expect(db.one(selected, { branch: "draft", base: "main" })).resolves.toEqual({
        id: row.id,
        value: 20,
      });
      await expect(db.one(selected, { branch: "main" })).resolves.toEqual({
        id: row.id,
        value: -1,
      });
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
      const broken = failing;
      const { value: row } = db.insert(safe.rows, {});
      await expect(db.one(broken.rows.where({ id: row.id }).select("failure"))).rejects.toThrow(
        "computed failure",
      );
      await expect(db.one(safe.rows.where({ id: row.id }))).resolves.toMatchObject({ score: 5 });
    });

    it.each([
      { text: '{"label":"hello"}', expected: { label: "hello" } },
      { text: "plain text", expected: "plain text" },
    ])("retries a caught missing dependency for $text", async ({ text, expected }) => {
      const models = parseCaught;
      const db = await open();
      const { value: row } = db.insert(safe.rows, { text });
      await expect(db.one(models.rows.select("parsed"))).resolves.toEqual({
        id: row.id,
        parsed: expected,
      });
    });

    it("retries missing dependencies when the callback wraps errors", async () => {
      const models = parseWrapped;
      const db = await open();
      const { value: row } = db.insert(safe.rows, { text: '{"valid":true}' });
      await expect(db.one(models.rows.select("parsed"))).resolves.toEqual({
        id: row.id,
        parsed: { valid: true },
      });
      await db.update(safe.rows, row.id, { text: "bad" }).wait({ tier: "local" });
      await expect(db.one(models.rows.select("parsed"))).rejects.toThrow("Invalid JSON");
    });

    it("preserves caught transform errors across repeated and separate computed reads", async () => {
      // `readFailure` (in jazz.computed.ts) catches the transform's JSON.parse error.
      const models = caughtTransformErrors;
      const safe = dataStored;
      const db = await open();
      const { value: row } = db.insert(safe.rows, { data: "bad" });
      for (const selected of [
        models.rows.select("first", "second", "repeated"),
        models.rows.select("repeated", "second", "first"),
      ]) {
        await expect(db.one(selected)).resolves.toEqual({
          id: row.id,
          first: "invalid JSON",
          second: "invalid JSON",
          repeated: ["invalid JSON", "invalid JSON"],
        });
      }
    });

    it("contains rejected async callbacks in the read error channel", async () => {
      const models = asyncCallbacks;
      const db = await open();
      db.insert(safe.rows, {});
      for (const column of ["missing", "failing"] as const) {
        await expect(db.one(models.rows.select(column))).rejects.toThrow(/synchronously/);
      }
      // Let rejected callback promises settle; the test runner fails on unhandled rejections.
      await new Promise((resolve) => setTimeout(resolve, 0));
      await expect(db.one(safe.rows.select("score"))).resolves.toMatchObject({ score: 5 });
    });

    it("discovers conditional dependencies and observes their later updates", async () => {
      const models = conditional;
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

    it("materialises row references nested in computed results before completing reads", async () => {
      const models = nestedRows;
      type Saved = { score: number };
      type Selected = {
        wrapper: { nested: [{ saved: Saved }] };
        mapping: Map<Saved, Saved>;
        membership: Set<Saved>;
      };
      const db = await open();
      // Insert through the stored schema so the selected read must discover its inputs.
      const { value: inserted } = db.insert(safe.rows, {});
      const selected = (await db.one(
        models.rows.where({ id: inserted.id }).select("wrapper", "mapping", "membership"),
      )) as unknown as Selected | null;
      const expected = { id: inserted.id, score: 5, text: "hello" };
      expect(selected!.wrapper.nested[0]!.saved.score).toBe(5);
      expect(selected!.wrapper.nested[0]!.saved).toEqual(expected);
      expect(Object.isFrozen(selected!.wrapper)).toBe(true);
      const [key, value] = [...selected!.mapping][0]!;
      expect(key.score).toBe(5);
      expect(value).toBe(key);
      expect([...selected!.membership][0]!.score).toBe(5);
      expect(Object.keys(selected!).sort()).toEqual(["id", "mapping", "membership", "wrapper"]);
      const immediate = db.insert(models.rows, {}).value as unknown as Selected;
      expect(immediate.wrapper.nested[0]!.saved.score).toBe(5);
    });

    it("observes dependencies of rows nested in computed subscription results", async () => {
      const models = nestedSubscription;
      const db = await open();
      const { value: row } = db.insert(safe.rows, {});
      let next: ((score: number) => void) | undefined;
      let fail: ((error: unknown) => void) | undefined;
      const waitFor = (score: number) =>
        new Promise<void>((resolve, reject) => {
          next = (value) => {
            if (value === score) resolve();
          };
          fail = reject;
        });
      const initial = waitFor(5);
      const stop = db.subscribe(models.rows.where({ id: row.id }).select("wrapper"), {
        onUpdate: (rows) => {
          try {
            if (rows[0])
              next?.((rows[0].wrapper as unknown as { saved: { score: number } }).saved.score);
          } catch (error) {
            fail?.(error);
          }
        },
        onError: (error) => fail?.(error),
      });
      try {
        await initial;
        const updated = waitFor(9);
        await db.update(safe.rows, row.id, { score: 9 }).wait({ tier: "local" });
        await updated;
      } finally {
        stop();
      }
    });

    it("keeps computed siblings out of the callback input row", async () => {
      const models = siblings;
      const db = await open();
      const { value: row } = db.insert(safe.rows, {});
      await expect(
        db.one(models.rows.select("toString", "valueOf", "tag", "original")),
      ).resolves.toEqual({
        id: row.id,
        toString: "custom",
        valueOf: "custom value",
        tag: "[object Object]",
        original: true,
      });
      expect(db.insert(models.rows, {}).value.tag).toBe("[object Object]");
    });

    it("supports row enumeration, JSON serialisation and returning a materialised row", async () => {
      const models = rowEnumeration;
      const db = await open();
      const { value: row } = db.insert(models.rows, {});
      const expected = { id: row.id, score: 5, text: "hello" };
      const result = await db.one(models.rows.select("json", "copy", "keys", "tag"));
      expect(JSON.parse(result!.json)).toEqual(expected);
      expect(result!.copy).toEqual(expected);
      expect(result!.keys).toEqual(["id", "score", "text"]);
      expect(result!.tag).toBe("[object Object]");
    });

    it("recomputes hidden included dependencies and stops when unsubscribed", async () => {
      const db = await open();
      const { value: row } = db.insert(app.items, {});
      const { value: basket } = db.insert(app.baskets, { itemId: row.id });
      let next: ((value: unknown) => void) | undefined;
      const events: unknown[] = [];
      const waitFor = (value: string) =>
        new Promise<void>((resolve) => {
          next = (event) => {
            if (event === value) resolve();
          };
        });
      const initial = waitFor("low");
      const stop = db.subscribe(
        app.baskets.where({ id: basket.id }).include({ item: app.items.select("priority") }),
        (rows) => {
          events.push(rows);
          next?.(rows[0]?.item?.priority);
        },
      );
      try {
        await initial;
        const updated = waitFor("high");
        await db.update(app.items, row.id, { score: 9 }).wait({ tier: "local" });
        await updated;
      } finally {
        stop();
      }
      const count = events.length;
      await db.update(app.items, row.id, { score: 1 }).wait({ tier: "local" });
      expect(events).toHaveLength(count);
    });

    it("rejects unknown dependencies, cycles, mutations and asynchronous callbacks at evaluation", async () => {
      const db = await open();
      const { value: row } = db.insert(safe.rows, {});
      // Each app's `invalid` (jazz.computed.ts) reads a column `from` doesn't name,
      // reads itself, writes its row, or returns a promise.
      const cases = [
        { broken: invalidUnknown, error: /unknown.*missing/i },
        { broken: invalidCyclic, error: /cyclic/i },
        { broken: invalidMutation, error: /read.only/i },
        { broken: invalidAsync, error: /synchronously/ },
      ];
      for (const { broken, error } of cases) {
        await expect(db.one(broken.rows.where({ id: row.id }).select("invalid"))).rejects.toThrow(
          error,
        );
      }
      await expect(db.one(safe.rows.where({ id: row.id }))).resolves.toMatchObject({ score: 5 });
    });
  });
}
