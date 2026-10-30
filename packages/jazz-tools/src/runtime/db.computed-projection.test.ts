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
    const app = s.defineApp(
      {
        rows: s.table(
          { large: s.bytes() },
          { constant: s.computed(() => "constant"), identity: s.computed((row) => row.id) },
        ),
        parents: s.table({ targetId: s.uuid() }, { target: s.rel("rows", "targetId") }),
      },
      { migrations: "none" },
    );
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
  const base = s.table(
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
    { derived: s.computed((row) => row.amount) },
  );
  const app = s.defineApp(
    {
      rows: s.table(base.columns, {
        ...base.relations,
        nested: s.computed((row) => `value:${row.derived}`),
      }),
    },
    { migrations: "none" },
  );
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

it("reuses available inputs and batches missing dependencies without whole-row reads", async () => {
  const app = s.defineApp(
    {
      rows: s.table(
        { useLeft: s.boolean(), left: s.int(), right: s.int(), large: s.bytes() },
        { value: s.computed((row) => (row.useLeft ? row.left : row.right)) },
      ),
    },
    { migrations: "none" },
  );
  const source = new ProjectionSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`computed-lazy-${crypto.randomUUID()}`),
    source,
  );
  try {
    db.insert(app.rows, { useLeft: true, left: 10, right: 20, large: new Uint8Array([1]) });
    db.insert(app.rows, { useLeft: false, left: 30, right: 40, large: new Uint8Array([2]) });
    const query = vi.spyOn(source.client, "queryInternal");
    const results = await db.all(app.rows.select("useLeft", "left", "right", "value"));
    expect(results.map((row) => row.value).sort()).toEqual([10, 40]);
    expect(query).toHaveBeenCalledTimes(1);
    query.mockClear();
    expect((await db.all(app.rows.select("value"))).map((row) => row.value).sort()).toEqual([
      10, 40,
    ]);
    expect(query).toHaveBeenCalledTimes(1);
    const selections = query.mock.calls.map(([json]) =>
      (JSON.parse(json).select_columns as { column: string }[])
        .map((column) => column.column)
        .sort(),
    );
    expect(selections).toEqual([["left", "right", "useLeft"]]);
  } finally {
    vi.restoreAllMocks();
    await db.shutdown();
  }
});

it("reuses subscription dependencies while still discovering newly taken branches", async () => {
  const app = s.defineApp(
    {
      rows: s.table(
        { useLeft: s.boolean(), left: s.int(), right: s.int(), large: s.bytes() },
        { value: s.computed((row) => (row.useLeft ? row.left : row.right)) },
      ),
    },
    { migrations: "none" },
  );
  const source = new ProjectionSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`computed-subscription-cache-${crypto.randomUUID()}`),
    source,
  );
  let stop: (() => void) | undefined;
  try {
    const { value: row } = db.insert(app.rows, {
      useLeft: true,
      left: 10,
      right: 20,
      large: new Uint8Array([1]),
    });
    const subscribe = vi.spyOn(source.client, "subscribeInternal");
    let resolveUpdate: (value: unknown) => void = () => {};
    let rejectUpdate: (reason: unknown) => void = () => {};
    const nextUpdate = () =>
      new Promise<unknown>((resolve, reject) => {
        resolveUpdate = resolve;
        rejectUpdate = reject;
      });
    const start = () =>
      db.subscribe(app.rows.where({ id: row.id }).select("value"), {
        onUpdate: (rows) => resolveUpdate(rows),
        onError: (error) => rejectUpdate(error),
      });

    const initial = nextUpdate();
    stop = start();
    await expect(initial).resolves.toEqual([{ id: row.id, value: 10 }]);
    expect(subscribe).toHaveBeenCalledTimes(3);
    stop();
    subscribe.mockClear();

    const reopened = nextUpdate();
    stop = start();
    await expect(reopened).resolves.toEqual([{ id: row.id, value: 10 }]);
    expect(subscribe).toHaveBeenCalledTimes(1);
    const selection = JSON.parse(subscribe.mock.calls[0]![0]).select_columns as {
      column: string;
    }[];
    expect(selection.map((column) => column.column).sort()).toEqual(["left", "useLeft"]);

    const switched = nextUpdate();
    await db.update(app.rows, row.id, { useLeft: false }).wait({ tier: "local" });
    await expect(switched).resolves.toEqual([{ id: row.id, value: 20 }]);
    expect(subscribe).toHaveBeenCalledTimes(2);

    const updated = nextUpdate();
    await db.update(app.rows, row.id, { right: 30 }).wait({ tier: "local" });
    await expect(updated).resolves.toEqual([{ id: row.id, value: 30 }]);
    expect(subscribe).toHaveBeenCalledTimes(2);
  } finally {
    stop?.();
    vi.restoreAllMocks();
    await db.shutdown();
  }
});

it("keeps an in-flight read independent of another read extending partially warm hints", async () => {
  const app = s.defineApp(
    {
      rows: s.table(
        { useLeft: s.boolean(), left: s.int(), right: s.int() },
        { value: s.computed((row) => (row.useLeft ? row.left : row.right)) },
      ),
    },
    { migrations: "none" },
  );
  const source = new ProjectionSource();
  const db = await createAccountDbWithRuntimeSource(
    await localAccountConfig(`computed-concurrent-cache-${crypto.randomUUID()}`),
    source,
  );
  let release = () => {};
  const blocked = new Promise<void>((resolve) => {
    release = resolve;
  });
  let entered = () => {};
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  try {
    const { value: first } = db.insert(app.rows, { useLeft: true, left: 10, right: 20 });
    const { value: row } = db.insert(app.rows, { useLeft: false, left: 10, right: 20 });
    await expect(db.one(app.rows.where({ id: first.id }).select("value"))).resolves.toEqual({
      id: first.id,
      value: 10,
    });
    const original = source.client.queryInternal.bind(source.client);
    const query = vi.spyOn(source.client, "queryInternal");
    query.mockImplementationOnce(async (...args) => {
      const rows = await original(...args);
      entered();
      await blocked;
      return rows;
    });
    const selected = app.rows.where({ id: row.id }).select("value");
    const pending = db.one(selected);
    const result = expect(pending).resolves.toEqual({ id: row.id, value: 20 });
    await started;
    await expect(db.one(selected)).resolves.toEqual({ id: row.id, value: 20 });
    release();
    await result;

    query.mockClear();
    await db.update(app.rows, row.id, { right: 30 }).wait({ tier: "local" });
    await expect(db.one(selected)).resolves.toEqual({ id: row.id, value: 30 });
    expect(query).toHaveBeenCalledTimes(1);
  } finally {
    release();
    vi.restoreAllMocks();
    await db.shutdown();
  }
});
