/**
 * Null passes through a column transform: Jazz never calls `from` or `to`
 * with null, so an alpha.65 transform's null branch (kept as is by
 * `jazz-tools upgrade`) is never reached, and null reads and writes as null.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { testApp } from "../testing/test-app.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import type { Db } from "./db.js";
import { createDb } from "./default-create-db.js";

describe("column transforms and null", () => {
  let db: Db | undefined;
  afterEach(async () => {
    await db?.shutdown();
    db = undefined;
  });

  it("passes null through without calling the transform, and transforms other values", async () => {
    // As `jazz-tools upgrade` moves `s.timestamp().optional().transform({ from, to })`.
    const from = vi.fn((v: Date | null) => (v == null ? null : v.toISOString().slice(0, 10)));
    const to = vi.fn((v: string | null) => (v == null ? null : new Date(`${v}T00:00:00Z`)));
    const app = testApp(
      {
        tables: {
          tasks: {
            columns: { title: "string", dueAt: { type: "timestamp?", transform: "isoDate" } },
          },
        },
        transforms: { isoDate: { stored: "timestamp", view: "string" } },
      },
      { transforms: { isoDate: { from, to } } },
    );
    db = await createDb({
      ...(await localAccountConfig("transform-null-pass-through")),
      driver: { type: "memory" },
    });

    const { value: empty } = db.insert(app.tasks, { title: "no date", dueAt: null });
    const { value: dated } = db.insert(app.tasks, { title: "dated", dueAt: "2026-12-01" });
    const rows = await db.all(app.tasks);
    const byId = new Map(rows.map((row) => [row.id, row]));
    expect(byId.get(empty.id)?.dueAt).toBeNull();
    expect(byId.get(dated.id)?.dueAt).toBe("2026-12-01");

    for (const call of [...from.mock.calls, ...to.mock.calls]) expect(call[0]).not.toBeNull();
    expect(to).toHaveBeenCalledWith("2026-12-01");
    expect(from).toHaveBeenCalled();

    // Setting it back to null doesn't reach the transform either.
    const calls = to.mock.calls.length;
    await db.update(app.tasks, dated.id, { dueAt: null }).wait({ tier: "local" });
    expect(to.mock.calls.length).toBe(calls);
    const [cleared] = await db.all(app.tasks.where({ id: dated.id }));
    expect(cleared?.dueAt).toBeNull();
  });

  it("refuses null from a transform for a required column, in both directions", async () => {
    const app = testApp(
      {
        tables: {
          tags: { columns: { label: { type: "string", transform: "trimmed" } } },
        },
        transforms: { trimmed: { stored: "string", view: "string" } },
      },
      {
        transforms: {
          trimmed: {
            from: (v: string) => (v === "blank" ? null : v),
            to: (v: string) => (v.trim() === "" ? null : v.trim()),
          },
        },
      },
    );
    db = await createDb({
      ...(await localAccountConfig("transform-null-required")),
      driver: { type: "memory" },
    });
    expect(() => db!.insert(app.tags, { label: "   " })).toThrow(
      /The transform "trimmed" returned null from `to` for tags\.label, which is required/,
    );
    // The insert reads its row back through `from`, which refuses there.
    expect(() => db!.insert(app.tags, { label: "blank" })).toThrow(
      /The transform "trimmed" returned null from `from` for tags\.label, which is required/,
    );
  });
});
