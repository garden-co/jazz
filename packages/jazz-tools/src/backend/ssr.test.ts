import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import type { QueryBuilder } from "../runtime/db.js";
import { openSnapshot } from "./snapshot-envelope.js";
import { createSnapshotBuilder } from "./ssr.js";

const SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "id", column_type: { type: "Text" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

function makeQuery(): QueryBuilder<{ id: string; title: string }> {
  return {
    _table: "todos",
    _schema: {},
    _rowType: {} as { id: string; title: string },
    _build() {
      return JSON.stringify({ table: "todos", conditions: [], includes: {}, orderBy: [] });
    },
  };
}

describe("createSnapshotBuilder bundle emission", () => {
  it("emits the bundle form, base64-encoded per entry, when the Db composes a bundle", async () => {
    const bundleBytes = new Uint8Array([1, 2, 3, 4]);
    const db = {
      subscribeAll(_query: unknown, callback: (delta: { all: unknown[] }) => void) {
        callback({ all: [{ id: "1", title: "x" }] });
        return () => {};
      },
      composeQueryBundle() {
        return bundleBytes;
      },
    };

    const builder = createSnapshotBuilder({ appId: "app", schema: SCHEMA });
    await builder.prefetch(db as never, makeQuery());
    const env = openSnapshot(builder.dehydrate());

    expect(env.payload.kind).toBe("bundle");
    const [entry] = env.payload.entries as { bundle: string }[];
    expect(entry!.bundle).toBe(Buffer.from(bundleBytes).toString("base64"));
  });

  it("falls back to the rows form when the Db cannot compose a bundle", async () => {
    const db = {
      subscribeAll(_query: unknown, callback: (delta: { all: unknown[] }) => void) {
        callback({ all: [{ id: "1", title: "x" }] });
        return () => {};
      },
    };

    const builder = createSnapshotBuilder({ appId: "app2", schema: SCHEMA });
    await builder.prefetch(db as never, makeQuery());
    const env = openSnapshot(builder.dehydrate());

    expect(env.payload.kind).toBe("rows");
  });
});
