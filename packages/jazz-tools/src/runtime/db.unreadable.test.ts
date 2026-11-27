import { expect, it, vi } from "vitest";
import { unreadableRows, withUnreadable } from "../encryption/unreadable.js";
import { bindApp } from "../typed-app.js";
import type { JazzClient } from "./client.js";
import { Db, Transaction } from "./db.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import { markThinDb, setLocalFirstUpgrader } from "./local-first-upgrade.js";

const app = bindApp<{
  projects: {
    row: { id: string; title: string };
    insert: { title: string };
    columns: { title: string };
    relations: {};
  };
}>({
  source: `sha256:${"0".repeat(64)}`,
  schema: {
    current: "v1",
    schemas: [
      {
        hash: "v1",
        schema: {
          tables: {
            projects: {
              columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
            },
          },
        },
      },
    ],
    migrations: [],
    encryption: { versions: { v1: { projects: { scope: "projects", space: "id", columns: {} } } } },
  },
});

it("keeps unreadable rows when a transaction maps its query result", async () => {
  const omission = { table: "projects", id: "missing", column: "title", code: "key-pending" };
  const queryInternal = vi.fn(async () =>
    withUnreadable([{ id: "visible", values: [{ type: "Text", value: "Visible" }] }], [omission]),
  );
  const client = { beginTransaction: () => "tx", queryInternal } as unknown as JazzClient;
  const tx = new Transaction("mergeable", () => client);

  const rows = await tx.all(app.projects);
  expect(rows).toEqual([{ id: "visible", title: "Visible" }]);
  expect(unreadableRows(rows)).toEqual([omission]);
});

it("waits for local-first membership reads from a thin app with no encrypted columns", async () => {
  const thin = Db.create(
    { appId: "first-encryption-migration", encryption: { app } },
    new DefaultRuntimeSource(),
  );
  markThinDb(thin);
  const space = { table: "projects", id: "project" };
  const row = vi.fn(async () => null);
  const next = Object.create(Db.prototype, {
    encryption: {
      value: { ready: async () => "active", members: { row } },
    },
  }) as Db;
  const loader = async () => ({ createClient: async () => next });
  const upgrade = vi.fn(async () => {
    vi.spyOn(thin, "current").mockReturnValue(next);
  });
  setLocalFirstUpgrader(thin, upgrade, loader);

  try {
    await expect(thin.encryption.members.row(space, "account")).resolves.toBeNull();
    expect(upgrade).toHaveBeenCalledExactlyOnceWith(loader);
    expect(row).toHaveBeenCalledWith(space, "account");
  } finally {
    vi.restoreAllMocks();
    await thin.shutdown();
  }
});
