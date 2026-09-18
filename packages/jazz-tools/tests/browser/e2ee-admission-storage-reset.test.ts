import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { beginDbTransactionAfter } from "../../src/runtime/db.js";
import { acquireBrowserTestAccount, createBrowserTestDb } from "./account-fixtures.js";

it("rejects suspended admission after browser storage reset without crossing account storage", async () => {
  const appId = `e2ee-admission-reset-${crypto.randomUUID()}`;
  const app = s.defineApp({ documents: s.table({ title: s.string() }, {}) });
  const account = await acquireBrowserTestAccount({ appId, key: "reset" });
  const otherAccount = await acquireBrowserTestAccount({ appId, key: "untouched" });
  const db = await createBrowserTestDb({ appId, account });
  const other = await createBrowserTestDb({ appId, account: otherAccount });
  let reopened: Awaited<ReturnType<typeof createBrowserTestDb>> | undefined;
  let release!: () => void;
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  try {
    await db.insert(app.documents, { title: "Before reset" }).wait({ tier: "local" });
    const untouched = await other
      .insert(app.documents, { title: "Other account" })
      .wait({ tier: "local" });
    const tx = beginDbTransactionAfter(db, async () => {
      entered();
      await ready;
    });
    tx.insert(app.documents, { title: "Must never appear" });
    const reading = tx.all(app.documents, { tier: "local" });
    reading.catch(() => {});
    const wait = tx.commit().wait({ tier: "local" });
    wait.catch(() => {});
    await started;
    await db.deleteClientStorage();
    release();
    await expect(wait).rejects.toThrow();
    await expect(reading).rejects.toThrow();
    reopened = await createBrowserTestDb({ appId, account });
    expect(await reopened.all(app.documents, { tier: "local" })).toEqual([]);
    const replacement = await reopened
      .insert(app.documents, { title: "After reset" })
      .wait({ tier: "local" });
    expect(await reopened.all(app.documents, { tier: "local" })).toEqual([replacement]);
    expect(await other.all(app.documents, { tier: "local" })).toEqual([untouched]);
  } finally {
    release();
    await Promise.all([db.shutdown(), other.shutdown(), reopened?.shutdown()]);
  }
}, 30_000);
