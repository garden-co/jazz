import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createPolicyTestApp, testApp, type PolicyTestApp } from "../testing/index.js";
import type { TestDb } from "../testing/policy-test-app.js";

// What fails, and when: a transform's `to` on a write, its `from` on a read,
// and a computed field's callback on a read.
const failing = { to: false, from: false, computed: false };
const fail = (what: string): never => {
  throw new Error(`${what} failed`);
};

const app = testApp(
  {
    tables: {
      notes: {
        columns: { title: { type: "string", transform: "lowercase" } },
        computed: { size: { type: "int", from: ["title"] } },
      },
    },
    transforms: { lowercase: { stored: "string", view: "string" } },
  },
  {
    transforms: {
      lowercase: {
        from: (value: string) => (failing.from ? fail("from") : value.toUpperCase()),
        to: (value: string) => (failing.to ? fail("to") : value.toLowerCase()),
      },
    },
    computed: {
      notes: {
        size: (row: { title: string }) => (failing.computed ? fail("computed") : row.title.length),
      },
    },
  },
);

const session = {
  issuer: "https://issuer.example",
  user_id: "transform-failures",
  claims: {},
  authMode: "external" as const,
};

/** The subscription's first callback: rows, or the error it reports. */
function firstDelivery(db: TestDb, query = app.notes): Promise<unknown> {
  return new Promise((resolve) => {
    const unsubscribe = db.subscribe(query, {
      onUpdate: (rows) => {
        queueMicrotask(unsubscribe);
        resolve(rows);
      },
      onError: (error) => {
        queueMicrotask(unsubscribe);
        resolve(error);
      },
    });
  });
}

describe("column transform and computed field failures", () => {
  let policyApp: PolicyTestApp;
  let db: TestDb;
  let id: string;

  beforeEach(async () => {
    Object.assign(failing, { to: false, from: false, computed: false });
    policyApp = await createPolicyTestApp(
      app,
      { tables: { notes: { read: "true", insert: "true", update: "true" } } },
      expect,
    );
    db = policyApp.as(session);
    ({ id } = await db.insert(app.notes, { title: "Hello" }).wait({ tier: "global" }));
  });

  afterEach(async () => {
    Object.assign(failing, { to: false, from: false, computed: false });
    await policyApp.shutdown();
  });

  it("throws a failing `to` from the insert or update, before anything is written", async () => {
    failing.to = true;
    expect(() => db.insert(app.notes, { title: "Other" })).toThrow("to failed");
    expect(() => db.update(app.notes, id, { title: "Changed" })).toThrow("to failed");

    failing.to = false;
    await expect(db.all(app.notes)).resolves.toEqual([{ id, title: "HELLO", size: 5 }]);
  });

  it("rejects a read, and errors a subscription, when `from` fails", async () => {
    failing.from = true;
    await expect(db.all(app.notes)).rejects.toThrow("from failed");
    await expect(db.one(app.notes.where({ id }))).rejects.toThrow("from failed");
    await expect(firstDelivery(db)).resolves.toEqual(
      expect.objectContaining({ message: expect.stringContaining("from failed") }),
    );
  });

  it("rejects a read of a failing computed field, but not a read that leaves it out", async () => {
    failing.computed = true;
    await expect(db.all(app.notes)).rejects.toThrow("computed failed");
    await expect(firstDelivery(db)).resolves.toEqual(
      expect.objectContaining({ message: expect.stringContaining("computed failed") }),
    );
    await expect(db.all(app.notes.select("title"))).resolves.toEqual([{ id, title: "HELLO" }]);
  });
});
