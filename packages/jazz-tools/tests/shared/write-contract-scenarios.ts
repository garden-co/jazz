import { expect } from "vitest";
import { schema, type Db } from "../../src/index.js";

/**
 * Write error / backpressure / read-your-writes contract (#3273), asserted
 * through the public `Db` API with the same calls on every binding: React
 * Native (native relay), NAPI and WASM.
 */
export const writeContractApp = schema.defineApp({
  notes: schema.table({ text: schema.string(), seq: schema.int() }, {}),
});

const notes = writeContractApp.notes;

/**
 * Synchronous class: a failure that the resident local state already decides
 * is thrown by the write call itself, before any handle exists.
 */
export async function assertResidentTombstoneThrowsSynchronously(db: Db): Promise<void> {
  const row = await db.insert(notes, { text: "doomed", seq: 0 }).wait({ tier: "local" });
  await db.delete(notes, row.id).wait({ tier: "local" });

  expect(() => db.update(notes, row.id, { text: "revived" })).toThrow(
    `row already deleted: ${row.id}`,
  );
  expect(() => db.upsert(notes, row.id, { text: "revived", seq: 1 })).toThrow(
    `row already deleted: ${row.id}`,
  );
  expect(() => db.delete(notes, row.id)).toThrow(`row already deleted: ${row.id}`);
  expect(await db.all(notes)).toEqual([]);
}

/**
 * Handle class: a failure only discovered when the write is applied (here: a
 * delete admitted earlier in the same turn) is reported by the write handle
 * and the mutation-error listener, not thrown by the call.
 */
export async function assertApplyFailureSurfacesThroughHandle(db: Db): Promise<void> {
  const row = await db.insert(notes, { text: "raced", seq: 0 }).wait({ tier: "local" });
  const errors: unknown[] = [];
  const stop = db.onMutationError((event) => errors.push(event));
  try {
    db.delete(notes, row.id);
    let handle: ReturnType<typeof db.update> | undefined;
    expect(() => {
      handle = db.update(notes, row.id, { text: "too late" });
    }).not.toThrow();
    await expect(handle!.wait({ tier: "local" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      code: "write_rejected",
      reason: `row already deleted: ${row.id}`,
    });
    expect(await db.all(notes)).toEqual([]);
  } finally {
    stop();
  }
}

/**
 * Bounded queue: a burst far beyond any per-client admission cap is accepted
 * (with backpressure, never an unbounded queue) and loses, reorders or
 * duplicates nothing.
 */
export async function assertBurstBeyondCapLosesNothing(db: Db, burst = 192): Promise<void> {
  const handles = [];
  for (let seq = 0; seq < burst; seq += 1) {
    handles.push(db.insert(notes, { text: `c${seq}`, seq }));
  }
  const written = await Promise.all(handles.map((handle) => handle.wait({ tier: "local" })));
  expect(written.map((row) => row.seq)).toEqual([...Array(burst).keys()]);
  const rows = await db.all(notes.orderBy("seq", "asc"));
  expect(rows.map((row) => [row.seq, row.text])).toEqual(
    [...Array(burst).keys()].map((seq) => [seq, `c${seq}`]),
  );
  expect(new Set(rows.map((row) => row.id)).size).toBe(burst);
}

/**
 * Read-your-writes fence: a one-shot read and a subscription opened after a
 * write in the same JS turn both reflect it in their first result.
 */
export async function assertSameTurnReadAndSubscriptionSeeWrite(db: Db): Promise<void> {
  const { value: row } = db.insert(notes, { text: "same turn", seq: 7 });
  const snapshots: string[][] = [];
  let resolveFirst: () => void;
  const first = new Promise<void>((resolve) => {
    resolveFirst = resolve;
  });
  const unsubscribe = db.subscribe(notes, (rows) => {
    snapshots.push(rows.map((current) => current.id));
    resolveFirst();
  });
  const read = db.all(notes);
  try {
    expect((await read).map((current) => current.id)).toEqual([row.id]);
    await first;
    expect(snapshots[0]).toEqual([row.id]);
    for (const snapshot of snapshots) expect(snapshot).toEqual([row.id]);
  } finally {
    unsubscribe();
  }
}
