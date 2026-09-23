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

/** How a binding reports a failure that resident local state already decides. */
export type ResidentFailureSurface = "sync" | "handle";

/**
 * Resident tombstone: update, upsert and delete of a row whose delete has
 * already been applied locally. The decided RN contract (#3273) throws these
 * synchronously from the call. NAPI and WASM, observed in CI on this PR,
 * report the same failure through the write handle instead; each binding's
 * test pins its own surface with the same calls, so that divergence stays
 * explicit.
 */
export async function assertResidentTombstoneRejects(
  db: Db,
  surface: ResidentFailureSurface,
): Promise<void> {
  const row = await db.insert(notes, { text: "doomed", seq: 0 }).wait({ tier: "local" });
  await db.delete(notes, row.id).wait({ tier: "local" });
  const reason = `row already deleted: ${row.id}`;
  const writes = [
    () => db.update(notes, row.id, { text: "revived" }),
    () => db.upsert(notes, row.id, { text: "revived", seq: 1 }),
    () => db.delete(notes, row.id),
  ];
  for (const write of writes) {
    if (surface === "sync") {
      expect(write).toThrow(reason);
    } else {
      let handle: ReturnType<typeof write> | undefined;
      expect(() => {
        handle = write();
      }).not.toThrow();
      await expect(handle!.wait({ tier: "local" })).rejects.toMatchObject({ reason });
    }
  }
  expect(await db.all(notes)).toEqual([]);
}

/**
 * Handle class: a failure that admission never decides from resident state
 * (here: re-inserting a deleted row's caller-supplied id, which only the
 * apply step checks) is reported by the write handle and the mutation-error
 * listener, not thrown by the call. This matches the NAPI receipt in
 * tests/ts-dsl/insert-api.test.ts.
 */
export async function assertApplyFailureSurfacesThroughHandle(db: Db): Promise<void> {
  const id = crypto.randomUUID();
  await db.insert(notes, { text: "reserved", seq: 0 }, { id }).wait({ tier: "local" });
  await db.delete(notes, id).wait({ tier: "local" });
  const errors: unknown[] = [];
  const stop = db.onMutationError((event) => errors.push(event));
  try {
    let handle: ReturnType<typeof db.insert> | undefined;
    expect(() => {
      handle = db.insert(notes, { text: "reused id", seq: 1 }, { id });
    }).not.toThrow();
    await expect(handle!.wait({ tier: "local" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      code: "write_rejected",
      reason: `row already deleted: ${id}`,
    });
    // Without an active wait(), the same apply-time rejection reaches the
    // mutation-error listener instead.
    expect(() => db.insert(notes, { text: "unawaited", seq: 2 }, { id })).not.toThrow();
    const deadline = Date.now() + 10_000;
    while (errors.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(errors).toHaveLength(1);
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
  // Await in call order: concurrent waits are themselves bounded pending
  // operations on native bindings, independent of the write queue.
  const written = [];
  for (const handle of handles) written.push(await handle.wait({ tier: "local" }));
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
