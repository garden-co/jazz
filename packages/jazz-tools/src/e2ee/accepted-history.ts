import { exclusiveE2eeTransaction } from "../runtime/db.js";
import type { Db, E2eeTransactionScope, QueryBuilder } from "../runtime/db.js";
import type { RowSettlement } from "../runtime/client.js";
import type { SpaceRoot, SpaceGrant } from "./spaces.js";
import { sameSnapshotValue } from "./public-snapshot.js";
import type { AccountStore } from "../accounts/persistence.js";
import { decodeLocalDeviceStore } from "./local-device.js";

type Read = {
  query: QueryBuilder<{ id: string }>;
  snapshot: { rows: { id: string }[]; settlements: RowSettlement[] };
};
type Retained = {
  reads: Read[];
  result: unknown;
  initial?: { rootId: string; rowIds: string[]; recipientIds: string[]; transactionId?: string };
};
const histories = new WeakMap<Db, Map<string, Retained>>();
type Storage = { store: AccountStore; scope: string; assertOpen(): void };
type StoredRead = { query: string; snapshot: Read["snapshot"] };
type StoredHistory = { scope: string; key: string; reads: string; initial?: Retained["initial"] };
const stores = new WeakMap<Db, Storage>();

/** Internal, account/application-scoped storage; never installs an authority receipt. */
export function configureAcceptedHistory(db: Db, storage: Storage): void {
  stores.set(db, storage);
}

function storedHistories(value: string | null) {
  const state = decodeLocalDeviceStore(value) as ReturnType<typeof decodeLocalDeviceStore> & {
    acceptedHistoryV1?: StoredHistory[];
  };
  const entries = state.acceptedHistoryV1 ?? [];
  const seen = new Set<string>();
  if (!Array.isArray(entries)) throw new Error("Invalid stored E2EE histories");
  for (const entry of entries) {
    if (
      !entry ||
      typeof entry.scope !== "string" ||
      typeof entry.key !== "string" ||
      typeof entry.reads !== "string"
    )
      throw new Error("Invalid stored E2EE history");
    if (
      entry.initial &&
      (typeof entry.initial.rootId !== "string" ||
        typeof entry.initial.transactionId !== "string" ||
        !entry.initial.transactionId ||
        !Array.isArray(entry.initial.rowIds) ||
        !Array.isArray(entry.initial.recipientIds) ||
        entry.initial.recipientIds.some((id) => typeof id !== "string") ||
        new Set(entry.initial.recipientIds).size !== entry.initial.recipientIds.length ||
        entry.initial.rowIds.length === 0 ||
        entry.initial.rowIds.some((id) => typeof id !== "string") ||
        !entry.initial.rowIds.includes(entry.initial.rootId) ||
        new Set(entry.initial.rowIds).size !== entry.initial.rowIds.length)
    )
      throw new Error("Invalid stored initial E2EE history");
    const key = JSON.stringify([entry.scope, entry.key]);
    if (seen.has(key)) throw new Error("Duplicate stored E2EE history");
    seen.add(key);
  }
  return { state, entries };
}

// Local JSON v1: snapshots contain public metadata only. BYTEA is the single-key
// object { e2eeBytesV1: [0..255] }; this tag is reserved in these package-owned rows.
export function encodeAcceptedHistoryReads(reads: StoredRead[]): string {
  return JSON.stringify(reads, (_key, value) =>
    value instanceof Uint8Array ? { e2eeBytesV1: Array.from(value) } : value,
  );
}

export function decodeAcceptedHistoryReads(value: string): StoredRead[] {
  const reads = JSON.parse(value, (_key, item) => {
    if (item && typeof item === "object" && Object.hasOwn(item, "e2eeBytesV1")) {
      if (
        Object.keys(item).length !== 1 ||
        !Array.isArray(item.e2eeBytesV1) ||
        item.e2eeBytesV1.some(
          (byte: unknown) =>
            typeof byte !== "number" || !Number.isInteger(byte) || byte < 0 || byte > 255,
        )
      )
        throw new Error("Invalid stored E2EE bytes");
      return Uint8Array.from(item.e2eeBytesV1);
    }
    return item;
  }) as StoredRead[];
  if (!Array.isArray(reads) || reads.length === 0) throw new Error("Invalid stored E2EE reads");
  for (const read of reads) {
    const snapshot = read?.snapshot;
    if (
      typeof read?.query !== "string" ||
      !Array.isArray(snapshot?.rows) ||
      !Array.isArray(snapshot?.settlements) ||
      snapshot.rows.some((row) => !row || typeof row.id !== "string") ||
      snapshot.settlements.some(
        (row) =>
          !row ||
          typeof row.rowId !== "string" ||
          typeof row.transactionId !== "string" ||
          typeof row.position !== "string" ||
          row.position.length > 20 ||
          !/^(0|[1-9][0-9]*)$/.test(row.position) ||
          BigInt(row.position) > 0xffffffffffffffffn,
      ) ||
      !sameMetadata(snapshot, snapshot)
    )
      throw new Error("Invalid stored E2EE snapshot");
  }
  return reads;
}

async function persist(
  db: Db,
  key: string,
  reads: Read[],
  initial?: Retained["initial"],
): Promise<void> {
  const storage = stores.get(db);
  if (!storage) return;
  const encoded = encodeAcceptedHistoryReads(
    reads.map(({ query, snapshot }) => ({
      query: query._build(),
      snapshot,
    })),
  );
  decodeAcceptedHistoryReads(encoded);
  const existing = storedHistories(await storage.store.read()).entries.find(
    (entry) => entry.scope === storage.scope && entry.key === key,
  );
  storage.assertOpen();
  if (existing?.reads === encoded && sameSnapshotValue(existing.initial, initial)) return;
  let updated = false;
  await storage.store.update((value) => {
    storage.assertOpen();
    const { state, entries } = storedHistories(value);
    state.acceptedHistoryV1 = [
      ...entries.filter((entry) => entry.scope !== storage.scope || entry.key !== key),
      { scope: storage.scope, key, reads: encoded, ...(initial ? { initial } : {}) },
    ];
    updated = true;
    return JSON.stringify(state);
  });
  if (!updated) throw new Error("E2EE history store did not perform the update");
  storage.assertOpen();
}

async function restore<T>(
  db: Db,
  key: string,
  read: (tx: E2eeTransactionScope, initialRecipients?: string[]) => Promise<T>,
) {
  const storage = stores.get(db);
  if (!storage) return undefined;
  const { entries } = storedHistories(await storage.store.read());
  storage.assertOpen();
  const entry = entries.find((item) => item.scope === storage.scope && item.key === key);
  if (!entry) return undefined;
  const pending = decodeAcceptedHistoryReads(entry.reads);
  // Re-run the existing membership verifier; never persist its derived verdict
  // or a decrypted key. Query matching also rejects a changed dependency shape.
  const bundle = await capture(
    {
      async allSettledForE2ee<T extends { id: string }>(query: QueryBuilder<T>) {
        const index = pending.findIndex((item) => item.query === query._build());
        if (index < 0) throw new Error("Stored E2EE history lacks a required read");
        return pending.splice(index, 1)[0]!.snapshot as { rows: T[]; settlements: RowSettlement[] };
      },
    },
    (tx) => read(tx, entry.initial?.recipientIds),
  );
  if (pending.length) throw new Error("Stored E2EE history has unused reads");
  storage.assertOpen();
  return { ...bundle, ...(entry.initial ? { initial: entry.initial } : {}) };
}

function sameMetadata(left: Read["snapshot"], right: Read["snapshot"]): boolean {
  const leftRows = new Map(left.rows.map((row) => [row.id, row]));
  const rightRows = new Map(right.rows.map((row) => [row.id, row]));
  const leftSettlements = new Map(left.settlements.map((row) => [row.rowId, row]));
  const rightSettlements = new Map(right.settlements.map((row) => [row.rowId, row]));
  return (
    leftRows.size === left.rows.length &&
    rightRows.size === right.rows.length &&
    leftSettlements.size === left.settlements.length &&
    rightSettlements.size === right.settlements.length &&
    sameSnapshotValue(leftRows, rightRows) &&
    sameSnapshotValue(leftSettlements, rightSettlements)
  );
}

function retain(db: Db, key: string, bundle: Retained): void {
  const entries = histories.get(db) ?? new Map<string, Retained>();
  // Clone together so result snapshots and recorded reads keep their shared identity.
  const copied = structuredClone({
    snapshots: bundle.reads.map((r) => r.snapshot),
    result: bundle.result,
  });
  entries.delete(key);
  entries.set(key, {
    ...bundle,
    reads: bundle.reads.map((r, i) => ({ query: r.query, snapshot: copied.snapshots[i]! })),
    result: copied.result,
  });
  // Keep provisional preparation until acceptance can save it; ordinary bundles
  // may be evicted because accepted reads have already been persisted.
  if (entries.size > 8) {
    const oldest = [...entries].find(([, entry]) => !entry.initial);
    if (oldest) entries.delete(oldest[0]);
  }
  histories.set(db, entries);
}

/** Capture a coherent preparation snapshot; it is not accepted merely by being retained. */
export async function prepareInitialHistory<
  T extends { roots: Read["snapshot"]; grants: Read["snapshot"] },
>(
  db: Db,
  tx: E2eeTransactionScope,
  read: (tx: E2eeTransactionScope) => Promise<T>,
  root: SpaceRoot,
  grants: SpaceGrant[],
): Promise<void> {
  const bundle = await capture(tx, read);
  if (bundle.result.roots.rows.length || bundle.result.grants.rows.length)
    throw new Error("Initial E2EE history must precede its root and grants");
  bundle.result.roots.rows.push(root);
  bundle.result.grants.rows.push(...grants);
  retain(db, JSON.stringify(["space", root.scopeId, root.identifier]), {
    ...bundle,
    initial: {
      rootId: root.id,
      rowIds: [root.id, ...grants.map((row) => row.id)],
      recipientIds: grants.map((row) => row.recipientId),
    },
  });
}

/** Discard an uncommitted preparation after rollback or rejection. */
export function discardInitialHistory(db: Db, root: SpaceRoot): void {
  const key = JSON.stringify(["space", root.scopeId, root.identifier]);
  const entry = histories.get(db)?.get(key);
  if (entry?.initial?.rootId === root.id && !entry.initial.transactionId)
    histories.get(db)!.delete(key);
}

/** Save the exact accepted preparation without changing the committed receipt on cache failure. */
export async function acceptInitialHistory(
  db: Db,
  root: SpaceRoot,
  transactionId: string,
): Promise<void> {
  const key = JSON.stringify(["space", root.scopeId, root.identifier]);
  const retained = histories.get(db)?.get(key);
  if (retained?.initial?.rootId !== root.id) return;
  retained.initial.transactionId = transactionId;
  try {
    await persist(db, key, retained.reads, retained.initial);
    if (stores.has(db) && histories.get(db)?.get(key) === retained) histories.get(db)!.delete(key);
  } catch {
    // The transaction is already accepted. Keep the in-memory history, but never
    // report that committed write as rejected because an optional cache failed.
  }
}

/** Retain complete accepted read bundles, never individual rows or unaccepted results. */
export async function readAcceptedHistory<T>(
  db: Db,
  key: string,
  read: (tx: E2eeTransactionScope, initialRecipients?: string[]) => Promise<T>,
  localOnly = false,
): Promise<T> {
  const entries = histories.get(db) ?? new Map<string, Retained>();
  histories.set(db, entries);
  if (localOnly || (await db.e2eeIsExplicitlyOffline())) {
    const retained: Retained | undefined = entries.get(key) ?? (await restore(db, key, read));
    if (!retained) throw new Error("Accepted E2EE history is unavailable offline");
    if (retained.initial && !retained.initial.transactionId)
      throw new Error("Initial E2EE history has not been accepted");
    const observed = await db.observeE2eeHistory(retained.reads.map(({ query }) => query));
    if (retained.initial) {
      const initial = retained.initial;
      const own = new Set(initial.rowIds);
      const additions = observed
        .flatMap((snapshot) => snapshot.settlements)
        .filter((row) => own.has(row.rowId));
      if (
        additions.length !== own.size ||
        new Set(additions.map((row) => row.rowId)).size !== own.size ||
        additions.some((row) => row.transactionId !== initial.transactionId)
      )
        throw new Error("Initial E2EE history lacks its accepted transaction");
      // Update only the settlement of our exact accepted writes. Other rows and
      // absences must still match the coherent preparation snapshot below.
      for (let i = 0; i < retained.reads.length; i++)
        retained.reads[i]!.snapshot.settlements.push(
          ...observed[i]!.settlements.filter((row) => own.has(row.rowId)),
        );
      delete retained.initial;
    }
    if (
      observed.some((snapshot, index) => !sameMetadata(snapshot, retained.reads[index]!.snapshot))
    ) {
      entries.delete(key);
      throw new Error("Accepted E2EE history changed; authority reconciliation is required");
    }
    if (!entries.has(key)) retain(db, key, retained);
    return structuredClone(retained.result) as T;
  }
  const transaction = await exclusiveE2eeTransaction(db, (tx) => capture(tx, read));
  const bundle = await transaction.wait({ tier: "global" });
  try {
    await persist(db, key, bundle.reads);
  } catch {
    // History is already verified. Optional persistence must not fail this read;
    // offline reuse still checks the retained bundle against accepted history.
  }
  retain(db, key, bundle);
  return bundle.result;
}

async function capture<T>(
  tx: Pick<E2eeTransactionScope, "allSettledForE2ee">,
  read: (tx: E2eeTransactionScope) => Promise<T>,
) {
  const reads: Read[] = [];
  const scope: E2eeTransactionScope = {
    kind: "exclusive",
    insert() {
      throw new Error("Accepted E2EE history reads cannot write");
    },
    upsert() {
      throw new Error("Accepted E2EE history reads cannot write");
    },
    async all<T>(query: QueryBuilder<T>) {
      const snapshot = await scope.allSettledForE2ee(query as QueryBuilder<T & { id: string }>);
      return snapshot.rows;
    },
    async one(query) {
      const rows = await scope.all(query);
      return rows[0] ?? null;
    },
    async allSettledForE2ee(query) {
      if (!query._table.startsWith("__e2ee_")) throw new Error("Expected E2EE metadata query");
      const snapshot = await tx.allSettledForE2ee(query);
      reads.push({ query, snapshot });
      return snapshot;
    },
  };
  return { reads, result: await read(scope) };
}
