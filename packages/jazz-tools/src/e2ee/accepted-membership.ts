import {
  beginDbTransactionAfter,
  readAcceptedTransactionRows,
  type Db,
  type QueryBuilder,
  type TransactionScope,
} from "../runtime/db.js";
import type { AcceptedRows } from "../runtime/accepted-read.js";

/** Account metadata uses the ordinary, explicitly opted-in authority batch. */
export type AcceptedDeviceTransactionScope = Pick<
  TransactionScope<"serializable">,
  "kind" | "all" | "one" | "insert" | "upsert"
> & {
  readAccepted<T>(query: QueryBuilder<T>): Promise<AcceptedRows<T>>;
};

/** Internal lifecycle work must not wait for the enrolment it implements. */
export async function acceptedDeviceTransaction<T>(
  db: Db,
  callback: (tx: AcceptedDeviceTransactionScope) => T | Promise<T>,
) {
  const transaction = beginDbTransactionAfter(db, async () => {}, "accepted-evidence");
  try {
    const value = await callback({
      kind: transaction.kind,
      all: transaction.all.bind(transaction),
      one: transaction.one.bind(transaction),
      insert: transaction.insert.bind(transaction),
      upsert: transaction.upsert.bind(transaction),
      readAccepted: (query) => readAcceptedTransactionRows(transaction, query, 15_000),
    });
    const committed = await transaction.commit();
    return {
      // Serializable confirmation always waits for the authority's Global fate.
      async wait(options: { tier?: "global"; timeoutMs?: number } = {}) {
        await committed.wait(options);
        return value;
      },
    };
  } catch (error) {
    try {
      await transaction.rollback();
    } catch {
      // Preserve the operation's first error, including unavailable evidence.
    }
    throw error;
  }
}

/** Signed immutable content keeps its original authority order after restore. */
export function acceptedContentPositions<T extends { id: string }>(snapshot: AcceptedRows<T>) {
  const rows = new Set(snapshot.rows.map((row) => row.id));
  if (rows.size !== snapshot.rows.length || snapshot.evidence.length !== rows.size)
    throw new Error("Incomplete E2EE authority coverage");
  const through = coordinate(snapshot.through);
  const positions = new Map<string, bigint>();
  for (const entry of snapshot.evidence) {
    const state = coordinate(entry.state.position);
    const content = coordinate(entry.content.position);
    if (
      !rows.delete(entry.rowId) ||
      !entry.state.transactionId ||
      !entry.content.transactionId ||
      content > state ||
      state > through
    )
      throw new Error("Invalid E2EE authority coverage");
    positions.set(entry.rowId, content);
  }
  return positions;
}

function coordinate(value: string): bigint {
  if (typeof value !== "string" || !/^[0-9]+$/.test(value))
    throw new Error("Invalid E2EE authority coordinate");
  const position = BigInt(value);
  if (position > 18_446_744_073_709_551_615n) throw new Error("Invalid E2EE authority coordinate");
  return position;
}
