import type { RowSettlement } from "./client.js";

/** Check one authority content receipt per result occurrence. */
export function checkTransactionSettlements(
  rows: { id: string }[],
  settlements: RowSettlement[],
  union = false,
): void {
  const remaining = new Map<string, number>();
  for (const row of rows) remaining.set(row.id, (remaining.get(row.id) ?? 0) + 1);
  if (settlements.length !== rows.length || (!union && remaining.size !== rows.length))
    throw new Error("Incomplete E2EE authority settlement metadata");
  const content = new Map<string, RowSettlement>();
  for (const entry of settlements) {
    const count = remaining.get(entry.rowId) ?? 0;
    const previous = content.get(entry.rowId);
    if (
      count === 0 ||
      typeof entry.transactionId !== "string" ||
      typeof entry.position !== "string" ||
      !/^[0-9]+$/.test(entry.position) ||
      BigInt(entry.position) > 18446744073709551615n ||
      (previous !== undefined &&
        (previous.transactionId !== entry.transactionId || previous.position !== entry.position))
    )
      throw new Error("Incomplete E2EE authority settlement metadata");
    remaining.set(entry.rowId, count - 1);
    content.set(entry.rowId, entry);
  }
}
