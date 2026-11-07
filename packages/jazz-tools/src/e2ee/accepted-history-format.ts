import { sameSnapshotValue } from "./public-snapshot.js";

/** Legacy local metadata representation only, never an authority receipt. */
export type StoredAcceptedHistoryRead = {
  query: string;
  snapshot: {
    rows: { id: string }[];
    settlements: { rowId: string; transactionId: string; position: string }[];
  };
};

// Local JSON v1: snapshots contain public metadata only. BYTEA is the single-key
// object { e2eeBytesV1: [0..255] }; this tag is reserved in these package-owned rows.
export function encodeAcceptedHistoryReads(reads: StoredAcceptedHistoryRead[]): string {
  return JSON.stringify(reads, (_key, value) =>
    value instanceof Uint8Array ? { e2eeBytesV1: Array.from(value) } : value,
  );
}

export function decodeAcceptedHistoryReads(value: string): StoredAcceptedHistoryRead[] {
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
  }) as StoredAcceptedHistoryRead[];
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
      !sameAcceptedHistoryMetadata(snapshot, snapshot)
    )
      throw new Error("Invalid stored E2EE snapshot");
  }
  return reads;
}

export function sameAcceptedHistoryMetadata(
  left: StoredAcceptedHistoryRead["snapshot"],
  right: StoredAcceptedHistoryRead["snapshot"],
): boolean {
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
