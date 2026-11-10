/** Direct local-content limits, not a native scan or allocation budget. */
export const retainedEqualityLimits = Object.freeze({
  rows: 256,
  decryptedCells: 1024,
  cellBytes: 1024 * 1024,
  totalBytes: 4 * 1024 * 1024,
  valueNodes: 65536,
  valueDepth: 32,
});

function overflow(): never {
  throw new Error(
    "Unsupported retained encrypted equality query: local candidate row, cell or byte limit exceeded; narrow the plaintext scope/predicates",
  );
}

/** Check the whole captured set, including nonmatches and rows beyond a page. */
export function checkRetainedEqualityCandidates(
  rows: readonly unknown[],
  encryptedColumns: number,
): void {
  if (
    rows.length > retainedEqualityLimits.rows ||
    rows.length * encryptedColumns > retainedEqualityLimits.decryptedCells
  )
    overflow();
  let total = 0;
  let nodes = 0;
  const active = new Set<object>();
  function bytes(value: unknown, depth: number): number {
    if (++nodes > retainedEqualityLimits.valueNodes || depth > retainedEqualityLimits.valueDepth)
      overflow();
    let count: number;
    if (value == null) count = 1;
    else if (typeof value === "string") {
      if (value.length > retainedEqualityLimits.cellBytes) overflow();
      count = new TextEncoder().encode(value).byteLength;
    } else if (typeof value === "bigint") count = value.toString().length;
    else if (typeof value === "number" || typeof value === "boolean") count = 8;
    else if (ArrayBuffer.isView(value)) count = value.byteLength;
    else if (value instanceof ArrayBuffer) count = value.byteLength;
    else if (typeof value === "object") {
      if (active.has(value)) overflow();
      active.add(value);
      count = 0;
      for (const [key, item] of Object.entries(value)) {
        count += key.length * 2 + bytes(item, depth + 1);
        if (count > retainedEqualityLimits.cellBytes) overflow();
      }
      active.delete(value);
    } else overflow();
    if (count > retainedEqualityLimits.cellBytes) overflow();
    return count;
  }
  for (const row of rows) {
    // A row has multiple cells; the per-cell ceiling applies to each value.
    if (!row || typeof row !== "object" || Array.isArray(row)) overflow();
    for (const [key, value] of Object.entries(row)) {
      total += key.length * 2 + bytes(value, 0);
      if (total > retainedEqualityLimits.totalBytes) overflow();
    }
  }
}
