/**
 * The rows a read left out because this device could not decrypt them
 * (protocol §4.3), kept beside the result the read returned.
 */

/** A row a read left out because this device could not decrypt it. */
export interface UnreadableRow {
  /** The table the row is of. */
  table: string;
  /** The row's id. */
  id: string;
  /** The first encrypted column that could not be opened. */
  column: string | null;
  /**
   * Why: `key-pending` (the key is being fetched; the row comes back once it
   * arrives), `key-unavailable` (this device holds no key for it),
   * `invalid-ciphertext`, `unsupported-format`, `empty-cell`, `stale-space`,
   * `space-pending`.
   */
  code: string;
}

// Bundlers can load more than one copy of jazz-tools in the same process.
const UNREADABLE = Symbol.for("jazz-tools.unreadableRows");
const NONE: readonly UnreadableRow[] = Object.freeze([]);

/** @internal Records that `rows`, a read's result, is missing `unreadable`. */
export function withUnreadable<T extends readonly unknown[]>(
  rows: T,
  unreadable: readonly UnreadableRow[] | undefined,
): T {
  if (unreadable && unreadable.length > 0) {
    Object.defineProperty(rows, UNREADABLE, {
      value: Object.freeze([...unreadable]),
      configurable: true,
    });
  }
  return rows;
}

/**
 * The rows a read left out because this device could not decrypt them: an
 * empty array when the result is complete.
 *
 * Pass the array `db.all()` or `tx.all()` returned (or a subscription's result). A read
 * with `limit` counts these rows toward its page, so a page shorter than its
 * limit is not the last one when this is not empty. For a query on an
 * encrypted column, some of them may be rows it would not have matched.
 */
export function unreadableRows(result: readonly unknown[]): readonly UnreadableRow[] {
  return (result as { [UNREADABLE]?: readonly UnreadableRow[] })[UNREADABLE] ?? NONE;
}
