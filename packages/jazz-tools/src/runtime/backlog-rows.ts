/** A subscription backlog limit: a whole number of rows, at least 1 (it cannot be turned off). */
export function assertBacklogRows(rows: unknown, name: string): asserts rows is number {
  if (typeof rows !== "number" || !Number.isSafeInteger(rows) || rows < 1) {
    throw new Error(
      `${name} must be a whole number of rows, at least 1: a subscription's backlog cannot be unbounded.`,
    );
  }
}
