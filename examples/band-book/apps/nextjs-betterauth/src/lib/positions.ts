/** Gap between blocks appended at the end of a page or list. */
export const POSITION_STEP = 1024;

/**
 * A position strictly between two neighbours. Missing neighbours mean the
 * start or the end of the list. Floats leave room for about fifty inserts at
 * the same spot before two blocks would compare equal; ties still render in a
 * stable order because blocks sort by `$createdAt` second.
 */
export function positionBetween(before: number | undefined, after: number | undefined): number {
  if (before === undefined && after === undefined) return POSITION_STEP;
  if (before === undefined) return (after as number) - POSITION_STEP;
  if (after === undefined) return before + POSITION_STEP;
  return before + (after - before) / 2;
}

type Positioned = { id: string; position: number; $createdAt?: Date | number | null };

function createdAtMs(row: Positioned): number {
  const value = row.$createdAt;
  if (value instanceof Date) return value.getTime();
  return typeof value === "number" ? value : 0;
}

/** Order siblings by position, then by creation time, then by id. */
export function comparePositioned(a: Positioned, b: Positioned): number {
  return a.position - b.position || createdAtMs(a) - createdAtMs(b) || a.id.localeCompare(b.id);
}
