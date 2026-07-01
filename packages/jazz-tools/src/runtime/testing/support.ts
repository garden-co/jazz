import type { Db, QueryBuilder } from "../db.js";

export async function waitForRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
): Promise<T[]> {
  const timeoutMs = 10_000;
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(query, { tier: "edge" });
      if (predicate(rows)) return rows;
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `Timed out waiting for rows; lastRows=${JSON.stringify(lastRows)}, lastError=${lastErrorMessage}`,
  );
}
