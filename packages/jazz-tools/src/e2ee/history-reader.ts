import type { Db, E2eeTransactionScope, QueryBuilder } from "../runtime/db.js";
import type { RowSettlement } from "../runtime/client.js";

/** Read-only replay seam shared by authority transactions and local observations. */
export type E2eeHistoryReader = Pick<E2eeTransactionScope, "allSettledForE2ee">;

export class E2eeHistoryUnavailable extends Error {}

type Observation = { rows: { id: string }[]; settlements: RowSettlement[] };

/**
 * Discover dependent queries, then reobserve the entire dependency set together.
 * The callback must be read-only: only its final, coherent replay is returned.
 * Observations prove retained acceptance, never current global eligibility.
 */
export async function observeE2eeHistory<T>(
  db: Db,
  read: (reader: E2eeHistoryReader) => Promise<T>,
): Promise<T> {
  const queries = new Map<string, QueryBuilder<{ id: string }>>();
  let observations = new Map<string, Observation>();
  const missing = Symbol("E2EE history dependency");
  const reader: E2eeHistoryReader = {
    async allSettledForE2ee<T extends { id: string }>(query: QueryBuilder<T>) {
      const key = query._build();
      if (!queries.has(key)) queries.set(key, query);
      const observation = observations.get(key);
      if (!observation) throw missing;
      return observation as { rows: T[]; settlements: RowSettlement[] };
    },
  };
  for (;;) {
    try {
      return await read(reader);
    } catch (error) {
      if (error !== missing) throw error;
    }
    const batch = [...queries];
    const results = await db.observeE2eeHistory(batch.map(([, query]) => query));
    observations = new Map(batch.map(([key], index) => [key, results[index]!]));
  }
}
