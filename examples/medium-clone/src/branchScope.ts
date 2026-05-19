import type { QueryBuilder } from "jazz-tools";

/**
 * Wrap a query so its reads come from the captured branch scope instead of
 * `main`. Works with `useAll`, `db.all`, etc. — these helpers only need a
 * `QueryBuilder<T>`, so a thin overlay on `_build()` is enough.
 *
 * `db.branch(branchId).all(query)` does the same thing under the hood, but
 * doesn't compose with `useAll`. This helper bridges that gap.
 */
export function withBranchScope<T>(query: QueryBuilder<T>, branchId: string): QueryBuilder<T> {
  return {
    ...query,
    _build() {
      const raw = JSON.parse(query._build()) as Record<string, unknown>;
      return JSON.stringify({
        ...raw,
        branchScope: { branchId },
      });
    },
  } as QueryBuilder<T>;
}
