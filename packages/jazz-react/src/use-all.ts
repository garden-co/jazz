import { useMemo, useSyncExternalStore } from "react";
import type { QueryBuilder, PersistenceTier } from "jazz-ts";
import { useDb } from "./provider.js";

export function useAll<T extends { id: string }>(query: QueryBuilder<T>): T[];
export function useAll<T extends { id: string }>(
  query: QueryBuilder<T>,
  tier: PersistenceTier,
): T[] | undefined;
export function useAll<T extends { id: string }>(
  query: QueryBuilder<T>,
  tier?: PersistenceTier,
): T[] | undefined {
  const db = useDb();
  const queryKey = query._build();

  const { subscribe, getSnapshot } = useMemo(() => {
    let snapshot: T[] | undefined = tier ? undefined : [];
    return {
      subscribe: (onStoreChange: () => void) => {
        return db.subscribeAll(
          query,
          (delta) => {
            snapshot = delta.all;
            onStoreChange();
          },
          tier,
        );
      },
      getSnapshot: () => snapshot,
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [db, queryKey, tier]);

  return useSyncExternalStore(subscribe, getSnapshot);
}
