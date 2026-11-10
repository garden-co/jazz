import { useMemo } from "react";
import { limitQueryToOne, type Db, type QueryBuilder, type QueryOptions } from "../runtime/db.js";
import { suspenseResult, useAll, useAllSuspense } from "./use-all.js";

export type UseOneResult<T extends { id: string }> =
  | UseOneLoadingResult
  | UseOneFulfilledResult<T>
  | UseOneErrorResult;

type UseOneLoadingResult = {
  data: undefined;
  isLoading: true;
  error: null;
  db: Db;
};

type UseOneFulfilledResult<T extends { id: string }> = {
  data: T | null;
  isLoading: false;
  error: null;
  db: Db;
};

type UseOneErrorResult = {
  data: undefined;
  isLoading: false;
  error: Error;
  db: Db;
};

type UseOneNoQueryResult = {
  data: undefined;
  isLoading: false;
  error: null;
  db: Db;
};

/** Result of {@link useOneSuspense}: the first matching row (or `null`) plus the {@link Db} handle. */
export type UseOneSuspenseResult<T extends { id: string }> = {
  data: T | null;
  db: Db;
};

export function useOne<_T extends { id: string } = { id: string }>(): UseOneNoQueryResult;
export function useOne<_T extends { id: string } = { id: string }>(
  query: undefined,
  options?: QueryOptions,
): UseOneNoQueryResult;
export function useOne<T extends { id: string }>(
  query: QueryBuilder<T>,
  options?: QueryOptions,
): UseOneResult<T>;
export function useOne<T extends { id: string }>(
  query: QueryBuilder<T> | undefined,
  options?: QueryOptions,
): UseOneResult<T> | UseOneNoQueryResult;
export function useOne<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): UseOneResult<T> | UseOneNoQueryResult {
  const limitedQuery = useMemo(() => (query ? limitQueryToOne(query) : undefined), [query]);
  const result = useAll(limitedQuery, options);

  return useMemo(() => {
    if (result.data === undefined) return result;
    return { data: result.data[0] ?? null, isLoading: false, error: null, db: result.db };
  }, [result]);
}

export function useOneSuspense<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): UseOneSuspenseResult<T> {
  const limitedQuery = useMemo(() => (query ? limitQueryToOne(query) : undefined), [query]);
  // No hooks after `useAllSuspense`: it may suspend via `use()` (see there).
  const { data, db } = useAllSuspense(limitedQuery, options);
  return suspenseResult(data[0] ?? null, db);
}
