import { useMemo } from "react";
import { limitQueryToOne, type QueryBuilder, type QueryOptions } from "../runtime/db.js";
import type { QuerySettlementLevel } from "../shared/index.js";
import { useAll, useAllSuspense } from "./use-all.js";

export type UseOneResult<T extends { id: string }> =
  | UseOneLoadingResult<T>
  | UseOneFulfilledResult<T>
  | UseOneErrorResult;

type UseOneLoadingResult<T extends { id: string }> = {
  data: T | null | undefined;
  isLoading: true;
  error: null;
  highestSettledAt: QuerySettlementLevel;
};

type UseOneFulfilledResult<T extends { id: string }> = {
  data: T | null;
  isLoading: false;
  error: null;
  highestSettledAt: QuerySettlementLevel;
};

type UseOneErrorResult = {
  data: undefined;
  isLoading: false;
  error: Error;
  highestSettledAt: QuerySettlementLevel;
};

type UseOneNoQueryResult = {
  data: undefined;
  isLoading: false;
  error: null;
  highestSettledAt: QuerySettlementLevel;
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
    if (result.data === undefined) {
      if (result.error) {
        return {
          data: undefined,
          isLoading: false,
          error: result.error,
          highestSettledAt: result.highestSettledAt,
        };
      }
      if (result.isLoading) {
        return {
          data: undefined,
          isLoading: true,
          error: null,
          highestSettledAt: result.highestSettledAt,
        };
      }
      return result;
    }
    if (result.isLoading) {
      return {
        data: result.data[0] ?? null,
        isLoading: true,
        error: null,
        highestSettledAt: result.highestSettledAt,
      };
    }
    return {
      data: result.data[0] ?? null,
      isLoading: false,
      error: result.error,
      highestSettledAt: result.highestSettledAt,
    };
  }, [result]);
}

export function useOneSuspense<T extends { id: string }>(
  query?: QueryBuilder<T>,
  options?: QueryOptions,
): T | null {
  const limitedQuery = useMemo(() => (query ? limitQueryToOne(query) : undefined), [query]);
  return useAllSuspense(limitedQuery, options)[0] ?? null;
}
