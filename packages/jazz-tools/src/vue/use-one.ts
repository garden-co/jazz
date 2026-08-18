import { computed, toValue, type MaybeRefOrGetter, type Ref } from "vue";
import { limitQueryToOne, type QueryBuilder, type QueryOptions } from "../runtime/db.js";
import { useAll, useAllSuspense } from "./use-all.js";

export interface UseOneResult<T extends { id: string }> {
  data: Ref<T | null | undefined>;
  error: Ref<Error | null>;
  loading: Ref<boolean>;
}

export interface UseOneSuspenseResult<T extends { id: string }> {
  data: Ref<T | null | undefined>;
  error: Ref<Error | null>;
}

export function useOne<T extends { id: string }>(
  query: MaybeRefOrGetter<QueryBuilder<T> | undefined>,
  options?: MaybeRefOrGetter<QueryOptions | undefined>,
): UseOneResult<T> {
  const result = useAll(() => {
    const resolved = toValue(query);
    return resolved ? limitQueryToOne(resolved) : undefined;
  }, options);
  const data = computed(() =>
    result.data.value === undefined ? undefined : (result.data.value[0] ?? null),
  );
  return { data, error: result.error, loading: result.loading };
}

export async function useOneSuspense<T extends { id: string }>(
  query: QueryBuilder<T>,
  options?: QueryOptions,
): Promise<UseOneSuspenseResult<T>> {
  const result = await useAllSuspense(limitQueryToOne(query), options);
  const data = computed(() =>
    result.data.value === undefined ? undefined : (result.data.value[0] ?? null),
  );
  return { data, error: result.error };
}
