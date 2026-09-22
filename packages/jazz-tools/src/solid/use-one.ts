import type { Accessor } from "solid-js";
import { limitQueryToOne, type QueryBuilder, type QueryOptions } from "../runtime/db.js";
import { useAll } from "./use-all.js";

export type UseOneResult<T extends { id: string }> = {
  readonly data: T | null | undefined;
  readonly isLoading: boolean;
  readonly error: Error | null;
};

export function useOne<T extends { id: string }>(
  args: Accessor<{
    query: QueryBuilder<T> | undefined;
    options?: QueryOptions | undefined;
  }>,
): UseOneResult<T> {
  const result = useAll(() => {
    const { query, options } = args();
    return { query: query ? limitQueryToOne(query) : undefined, options };
  });

  return {
    get data() {
      return result.data === undefined ? undefined : (result.data[0] ?? null);
    },
    get isLoading() {
      return result.isLoading;
    },
    get error() {
      return result.error;
    },
  };
}
