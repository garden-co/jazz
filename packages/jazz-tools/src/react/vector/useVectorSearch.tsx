import { CoList } from "jazz-tools";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  VectorSearchFilter,
  VectorSearchOptions,
  VectorSearchOutcome,
  searchSimilar,
} from "../../vector";

export type VectorSearchHookOptions<L extends CoList> = {
  $orderBy: VectorSearchOptions<L>["$orderBy"];
} & VectorSearchFilter;

/**
 * React hook that performs a vector search on a CoList.
 * Automatically recalculates the results when the searched list or query changes.
 */
export const useVectorSearch = <L extends CoList>(
  list: L | undefined | null,
  options: VectorSearchHookOptions<L>,
): {
  isSearching: boolean;
  search: VectorSearchOutcome<L>;
  error: string | null;
} => {
  const [search, setSearch] = useState<VectorSearchOutcome<L>>(undefined);
  const [error, setError] = useState<string | null>(null);
  const [isSearching, setIsSearching] = useState(false);

  const searchKey = useMemo(
    () => makeSearchKey(list, options),
    [list, options],
  );
  const currentSearchKeyRef = useRef(searchKey);
  currentSearchKeyRef.current = searchKey;

  useEffect(() => {
    const abortController = new AbortController();

    (async () => {
      setIsSearching(true);

      try {
        const results = await searchSimilar(
          list,
          Object.assign({}, options, { $abortSignal: abortController.signal }),
        );

        if (searchKey === currentSearchKeyRef.current) {
          setSearch(results);
        }
      } catch (error) {
        if (searchKey === currentSearchKeyRef.current) {
          setError(error instanceof Error ? error.message : String(error));
        }
      } finally {
        if (searchKey === currentSearchKeyRef.current) {
          setIsSearching(false);
        }
      }
    })();

    return () => {
      abortController.abort();
    };
  }, [list, searchKey]);

  return { isSearching, search, error };
};

const makeSearchKey = <L extends CoList>(
  list: L | null | undefined,
  options: VectorSearchHookOptions<L>,
) => {
  const queryKey = Object.entries(options.$orderBy)
    .map(([key, orderBy]) => {
      const orderByKey = Object.entries(orderBy ?? {})
        .map(([key, value]) => `${key}::${value?.slice(0, 3).join(":")}`)
        .join("|");
      return `${key}<>${orderByKey}`;
    })
    .join("|");

  // Include other search options in the key
  const otherOptions = Object.entries(options)
    .filter(([key]) => key !== "$orderBy")
    .map(([key, value]) => `${key}::${JSON.stringify(value)}`)
    .join("|");

  return `${list?.$jazz.id ?? "nolist"}|${queryKey}|${otherOptions}`;
};
