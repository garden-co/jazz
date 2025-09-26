import { CoList, CoVector } from "../tools";

/**
 * An item in the search outcome.
 *
 * - `value` is the original item from the list.
 * - `similarity` is the cosine similarity score of the item's vector to the query vector (between `0` and `1`).
 *
 * If `similarity` is `undefined`, the item was not matched by the search.
 */
export type VectorSearchOutcomeItem<Value> = {
  value: Value;
  similarity?: number;
};

/**
 * The result of a vector search. Either:
 * - An array of items with their similarity score (sorted descending); or
 * - `undefined`/`null` (which extends the default loading behavior)
 */
export type VectorSearchOutcome<L extends CoList> =
  | { results: Array<VectorSearchOutcomeItem<L[number]>> }
  | undefined
  | null;

const SimilarityOp = "$similarity" as const;

type QueryVector = ReadonlyArray<number> | number[] | Float32Array;

type OrderBySimilarity = {
  [SimilarityOp]: QueryVector | null;
};

type OrderByKey<L extends CoList> = Exclude<
  keyof L[number],
  "$jazz" | "$type$" | "toJSON"
>;

// Only one key allowed at a time - creates a union of single-key objects
type OrderBy<L extends CoList> = {
  [K in OrderByKey<L>]?: OrderBySimilarity;
};

// Only one of the three filter options can be present at a time (or none)
export type VectorSearchFilter =
  | {
      $similarityTopPercent: number;
      $similarityThreshold?: never;
      $limit?: never;
    }
  | {
      $similarityThreshold: number;
      $similarityTopPercent?: never;
      $limit?: never;
    }
  | {
      $limit: number;
      $similarityTopPercent?: never;
      $similarityThreshold?: never;
    }
  | {};

export type VectorSearchOptions<L extends CoList> = {
  $orderBy: OrderBy<L>;
  $abortSignal?: AbortSignal;
} & VectorSearchFilter;

const DEFAULT_FILTER_OPTION: VectorSearchFilter = { $limit: 25 };

/**
 * Search a list of items for the most similar items to a given query.
 *
 * @returns @link VectorSearchOutcome
 */
export const searchSimilar = async <L extends CoList>(
  list: L | undefined | null,
  options: VectorSearchOptions<L>,
): Promise<VectorSearchOutcome<L>> => {
  if (list === null) return null;
  if (list === undefined) return undefined;

  const selector = options.$orderBy;

  if (Object.keys(selector).length === 0) {
    throw new Error("At least one '\$orderBy' key is required");
  } else if (Object.keys(selector).length > 1) {
    throw new Error(
      "Only single '\$orderBy' key is allowed for vector similarity",
    );
  }

  const listItemVectorParam = Object.keys(selector)[0]! as OrderByKey<L>;
  const query = selector[listItemVectorParam]![SimilarityOp];

  const wrappedList: VectorSearchOutcomeItem<L[number]>[] = list.map(
    (value) => ({ value }),
  );

  if (query === null) return { results: wrappedList };

  const queryVector =
    query instanceof Float32Array ? query : new Float32Array(query);

  const similarityResults = await Promise.all(
    wrappedList.map(async (listItem) => {
      if (options.$abortSignal?.aborted) return listItem;
      if (!listItem.value) return listItem;

      const vector =
        "_refs" in listItem.value
          ? await (listItem.value?._refs[listItemVectorParam]).load()
          : listItem.value?.[listItemVectorParam];

      if (!vector) return listItem;

      if (!(vector instanceof CoVector)) {
        throw new Error(
          `Cannot use '${SimilarityOp}' with non-vector field '${String(listItemVectorParam)}'`,
        );
      }

      return {
        value: listItem.value,
        similarity: vector.$jazz.cosineSimilarity(queryVector),
      };
    }),
  );

  if (options.$abortSignal?.aborted)
    // if the search was aborted, return the results as is
    return {
      results: similarityResults,
    };

  // TODO: Optimize the .map/.filter/.sort to be more efficient (not a bottleneck though)
  const results = similarityResults
    .filter((value) => typeof value.similarity === "number")
    .sort((a, b) => (b.similarity ?? 0) - (a.similarity ?? 0));

  return {
    results: filterResults(results, options),
  };
};

const filterResults = <L extends CoList>(
  results: VectorSearchOutcomeItem<L[number]>[],
  filter: VectorSearchFilter,
) => {
  if ("$limit" in filter) {
    return results.slice(0, filter.$limit);
  }

  if (
    "$similarityThreshold" in filter &&
    filter.$similarityThreshold !== undefined
  ) {
    const clampedThreshold = Math.max(
      -1,
      Math.min(1, filter.$similarityThreshold),
    );

    const lastIndex = results.findIndex(
      (r) =>
        typeof r.similarity === "number" && r.similarity < clampedThreshold,
    );
    return lastIndex === -1 ? results : results.slice(0, lastIndex);
  }

  if (
    "$similarityTopPercent" in filter &&
    filter.$similarityTopPercent !== undefined
  ) {
    const clampedTopPercent = Math.max(
      0,
      Math.min(1, filter.$similarityTopPercent),
    );

    const topPercentThreshold = 1 - clampedTopPercent;
    const topSimilarityScore = results[0]?.similarity ?? 0;

    const similarityThreshold = topSimilarityScore * topPercentThreshold;

    return filterResults(results, {
      $similarityThreshold: similarityThreshold,
    });
  }

  return filterResults(results, DEFAULT_FILTER_OPTION);
};
