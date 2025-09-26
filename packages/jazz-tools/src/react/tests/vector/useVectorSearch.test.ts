// @vitest-environment happy-dom

import { Group, co, z } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createJazzTestAccount } from "../../testing.js";
import { renderHook, waitFor } from "../testUtils.js";
import { useVectorSearch } from "../../index.js";
import { EMBEDDING } from "../../../tools/vector/testing.js";

const Embedding = co.vector(50);
const ListItem = co.map({
  content: z.string(),
  embedding: Embedding,
});
const List = co.list(ListItem);

const initNodeAndListOfVectors = async () => {
  const me = await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });

  const group = Group.create(me);
  group.addMember("everyone", "reader");

  const list = List.create([], group);

  for (const [name, value] of Object.entries(EMBEDDING)) {
    list.$jazz.push(
      ListItem.create(
        {
          content: name,
          embedding: Embedding.create(value, group),
        },
        group,
      ),
    );
  }

  return { account: me, group, list };
};

describe("useVectorSearch (React hook)", () => {
  describe("basic functionality", () => {
    it("should perform vector search with valid data", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
            $limit: list.length,
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
        expect(result.current.error).toBeNull();
        expect(result.current.search).toBeDefined();
      });

      expect(result.current.search?.results).toHaveLength(list.length);
      expect(result.current.search?.results[0]!.value.content).toBe("MAN");
      expect(result.current.search?.results[0]!.similarity).toBeCloseTo(1, 0);
    });

    it("should handle empty list", async () => {
      const { account, group } = await initNodeAndListOfVectors();

      const emptyList = List.create([], group);

      const { result } = renderHook(
        () =>
          useVectorSearch(emptyList, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
        expect(result.current.error).toBeNull();
      });

      expect(result.current.search?.results).toHaveLength(0);
    });

    it("should handle null list", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(null, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
        expect(result.current.error).toBeNull();
      });

      expect(result.current.search).toBeNull();
    });

    it("should handle undefined list", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(undefined, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
        expect(result.current.error).toBeNull();
      });

      expect(result.current.search).toBeUndefined();
    });
  });

  describe("loading states", () => {
    it("should show loading state during search", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
          }),
        { account },
      );

      // Initially should be loading
      expect(result.current.isSearching).toBe(true);
      expect(result.current.search).toBeUndefined();
      expect(result.current.error).toBeNull();

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });
    });
  });

  describe("error handling", () => {
    it("should handle invalid orderBy key", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              content: { $similarity: EMBEDDING.MAN },
            },
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
        expect(result.current.error).toBeDefined();
      });

      expect(typeof result.current.error).toBe("string");
      expect(result.current.error).toContain(
        `Cannot use '\$similarity' with non-vector field 'content'`,
      );
    });
  });

  describe("search options", () => {
    it("should respect limit option", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
            $limit: 2,
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      expect(result.current.search?.results).toHaveLength(2);
    });

    it("should respect similarity threshold", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
            $similarityThreshold: 0.5,
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      const results = result.current.search?.results || [];
      results.forEach((item) => {
        if (item.similarity !== undefined) {
          expect(item.similarity).toBeGreaterThanOrEqual(0.5);
        }
      });
    });

    it("should respect top percent option", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
            $similarityTopPercent: 0.2,
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      expect(result.current.search?.results).toBeDefined();
      expect(
        result.current.search?.results.every((r) => r.similarity! >= 1 - 0.2),
      ).toBe(true);
    });
  });

  describe("search ID memoization", () => {
    it("should not trigger new search when list and options are the same", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const searchOptions = {
        $orderBy: {
          embedding: { $similarity: EMBEDDING.MAN },
        },
      };

      const { result, rerender } = renderHook(
        () => useVectorSearch(list, searchOptions),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      const firstSearch = result.current.search;

      // Rerender with same options
      rerender();

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      // Should be the same search result (memoized)
      expect(result.current.search).toBe(firstSearch);
    });

    it("should trigger new search when list changes", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const separateItem = list.$jazz.shift()!;

      const { result, rerender } = renderHook(
        ({ list }) =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: EMBEDDING.MAN },
            },
            $limit: list.length,
          }),
        {
          account,
          initialProps: { list },
        },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      const firstSearch = result.current.search;

      list.$jazz.push(separateItem);

      rerender({ list });

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      // Should be a different search result
      expect(result.current.search).not.toBe(firstSearch);
      expect(result.current.search?.results).toHaveLength(list.length);
    });
  });

  describe("edge cases", () => {
    it("should handle null query vector", async () => {
      const { account, list } = await initNodeAndListOfVectors();

      const { result } = renderHook(
        () =>
          useVectorSearch(list, {
            $orderBy: {
              embedding: { $similarity: null },
            },
            $limit: list.length,
          }),
        { account },
      );

      await waitFor(() => {
        expect(result.current.isSearching).toBe(false);
      });

      expect(result.current.search?.results).toHaveLength(list.length);
      expect(
        result.current.search?.results.every((r) => r.similarity === undefined),
      ).toBe(true);
    });
  });
});
