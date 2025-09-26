import { assert, describe, expect, test } from "vitest";
import { co, Group, z } from "../exports.js";
import { createJazzTestAccount } from "../testing.js";
import { searchSimilar } from "./search.js";
import { EMBEDDING } from "./testing.js";

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
          embedding: Embedding.create(value as unknown as number[], group),
        },
        group,
      ),
    );
  }

  return { me, list };
};

describe("Vector search (rank CoList items by similarity)", () => {
  test("when input list is `undefined` • returns undefined", async () => {
    const results = await searchSimilar(undefined, {
      $orderBy: {
        embedding: { $similarity: EMBEDDING.AUNT },
      },
    });

    expect(results).toBeUndefined();
  });

  test("when input list is `null` • returns null", async () => {
    const results = await searchSimilar(null, {
      $orderBy: {
        embedding: { $similarity: EMBEDDING.AUNT },
      },
    });

    expect(results).toBeNull();
  });

  test("when search query is `null` • returns result object with original list without ranking", async () => {
    const { list } = await initNodeAndListOfVectors();

    const loadedList = await List.load(list.$jazz.id, {
      resolve: { $each: true },
    });

    const res = await searchSimilar(loadedList, {
      $orderBy: { embedding: { $similarity: null } },
    });

    expect(res?.durationMs).toBeUndefined();
    expect(res?.results.length).toBe(list.length);
    expect(res?.results.every((r) => typeof r.similarity === "undefined")).toBe(
      true,
    );
  });

  test("when search query is provided • computes similarity and sorts results descending by similarity", async () => {
    const { list } = await initNodeAndListOfVectors();

    const loadedList = await List.load(list.$jazz.id, {
      resolve: {
        $each: true,
      },
    });

    const res = await searchSimilar(loadedList, {
      $orderBy: {
        embedding: { $similarity: EMBEDDING.AUNT },
      },
      $limit: list.length,
    });

    assert(res);
    expect(res?.durationMs).toBeDefined();
    expect(res?.durationMs).toBeGreaterThan(0);
    expect(res.results.length).toBe(list.length);

    const expectedResultsRanking = [
      "AUNT",
      "MOTHER",
      "SISTER",
      "UNCLE",
      "FATHER",
      "WOMAN",
      "BROTHER",
      "QUEEN",
      "KING",
      "MAN",
      "SIBLING",
    ];

    // top result should be the exact match
    expect(res.results[0]?.similarity).toBeCloseTo(1.0, 0);

    for (let i = 0; i < res.results.length; i++) {
      expect(res.results[i]!.value.content).toBe(expectedResultsRanking[i]);
    }
  });

  describe("results filtering", () => {
    test("supports filtering by count `limit`", async () => {
      const { list } = await initNodeAndListOfVectors();

      const res = await searchSimilar(list, {
        $orderBy: {
          embedding: { $similarity: EMBEDDING.SIBLING },
        },
        $limit: 1,
      });

      assert(res);
      expect(res.results.length).toBe(1);
      expect(res.results[0]!.value.content).toBe("SIBLING");
      expect(res.results[0]!.similarity).toBeCloseTo(1.0, 0);
    });

    test("supports filtering by `similarityThreshold`", async () => {
      const { list } = await initNodeAndListOfVectors();

      const THRESHOLD = 0.55;

      const res = await searchSimilar(list, {
        $orderBy: {
          embedding: { $similarity: EMBEDDING.SIBLING },
        },
        $similarityThreshold: THRESHOLD,
      });

      assert(res);
      expect(res.results.every((r) => r.similarity! >= THRESHOLD)).toBe(true);
    });

    test("supports filtering by `similarityTopPercent`", async () => {
      const { list } = await initNodeAndListOfVectors();

      const TOP_PERCENT = 0.15;

      const res = await searchSimilar(list, {
        $orderBy: {
          embedding: { $similarity: EMBEDDING.AUNT },
        },
        $similarityTopPercent: TOP_PERCENT,
      });

      assert(res);
      expect(res.results.length).toBeGreaterThanOrEqual(1);
      expect(res.results[0]!.value.content).toBe("AUNT");

      expect(res.results.every((r) => r.similarity! >= 1 - TOP_PERCENT)).toBe(
        true,
      );
    });
  });

  test("when AbortSignal is engaged • returns unsorted items without similarity", async () => {
    const { list } = await initNodeAndListOfVectors();

    const controller = new AbortController();
    controller.abort();

    const res = await searchSimilar(list, {
      $orderBy: {
        embedding: { $similarity: EMBEDDING.SIBLING },
      },
      $abortSignal: controller.signal,
    });

    assert(res);
    expect(res.durationMs).toBeUndefined();
    expect(res.results.length).toBe(list.length);
    expect(res.results.every((r) => typeof r.similarity === "undefined")).toBe(
      true,
    );
  });

  test("when no `orderBy` key is provided • throws an error", async () => {
    const { list } = await initNodeAndListOfVectors();

    await expect(
      searchSimilar(list, {
        $orderBy: {},
      }),
    ).rejects.toThrow(/At least one '\$orderBy' key is required/);
  });

  test("when multiple `orderBy` keys are provided • throws an error", async () => {
    const { list } = await initNodeAndListOfVectors();

    await expect(
      searchSimilar(list, {
        $orderBy: {
          embedding: { $similarity: EMBEDDING.SIBLING },
          content: { $similarity: EMBEDDING.SIBLING },
        },
      }),
    ).rejects.toThrow(
      /Only single '\$orderBy' key is allowed for vector similarity/,
    );
  });

  test("when `orderBy` key does not reference a vector field • throws an error", async () => {
    const { list } = await initNodeAndListOfVectors();

    await expect(
      searchSimilar(list, {
        $orderBy: {
          // 'content' is a string, not a vector
          content: { $similarity: EMBEDDING.SIBLING },
        },
      }),
    ).rejects.toThrow(
      /Cannot use '\$similarity' with non-vector field 'content'/,
    );
  });

  test("when query vector has dimensions mismatch • throws an error", async () => {
    const { list } = await initNodeAndListOfVectors();

    await expect(
      searchSimilar(list, {
        $orderBy: {
          embedding: { $similarity: [1, 0, 0] },
        },
      }),
    ).rejects.toThrow(/Vector dimensions don't match/);
  });
});
