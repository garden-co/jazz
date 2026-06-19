import { describe, expect, it } from "vitest";
import { normalizeSchemaHashInfos } from "./schema-hash-display";

describe("schema hash display", () => {
  it("sorts schema hashes by publishedAt descending with unknown times last", () => {
    const sorted = normalizeSchemaHashInfos(
      ["old", "unknown", "new"],
      [
        { hash: "old", publishedAt: 100 },
        { hash: "new", publishedAt: 300 },
      ],
    );

    expect(sorted).toEqual([
      { hash: "new", publishedAt: 300 },
      { hash: "old", publishedAt: 100 },
      { hash: "unknown", publishedAt: null },
    ]);
  });
});
