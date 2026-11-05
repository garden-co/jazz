import { describe, expect, it } from "vitest";
import { schema as s } from "./schema-namespace.js";

// Import the schema surface directly: these contracts do not need native artifacts.
describe("computed table fields across the SQLite restack", () => {
  it("keeps virtual fields out of default encryption and ordinary indexes", () => {
    const table = s.table(
      { space: s.uuid(), score: s.int() },
      {
        spaceRef: s.rel("spaces", "space"),
        doubled: s.computed((row) => row.score * 2),
      },
    );
    expect(table.encrypted({ space: "space" }).encryption?.columns).toEqual(["score"]);
    expect(() => table.indexOnly(["doubled" as never])).toThrow(/unknown column/);
    expect(() => table.encrypted({ space: "space", columns: ["doubled" as never] })).toThrow(
      /cannot be encrypted/,
    );
  });
});
