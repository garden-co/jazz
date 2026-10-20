import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";

type Priority = "low" | "medium" | "high";

describe("virtual computed columns", () => {
  it("keeps transforms stored and writable while computed fields are virtual and read-only", () => {
    const app = s.defineApp({
      items: s.computed(
        s.table(
          {
            score: s.int(),
            storedPriority: s.int().transform<Priority>({
              from: (score) => (score >= 8 ? "high" : score >= 4 ? "medium" : "low"),
              to: (priority) => ({ low: 1, medium: 5, high: 10 })[priority],
            }),
          },
          {},
        ),
        {
          priority: {
            dependsOn: ["score"],
            compute: (row): Priority => {
              expectTypeOf(row).toEqualTypeOf<Readonly<{ score: number }>>();
              return row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low";
            },
          },
        },
      ),
    });

    expect(app.schemaAst?.tables[0]?.columns.map((column) => column.name)).toEqual([
      "score",
      "storedPriority",
    ]);
    expect(s.int()).not.toHaveProperty("computed");
    expectTypeOf<s.RowOf<typeof app.items>>().toEqualTypeOf<{
      id: string;
      score: number;
      readonly priority: Priority;
      storedPriority: Priority;
    }>();
    expectTypeOf<s.InsertOf<typeof app.items>>().toEqualTypeOf<{
      score: number;
      storedPriority: Priority;
    }>();
    const selected = app.items.select("priority");
    expectTypeOf<s.RowOf<typeof selected>>().toEqualTypeOf<{
      id: string;
      readonly priority: Priority;
    }>();
  });
});
