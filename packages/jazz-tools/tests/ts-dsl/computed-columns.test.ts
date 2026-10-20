import { describe, expect, expectTypeOf, it } from "vitest";
import { defineMigration, schema as s } from "../../src/index.js";

type Priority = "low" | "medium" | "high";

describe("virtual computed columns", () => {
  it("validates declarations and leaves the original table unchanged", () => {
    const base = s.table({ score: s.int() }, {});
    const computed = s.computed(base, {
      doubled: { dependsOn: ["score"], compute: (row) => row.score * 2 },
    });
    expect(Object.keys(base.columns)).toEqual(["score"]);
    expect(Object.keys(computed.columns)).toEqual(["score", "doubled"]);
    const migration = defineMigration({ from: { items: base }, to: { items: computed } });
    expect(migration.forward).toEqual([]);
    expect(() => s.computed(base, { score: { dependsOn: [], compute: () => 1 } })).toThrow(
      /collides/,
    );
    expect(() =>
      s.computed(base, { invalid: { dependsOn: ["missing"], compute: () => 1 } } as never),
    ).toThrow(/unknown/);
    expect(() =>
      s.computed(base, { invalid: { dependsOn: ["invalid"], compute: () => 1 } } as never),
    ).toThrow(/itself/);
    expect(() =>
      s.computed(base, {
        a: { dependsOn: ["b"], compute: () => 1 },
        b: { dependsOn: ["a"], compute: () => 1 },
      } as never),
    ).toThrow(/cyclic/i);
    expect(() => computed.indexOnly(["doubled"] as never)).toThrow(/unknown column/);
    expect(() => computed.branchBy("doubled" as never)).toThrow(/unknown column/);
    expect(() =>
      s.table(
        {
          ...computed.columns,
          stored: s.int().transform({
            dependsOn: ["doubled"],
            from: (value) => value,
            to: (value: number) => value,
          }),
        } as never,
        {},
      ),
    ).toThrow(/cannot depend on computed/);
  });

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
