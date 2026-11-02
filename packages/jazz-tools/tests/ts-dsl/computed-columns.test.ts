import { describe, expect, expectTypeOf, it } from "vitest";
import { defineMigration, schema as s, type RowRefValue } from "../../src/index.js";

type Priority = "low" | "medium" | "high";

describe("virtual computed columns", () => {
  it("rejects computed permission row references for actions and relation callbacks", () => {
    const app = s.defineApp(
      {
        items: s.table(
          { score: s.int() },
          { priority: s.computed((row) => (row.score >= 8 ? "high" : "low")) },
        ),
      },
      { migrations: "none" },
    );
    for (const action of ["allowRead", "allowInsert", "allowUpdate", "allowDelete"] as const) {
      expect(() =>
        s.definePermissions(app, ({ policy }) => {
          policy.items[action].where((row) =>
            policy.items.exists.where({
              score: (row as unknown as { priority: RowRefValue }).priority,
            }),
          );
        }),
      ).toThrow(/computed field "priority".*permission/i);
    }
    for (const method of ["whereOld", "whereNew"] as const) {
      expect(() =>
        s.definePermissions(app, ({ policy }) => {
          policy.items.allowUpdate[method]((row) =>
            policy.items.exists.where({
              score: (row as unknown as { priority: RowRefValue }).priority,
            }),
          );
        }),
      ).toThrow(/computed field "priority".*permission/i);
    }
    expect(() =>
      s.definePermissions(app, ({ policy }) => {
        const relation = policy.items.where((row) => ({
          score: (row as unknown as { priority: RowRefValue }).priority,
        }));
        policy.items.allowRead.where(policy.exists(relation));
      }),
    ).toThrow(/computed field "priority".*permission/i);
  });

  it("keeps stored permission references available when another table computes the same name", () => {
    const app = s.defineApp(
      {
        items: s.table({ score: s.int() }, { priority: s.computed((row) => row.score) }),
        stored: s.table({ priority: s.int() }, {}),
      },
      { migrations: "none" },
    );
    const permissions = s.definePermissions(app, ({ policy }) => {
      policy.items.allowRead.where((row) => policy.items.exists.where({ score: row.score }));
      policy.stored.allowRead.where((row) =>
        policy.stored.exists.where({ priority: row.priority }),
      );
    });
    expect(permissions.items?.select?.using).toMatchObject({ type: "Exists", table: "items" });
    expect(permissions.stored?.select?.using).toMatchObject({ type: "Exists", table: "stored" });
  });

  it("rejects computed outer references in permission relation callbacks", () => {
    const app = s.defineApp(
      {
        items: s.table({ score: s.int() }, { priority: s.computed((row) => row.score) }),
        stored: s.table({ priority: s.int() }, {}),
      },
      { migrations: "none" },
    );
    for (const action of ["allowRead", "allowInsert", "allowUpdate", "allowDelete"] as const) {
      expect(() =>
        s.definePermissions(app, ({ policy }) => {
          policy.items[action].where(
            policy.exists(policy.stored.where((row) => ({ priority: row.priority }))),
          );
        }),
      ).toThrow(/computed field "priority".*permission/i);
    }
  });

  it("allows stored outer references when the permission relation computes the same name", () => {
    const app = s.defineApp(
      {
        items: s.table({ score: s.int() }, { priority: s.computed((row) => row.score) }),
        stored: s.table({ priority: s.int() }, {}),
      },
      { migrations: "none" },
    );
    const permissions = s.definePermissions(app, ({ policy }) => {
      const matching = policy.items.where((row: Record<string, RowRefValue>) => ({
        score: row.priority,
      }));
      policy.stored.allowRead.where(policy.exists(matching));
    });
    expect(permissions.stored?.select?.using).toMatchObject({
      type: "ExistsRel",
      rel: { Filter: { predicate: { Cmp: { right: { OuterColumn: { column: "priority" } } } } } },
    });
  });

  it("checks composed permission relations again for each owning table", () => {
    const app = s.defineApp(
      {
        items: s.table({ score: s.int() }, { priority: s.computed((row) => row.score) }),
        stored: s.table({ priority: s.int() }, {}),
      },
      { migrations: "none" },
    );
    const compile = (includeComputedOwner: boolean) =>
      s.definePermissions(app, ({ policy, anyOf }) => {
        const matching = policy.stored.where((row) => ({ priority: row.priority }));
        const composed = anyOf([policy.exists(policy.union([matching, matching]))]);
        policy.stored.allowRead.where(composed);
        if (includeComputedOwner) policy.items.allowRead.where(composed);
      });
    expect(compile(false).stored?.select?.using).toMatchObject({ type: "ExistsRel" });
    expect(() => compile(true)).toThrow(/computed field "priority".*permission/i);
  });

  it("validates declarations and leaves the original table unchanged", () => {
    const base = s.table({ score: s.int() }, {});
    const computed = s.table(base.columns, {
      ...base.relations,
      doubled: s.computed((row) => row.score * 2),
    });
    expect(Object.keys(base.columns)).toEqual(["score"]);
    expect(Object.keys(computed.columns)).toEqual(["score", "doubled"]);
    const migration = defineMigration({ from: { items: base }, to: { items: computed } });
    expect(migration.forward).toEqual([]);
    expect(() => s.table(base.columns, { score: s.computed(() => 1) } as never)).toThrow(
      /collides/,
    );
    expect(() => s.computed(null as never)).toThrow(/callback/);
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
    const app = s.defineApp(
      {
        items: s.table(
          {
            score: s.int(),
            storedPriority: s.int().transform<Priority>({
              from: (score) => (score >= 8 ? "high" : score >= 4 ? "medium" : "low"),
              to: (priority) => ({ low: 1, medium: 5, high: 10 })[priority],
            }),
          },
          {
            priority: s.computed((row): Priority => {
              expectTypeOf(row.score).toEqualTypeOf<number>();
              expectTypeOf(row.storedPriority).toEqualTypeOf<Priority>();
              expectTypeOf(row.id).toEqualTypeOf<string>();
              return row.score >= 8 ? "high" : row.score >= 4 ? "medium" : "low";
            }),
          },
        ),
      },
      { migrations: "none" },
    );

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
