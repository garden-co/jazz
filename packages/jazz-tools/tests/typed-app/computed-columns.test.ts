import { describe, expect, expectTypeOf, it } from "vitest";
import type { InsertOf, RowOf } from "../../src/index.js";
import { compileTestApp, testApp, testPermissions } from "../../src/testing/test-app.js";
import { app as priorityApp, type ItemPriority } from "./apps/computed-priority/jazz.generated.js";

type Priority = "low" | "medium" | "high";

// Permissions can't read computed fields: the loader checks every policy
// against the stored columns, so a computed field is no column there.
const COMPUTED_IN_POLICY = /unknown column `?(items\.)?priority`?/;

const computedItems = testApp(
  {
    tables: {
      items: {
        columns: { score: "int" },
        computed: { priority: { type: "int", from: ["score"] } },
      },
      stored: { columns: { priority: "int" } },
    },
  },
  { computed: { items: { priority: (row: { score: number }) => row.score } } },
);

describe("virtual computed columns", () => {
  it("rejects computed permission row references for actions and relation conditions", () => {
    const refersToComputed = "exists items(score = this.priority)";
    for (const action of ["read", "insert", "delete"] as const) {
      expect(() =>
        testPermissions(computedItems, { tables: { items: { [action]: refersToComputed } } }),
      ).toThrow(COMPUTED_IN_POLICY);
    }
    for (const side of ["old", "new"] as const) {
      const update = { old: "true", new: "true", [side]: refersToComputed };
      expect(() => testPermissions(computedItems, { tables: { items: { update } } })).toThrow(
        COMPUTED_IN_POLICY,
      );
    }
    expect(() =>
      testPermissions(computedItems, {
        tables: { items: { read: "exists (from items where score = outer.priority)" } },
      }),
    ).toThrow(COMPUTED_IN_POLICY);
  });

  it("keeps stored permission references available when another table computes the same name", () => {
    const permissions = testPermissions(computedItems, {
      tables: {
        items: { read: "exists items(score = this.score)" },
        stored: { read: "exists stored(priority = this.priority)" },
      },
    });
    expect(permissions.items?.select?.using).toMatchObject({ type: "Exists", table: "items" });
    expect(permissions.stored?.select?.using).toMatchObject({ type: "Exists", table: "stored" });
  });

  it("rejects computed outer references in permission relations", () => {
    for (const action of ["read", "insert", "delete"] as const) {
      expect(() =>
        testPermissions(computedItems, {
          tables: { items: { [action]: "exists stored(priority = this.priority)" } },
        }),
      ).toThrow(COMPUTED_IN_POLICY);
    }
  });

  it("allows stored outer references when the permission relation computes the same name", () => {
    const permissions = testPermissions(computedItems, {
      tables: { stored: { read: "exists (from items where score = outer.priority)" } },
    });
    expect(permissions.stored?.select?.using).toMatchObject({
      type: "ExistsRel",
      rel: { Filter: { predicate: { Cmp: { right: { OuterColumn: { column: "priority" } } } } } },
    });
  });

  it("checks composed permission relations again for each owning table", () => {
    // FILES-AS-SOURCE: the DSL composed `union([matching, matching])`; the
    // engine refuses Union inside ExistsRel policies ("core schema ExistsRel
    // policies do not support Union yet"), so the composition is anyOf.
    const composed =
      "anyOf(exists (from stored where priority = outer.priority), exists (from stored where priority = outer.priority))";
    const compile = (includeComputedOwner: boolean) =>
      testPermissions(computedItems, {
        tables: {
          stored: { read: composed },
          ...(includeComputedOwner ? { items: { read: composed } } : {}),
        },
      });
    expect(compile(false).stored?.select?.using).toMatchObject({ type: "Or" });
    expect(() => compile(true)).toThrow(COMPUTED_IN_POLICY);
  });

  it("validates declarations and leaves the stored schema unchanged", () => {
    const base = { columns: { score: "int" } };
    const computed = { ...base, computed: { doubled: { type: "int", from: ["score"] } } };
    const doubled = { computed: { items: { doubled: (row: { score: number }) => row.score * 2 } } };
    // A computed field is not stored: the schema (and its hash) stay as they were.
    expect(compileTestApp({ tables: { items: computed } }).schema.current).toBe(
      compileTestApp({ tables: { items: base } }).schema.current,
    );
    expect(() =>
      testApp(
        { tables: { items: { ...base, computed: { score: { type: "int", from: ["score"] } } } } },
        { computed: { items: { score: () => 1 } } },
      ),
    ).toThrow(/already has a column score; a computed field needs its own name/);
    expect(() =>
      testApp({ tables: { items: computed } }, { computed: { items: {} } } as never),
    ).toThrow(/doesn't implement it/);
    expect(() =>
      testApp({ tables: { items: { ...computed, branchBy: ["doubled"] } } }, doubled),
    ).toThrow(/unknown branch column "doubled"/);
    expect(() =>
      testApp(
        {
          tables: {
            items: {
              ...base,
              computed: {
                doubled: { type: "int", from: ["score"] },
                quad: { type: "int", from: ["doubled"] },
              },
            },
          },
        },
        { computed: { items: { ...doubled.computed.items, quad: () => 4 } } },
      ),
    ).toThrow(
      /`doubled` is a computed field; a computed field is computed from stored columns only/,
    );
  });

  it("refuses an index on a computed field", () => {
    expect(() =>
      testApp(
        {
          tables: {
            items: {
              columns: { score: "int" },
              indexes: ["doubled"],
              computed: { doubled: { type: "int", from: ["score"] } },
            },
          },
        },
        { computed: { items: { doubled: () => 1 } } },
      ),
    ).toThrow(/doubled/);
  });

  it("keeps transforms stored and writable while computed fields are virtual and read-only", () => {
    expect(priorityApp.wasmSchema.items!.columns.map((column) => column.name)).toEqual([
      "score",
      "storedPriority",
    ]);
    expectTypeOf<ItemPriority>().toEqualTypeOf<Priority>();
    expectTypeOf<RowOf<typeof priorityApp.items>>().toEqualTypeOf<{
      id: string;
      score: number;
      readonly priority: Priority;
      storedPriority: Priority;
    }>();
    expectTypeOf<InsertOf<typeof priorityApp.items>>().toEqualTypeOf<{
      score: number;
      storedPriority: Priority;
    }>();
    const selected = priorityApp.items.select("priority");
    expectTypeOf<RowOf<typeof selected>>().toEqualTypeOf<{
      id: string;
      readonly priority: Priority;
    }>();
  });
});
