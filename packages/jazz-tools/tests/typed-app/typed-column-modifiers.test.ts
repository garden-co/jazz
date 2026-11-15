import { describe, expectTypeOf, it } from "vitest";
import type { InsertOf, RowOf, WhereOf } from "../../src/index.js";
import { app, type ItemInsert } from "./apps/column-modifiers/jazz.generated.js";

describe("typed column modifiers", () => {
  it("preserves specialised row and insert types", () => {
    expectTypeOf<RowOf<typeof app.items>>().toEqualTypeOf<{
      id: string;
      count: number;
      tags: string[];
      transformed: number;
      optionalCount: number | null;
      defaultedCount: number;
      optionalDefaulted: number | null;
      transformedOptional: number | null;
      defaultedMerge: number;
    }>();

    expectTypeOf<InsertOf<typeof app.items>>().toEqualTypeOf<{
      count: number;
      tags: string[];
      transformed: number;
      optionalCount?: number | null;
      defaultedCount?: number;
      optionalDefaulted?: number | null;
      transformedOptional?: number | null;
      defaultedMerge?: number;
    }>();
  });

  it("matches the generated insert interface", () => {
    expectTypeOf<InsertOf<typeof app.items>>().toEqualTypeOf<ItemInsert>();
  });

  it("keeps query filters based on stored column types", () => {
    expectTypeOf<WhereOf<typeof app.items>["count"]>().branded.toEqualTypeOf<
      | number
      | {
          eq?: number;
          ne?: number;
          gt?: number;
          gte?: number;
          lt?: number;
          lte?: number;
          in?: number[];
          notIn?: number[];
        }
      | undefined
    >();

    expectTypeOf<WhereOf<typeof app.items>["transformed"]>().branded.toEqualTypeOf<
      | string
      | {
          eq?: string;
          ne?: string;
          contains?: string;
          in?: string[];
          notIn?: string[];
        }
      | undefined
    >();
  });
});
