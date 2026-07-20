import { describe, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";

const schema = {
  items: s.table({
    count: s.int().merge("counter"),
    tags: s.array(s.string()).merge("g-set"),
    transformed: s.string().transform({
      from: (value) => value.length,
      to: (value) => value.toString(),
    }),
    optionalCount: s.int().optional(),
    defaultedCount: s.int().default(0),
    optionalDefaulted: s.int().optional().default(null),
    transformedOptional: s
      .string()
      .optional()
      .transform({
        from: (value) => (value === null ? null : value.length),
        to: (value) => (value === null ? null : value.toString()),
      }),
    defaultedMerge: s.int().default(0).merge("counter"),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

describe("typed column modifiers", () => {
  it("preserves specialised row and insert types", () => {
    expectTypeOf<s.RowOf<typeof app.items>>().toEqualTypeOf<{
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

    expectTypeOf<s.InsertOf<typeof app.items>>().toEqualTypeOf<{
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

  it("keeps query filters based on stored column types", () => {
    expectTypeOf<s.WhereOf<typeof app.items>["count"]>().branded.toEqualTypeOf<
      | number
      | {
          eq?: number;
          ne?: number;
          gt?: number;
          gte?: number;
          lt?: number;
          lte?: number;
          in?: number[];
        }
      | undefined
    >();

    expectTypeOf<s.WhereOf<typeof app.items>["transformed"]>().branded.toEqualTypeOf<
      | string
      | {
          eq?: string;
          ne?: string;
          contains?: string;
          in?: string[];
        }
      | undefined
    >();
  });
});
