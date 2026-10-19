import { schema as s } from "./index.js";

const price = s
  .float()
  .transform({
    dependsOn: ["scale"],
    from: (value, context: Readonly<{ scale: number }>) => value / context.scale,
    to: (value: number, context: Readonly<{ scale: string }>) => value * Number(context.scale),
  })
  .optional();
const scale = s
  .int()
  .transform({ from: (value) => String(value), to: (value: string) => Number(value) });
s.table({ price, scale }, {});
// @ts-expect-error the completed table validates stored context types
s.table({ price, scale: s.string() }, {});
// @ts-expect-error dependencies must exist in the completed table
s.table({ price }, {});
// @ts-expect-error a transform cannot depend on itself
s.table({ scale: price }, {});

const extraContext = s.int().transform({
  dependsOn: ["scale"],
  from: (value, context: Readonly<{ scale: number; other: string }>) => value + context.scale,
  to: (value: number, _context: Readonly<{ scale: string }>) => value,
});
// @ts-expect-error context exposes only declared dependency keys
s.table({ amount: extraContext, scale }, {});

s.table(
  {
    amount: s.int().transform({
      dependsOn: ["scale"],
      from(value, context) {
        // @ts-expect-error unannotated dependencies remain unknown, never implicit any
        return value + context.scale;
      },
      to(value: number, context) {
        // @ts-expect-error the dependency context is read-only
        context.scale = 2;
        return value;
      },
    }),
    scale: s.int(),
  },
  {},
);
