import { schema as s } from "./index.js";
import type { TSInitFromSqlType, TSTypeFromSqlType } from "./schema.js";

const event = s.enum({
  message: {
    requiredText: s.string(),
    nullableText: s.string().optional(),
    defaultedText: s.string().default("default"),
  },
  empty: {},
});
const app = s.defineApp({ events: s.table({ event }, {}) });
const omitted: s.InsertOf<typeof app.events> = {
  event: { type: "message", requiredText: "required" },
};
const nullable: s.InsertOf<typeof app.events> = {
  event: { type: "message", requiredText: "required", nullableText: null },
};
// @ts-expect-error Required payload fields cannot be omitted.
const missing: s.InsertOf<typeof app.events> = { event: { type: "message" } };
const invalid: s.InsertOf<typeof app.events> = {
  // @ts-expect-error Defaulted non-nullable fields cannot be null.
  event: { type: "message", requiredText: "x", defaultedText: null },
};
declare const row: s.RowOf<typeof app.events>;
if (row.event.type === "message") {
  const required: string = row.event.requiredText;
  const defaulted: string = row.event.defaultedText;
  const nullable: string | null = row.event.nullableText;
  void [required, defaulted, nullable];
}

// Pin recursive mapping independently of which payload shapes the DSL accepts.
type Nested = {
  kind: "ARRAY";
  element: {
    kind: "ENUM";
    cases: [
      {
        name: "nested";
        fields: [
          {
            name: "event";
            sqlType: typeof event.__jazzSqlType;
            nullable: false;
          },
        ];
      },
    ];
  };
};
const nested: TSInitFromSqlType<Nested> = [
  { type: "nested", event: { type: "message", requiredText: "x" } },
];
declare const nestedRow: TSTypeFromSqlType<Nested>;
if (nestedRow[0]!.event.type === "message") {
  const defaulted: string = nestedRow[0]!.event.defaultedText;
  const nullable: string | null = nestedRow[0]!.event.nullableText;
  void [defaulted, nullable];
}
const transformed = s.defineApp({
  events: s.table(
    {
      event: event.transform({
        from: (value) => value.type,
        to: () => ({ type: "empty" as const }),
      }),
    },
    {},
  ),
});
const transformedInit: s.InsertOf<typeof transformed.events> = { event: "message" };
void [omitted, nullable, missing, invalid, nested, transformedInit];

const sameShape = s.enum({ message: { text: s.string().default("default") } }).transform({
  from: (value) => value,
  to: (value) => ({ ...value, text: value.text.toUpperCase() }),
});
const sameShapeApp = s.defineApp({
  events: s.table(
    {
      event: sameShape,
      modified: sameShape.optional().default({ type: "message" }).merge("lww"),
    },
    {},
  ),
});
const completeTransformInput: s.InsertOf<typeof sameShapeApp.events> = {
  event: { type: "message", text: "present" },
  modified: { type: "message", text: "present" },
};
const missingTransformInput: s.InsertOf<typeof sameShapeApp.events> = {
  // @ts-expect-error Same-shape transform callbacks require their complete input.
  event: { type: "message" },
};
const missingModifiedTransformInput: s.InsertOf<typeof sameShapeApp.events> = {
  event: { type: "message", text: "present" },
  // @ts-expect-error Modifiers must preserve the explicit transform input contract.
  modified: { type: "message" },
};
void [completeTransformInput, missingTransformInput, missingModifiedTransformInput];
