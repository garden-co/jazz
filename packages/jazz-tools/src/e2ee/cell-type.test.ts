import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { encodeCellType } from "./cell-type.js";

const app = s.defineApp({
  values: s.table(
    {
      text: s.string(),
      renamed: s.string().default("default"),
      nullable: s.string().optional(),
      json: s.json(),
      choice: s.enum("a", "b"),
      bytes: s.bytes(),
      array: s.array(s.string()),
      event: s.enum({ message: { text: s.string() } }),
    },
    {},
  ),
});

// Literal UTF-8 fixtures pin the durable authenticated descriptor, separately
// from the physical Groove carrier (which conflates Text, Json and Enum).
it.each([
  ["text", '{"column_type":{"type":"Text"},"nullable":false}'],
  ["renamed", '{"column_type":{"type":"Text"},"nullable":false}'],
  ["nullable", '{"column_type":{"type":"Text"},"nullable":true}'],
  ["json", '{"column_type":{"type":"Json"},"nullable":false}'],
  ["choice", '{"column_type":{"type":"Enum","variants":["a","b"]},"nullable":false}'],
  ["bytes", '{"column_type":{"type":"Bytea"},"nullable":false}'],
  ["array", '{"column_type":{"element":{"type":"Text"},"type":"Array"},"nullable":false}'],
  [
    "event",
    '{"column_type":{"cases":[{"fields":[{"column_type":{"type":"Text"},"name":"text","nullable":false}],"name":"message"}],"type":"EnumPayload"},"nullable":false}',
  ],
])("pins the logical %s descriptor bytes", (name, fixture) => {
  const column = app.wasmSchema.values.columns.find((column) => column.name === name)!;
  expect(encodeCellType(column)).toEqual(new TextEncoder().encode(fixture));
});

it("canonicalises JSON schema key order without discarding its constraints", () => {
  const schema = s.defineApp({
    values: s.table(
      {
        first: s.json({ type: "string", minLength: 1 }),
        reordered: s.json({ minLength: 1, type: "string" }),
        changed: s.json({ type: "string", minLength: 2 }),
      },
      {},
    ),
  });
  const [first, reordered, changed] = schema.wasmSchema.values.columns;
  expect(encodeCellType(first!)).toEqual(encodeCellType(reordered!));
  expect(encodeCellType(first!)).not.toEqual(encodeCellType(changed!));
});

it("rejects sparse arrays in JSON type metadata rather than erasing their holes", () => {
  const required: string[] = [];
  required.length = 1;
  const schema = s.defineApp({
    values: s.table(
      {
        invalid: s.json({ type: "object", required }),
      },
      {},
    ),
  });
  expect(() => encodeCellType(schema.wasmSchema.values.columns[0]!)).toThrow(
    "finite JSON metadata",
  );
});
