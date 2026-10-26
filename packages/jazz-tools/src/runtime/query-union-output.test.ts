import { blake3 } from "@noble/hashes/blake3.js";
import { expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../index.js";
import fixture from "../../fixtures/union-arm-output-v1.json";

const app = s.defineApp({ rooms: s.table({ title: s.string(), code: s.string() }, {}) });

it("infers selected UNION rows through named, array and nested windows", () => {
  const first = app.rooms.select("title");
  const second = app.rooms.where({ code: "a" }).select("title");
  const named = app.union({ first, second }).orderBy("title").offset(1).limit(2);
  const array = app.union([first, second]);
  const nested = app.union({ named, array });
  const full = app.union({ first: app.rooms, second: app.rooms.where({ code: "a" }) });
  expectTypeOf<s.RowOf<typeof named>>().toEqualTypeOf<{ id: string; title: string }>();
  expectTypeOf<s.RowOf<typeof array>>().toEqualTypeOf<{ id: string; title: string }>();
  expectTypeOf<s.RowOf<typeof nested>>().toEqualTypeOf<{ id: string; title: string }>();
  expectTypeOf<s.RowOf<typeof full>>().toEqualTypeOf<{
    id: string;
    title: string;
    code: string;
  }>();
});

// Labels persist in occurrence and serializable-read identities. Pin independently
// assembled bytes, retain the legacy label, and distinguish different arm pages.
it("pins versioned UNION output labels without changing unmodified seed labels", () => {
  const queries = [
    app.rooms,
    app.rooms.select("title").orderBy("title").offset(1).limit(1),
    app.rooms.select("title").orderBy("title").offset(2).limit(1),
  ];
  for (const [index, query] of queries.entries()) {
    const expected = fixture.cases[index]!;
    const bytes = Buffer.from(expected.utf8_hex, "hex");
    expect(bytes.toString("utf8")).toBe(expected.preimage);
    expect(`derived:${Buffer.from(blake3(bytes)).toString("hex")}`).toBe(expected.label);
    const built = JSON.parse(app.union([query])._build());
    expect(built.union.inputs[0].label).toBe(expected.label);
  }
});

// The seed format cannot express these modifiers. Reject them before query
// execution instead of silently returning a different projection or row set.
it("rejects seed modifiers that UNION cannot represent", () => {
  expect(() => app.union([app.rooms.select({ title: { from: 0, to: 1 } })])).toThrow(
    "partial large-value selections",
  );
  expect(() => app.union([app.rooms.includeDeleted()])).toThrow("deleted-row output");
  expect(() => app.union([app.rooms.requireIncludes()])).toThrow("includes");
});
