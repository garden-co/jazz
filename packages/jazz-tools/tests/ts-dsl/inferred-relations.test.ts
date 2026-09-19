import { describe, expect, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { transformRows } from "../../src/runtime/row-transformer.js";
import { translateQuery } from "../../src/runtime/query-adapter.js";
import { app } from "./fixtures/inferred-relations/app.js";
import { schema, type AppSchema } from "./fixtures/inferred-relations/schema.js";

const checkedSchema = s.defineSchema(schema);
const checkedApp: s.App<typeof checkedSchema> = s.defineApp(checkedSchema);
const rawSliceable: s.SliceableApp<AppSchema> = s.defineSliceableApp(schema);
const checkedSliceable: s.SliceableApp<typeof checkedSchema> = s.defineSliceableApp(checkedSchema);
const slice = checkedSliceable.slice("records", "categories");
const arrays = app.records
  .include({ categories: true, people: true, analyses: true, statuses: true })
  .requireIncludes();
const scalars = app.records.include({
  category: true,
  person: true,
  team: true,
  teams: true,
  addressRelation: true,
});
const required = scalars.requireIncludes();
const reverse = app.categories.include({ recordsViaCategories: true, teamsViaCategory: true });
const sliced = slice.records.include({ categories: true });
type Label = { id: string; label: string };
type Person = { id: string; name: string };
type Team = { id: string; category_id: string };

describe("explicit public relation APIs", () => {
  it("preserves array results, scalar nullability, and required-include refinement", () => {
    expectTypeOf<s.RowOf<typeof arrays>["categories"]>().toEqualTypeOf<Label[]>();
    expectTypeOf<s.RowOf<typeof arrays>["people"]>().toEqualTypeOf<Person[]>();
    expectTypeOf<s.RowOf<typeof arrays>["analyses"]>().toEqualTypeOf<
      Array<{ id: string; summary: string }>
    >();
    expectTypeOf<s.RowOf<typeof arrays>["statuses"]>().toEqualTypeOf<Label[]>();
    expectTypeOf<s.RowOf<typeof scalars>["category"]>().toEqualTypeOf<Label | null>();
    expectTypeOf<s.RowOf<typeof scalars>["person"]>().toEqualTypeOf<Person | null>();
    expectTypeOf<s.RowOf<typeof scalars>["team"]>().toEqualTypeOf<Team | null>();
    expectTypeOf<s.RowOf<typeof required>["category"]>().toEqualTypeOf<Label>();
    expectTypeOf<s.RowOf<typeof required>["person"]>().toEqualTypeOf<Person>();
    expectTypeOf<s.RowOf<typeof required>["team"]>().toEqualTypeOf<Team>();
    expectTypeOf<s.RowOf<typeof required>["teams"]>().toEqualTypeOf<Team[]>();
    // A nullable FK remains nullable even when matching includes are required.
    expectTypeOf<s.RowOf<typeof required>["addressRelation"]>().toEqualTypeOf<Person | null>();
    expectTypeOf<s.RowOf<typeof required>["personId"]>().toEqualTypeOf<string>();
    expectTypeOf<s.RowOf<typeof arrays>["person_ids"]>().toEqualTypeOf<string[]>();
    expect(JSON.parse(arrays._build())).toMatchObject({
      includes: { categories: true, people: true, analyses: true, statuses: true },
      __jazz_requireIncludes: true,
    });
    const translated = JSON.parse(translateQuery(required._build(), app.wasmSchema));
    expect(translated.array_subqueries).toMatchObject([
      { column_name: "category", outer_column: "records.category_id", inner_column: "id" },
      { column_name: "person", outer_column: "records.personId", inner_column: "id" },
      { column_name: "team", outer_column: "records.team_id", inner_column: "id" },
      { column_name: "teams", outer_column: "records.teamIds", inner_column: "id" },
      {
        column_name: "addressRelation",
        outer_column: "records.address",
        inner_column: "id",
      },
    ]);
    expect(
      translated.array_subqueries.map((include: { requirement?: string }) => include.requirement),
    ).toEqual(["AtLeastOne", "AtLeastOne", "AtLeastOne", "MatchCorrelationCardinality", undefined]);
    expect(
      transformRows(
        [
          {
            id: "record-1",
            values: [
              { type: "Uuid", value: "person-1" },
              {
                type: "Array",
                value: [
                  {
                    type: "Row",
                    value: { id: "person-1", values: [{ type: "Text", value: "Pat" }] },
                  },
                ],
              },
            ],
          },
          { id: "record-2", values: [{ type: "Null" }, { type: "Array", value: [] }] },
        ],
        app.wasmSchema,
        "records",
        { addressRelation: true },
        ["address"],
      ),
    ).toEqual([
      { id: "record-1", address: "person-1", addressRelation: { id: "person-1", name: "Pat" } },
      { id: "record-2", address: null, addressRelation: null },
    ]);
  });

  it("infers reverse includes and both hop directions through declared schema/app types", () => {
    expectTypeOf<s.RowOf<typeof reverse>["recordsViaCategories"]>().toEqualTypeOf<
      s.RowOf<typeof app.records>[]
    >();
    expectTypeOf<s.RowOf<typeof reverse>["teamsViaCategory"]>().toEqualTypeOf<Team[]>();
    const forwardHop = checkedApp.records.hopTo("people");
    const reverseHop = checkedApp.people.hopTo("recordsViaPeople");
    expectTypeOf<s.RowOf<typeof forwardHop>>().toEqualTypeOf<Person>();
    expectTypeOf<s.RowOf<typeof reverseHop>>().toEqualTypeOf<s.RowOf<typeof app.records>>();
    expect(JSON.parse(forwardHop._build()).hops).toEqual(["people"]);
    expect(JSON.parse(reverseHop._build()).hops).toEqual(["recordsViaPeople"]);
    expect(
      JSON.parse(translateQuery(reverse._build(), app.wasmSchema)).array_subqueries,
    ).toMatchObject([
      {
        column_name: "recordsViaCategories",
        table: "records",
        inner_column: "category_ids",
        outer_column: "categories.id",
      },
      {
        column_name: "teamsViaCategory",
        table: "teams",
        inner_column: "category_id",
        outer_column: "categories.id",
      },
    ]);
    expect(checkedApp.wasmSchema).toEqual(app.wasmSchema);
  });

  it("retains only the selected typed relation graph while sharing the full structural schema", () => {
    expectTypeOf<s.RowOf<typeof sliced>["categories"]>().toEqualTypeOf<Label[]>();
    expectTypeOf<s.RowOf<typeof sliced>["person_ids"]>().toEqualTypeOf<string[]>();
    const reverseSlice = slice.categories.include({ recordsViaCategories: true });
    expectTypeOf<s.RowOf<typeof reverseSlice>["recordsViaCategories"]>().toEqualTypeOf<
      s.RowOf<typeof app.records>[]
    >();
    const rawSlice = rawSliceable.slice("records", "categories");
    expect(JSON.parse(rawSlice.records.include({ categories: true })._build())).toEqual(
      JSON.parse(sliced._build()),
    );
    expect(slice.wasmSchema).toBe(checkedSliceable.wasmSchema);
    expect(slice.wasmSchema).toEqual(app.wasmSchema);
    expect(Object.keys(slice.wasmSchema)).toContain("people");
  });
});

if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
  // @ts-expect-error only the declared addressRelation alias is available
  app.records.include({ addresses: true });
  // @ts-expect-error only the declared reverse alias is available
  app.people.include({ recordsViaAddresses: true });
  // @ts-expect-error only the explicitly declared people alias is available
  checkedApp.records.include({ persons: true });
  // @ts-expect-error slices exclude reverse references from unselected source tables
  slice.categories.include({ teamsViaCategory: true });
  // @ts-expect-error slices exclude forward targets outside their selected tables
  slice.records.include({ people: true });
  // @ts-expect-error sliced hops also exclude targets outside their selected tables
  slice.records.hopTo("analyses");
  // @ts-expect-error the declared app preserves the exact table graph
  checkedApp.records.hopTo("persons");
}
