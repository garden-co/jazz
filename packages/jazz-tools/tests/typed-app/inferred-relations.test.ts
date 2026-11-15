import { describe, expect, expectTypeOf, it } from "vitest";
import type { RowOf } from "../../src/index.js";
import { transformRows } from "../../src/runtime/row-transformer.js";
import { translateQuery } from "../../src/runtime/query-adapter.js";
import { app, type Entry } from "./apps/inferred-relations/jazz.generated.js";

// FILES-AS-SOURCE: the table was `records`; the codegen refuses that name (its type `Record`
// clashes with a name the generated module uses), so it is `entries` here.
const arrays = app.entries
  .include({ categories: true, people: true, analyses: true, statuses: true })
  .requireIncludes();
const scalars = app.entries.include({
  category: true,
  person: true,
  team: true,
  teams: true,
  addressRelation: true,
});
const required = scalars.requireIncludes();
const reverse = app.categories.include({ entriesViaCategories: true, teamsViaCategory: true });
type Label = { id: string; label: string };
type Person = { id: string; name: string };
type Team = { id: string; category_id: string };

describe("explicit public relation APIs", () => {
  it("preserves array results, scalar nullability, and required-include refinement", () => {
    expectTypeOf<RowOf<typeof arrays>["categories"]>().toEqualTypeOf<Label[]>();
    expectTypeOf<RowOf<typeof arrays>["people"]>().toEqualTypeOf<Person[]>();
    expectTypeOf<RowOf<typeof arrays>["analyses"]>().toEqualTypeOf<
      Array<{ id: string; summary: string }>
    >();
    expectTypeOf<RowOf<typeof arrays>["statuses"]>().toEqualTypeOf<Label[]>();
    expectTypeOf<RowOf<typeof scalars>["category"]>().toEqualTypeOf<Label | null>();
    expectTypeOf<RowOf<typeof scalars>["person"]>().toEqualTypeOf<Person | null>();
    expectTypeOf<RowOf<typeof scalars>["team"]>().toEqualTypeOf<Team | null>();
    expectTypeOf<RowOf<typeof required>["category"]>().toEqualTypeOf<Label>();
    expectTypeOf<RowOf<typeof required>["person"]>().toEqualTypeOf<Person>();
    expectTypeOf<RowOf<typeof required>["team"]>().toEqualTypeOf<Team>();
    expectTypeOf<RowOf<typeof required>["teams"]>().toEqualTypeOf<Team[]>();
    // A nullable FK remains nullable even when matching includes are required.
    expectTypeOf<RowOf<typeof required>["addressRelation"]>().toEqualTypeOf<Person | null>();
    expectTypeOf<RowOf<typeof required>["personId"]>().toEqualTypeOf<string>();
    expectTypeOf<RowOf<typeof arrays>["person_ids"]>().toEqualTypeOf<string[]>();
    expect(JSON.parse(arrays._build())).toMatchObject({
      includes: { categories: true, people: true, analyses: true, statuses: true },
      __jazz_requireIncludes: true,
    });
    const translated = JSON.parse(translateQuery(required._build(), app.wasmSchema));
    expect(translated.array_subqueries).toMatchObject([
      { column_name: "category", outer_column: "entries.category_id", inner_column: "id" },
      { column_name: "person", outer_column: "entries.personId", inner_column: "id" },
      { column_name: "team", outer_column: "entries.team_id", inner_column: "id" },
      { column_name: "teams", outer_column: "entries.teamIds", inner_column: "id" },
      {
        column_name: "addressRelation",
        outer_column: "entries.address",
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
        "entries",
        { addressRelation: true },
        ["address"],
      ),
    ).toEqual([
      { id: "record-1", address: "person-1", addressRelation: { id: "person-1", name: "Pat" } },
      { id: "record-2", address: null, addressRelation: null },
    ]);
  });

  it("infers reverse includes and both hop directions through declared schema/app types", () => {
    expectTypeOf<RowOf<typeof reverse>["entriesViaCategories"]>().toEqualTypeOf<
      RowOf<typeof app.entries>[]
    >();
    expectTypeOf<RowOf<typeof reverse>["teamsViaCategory"]>().toEqualTypeOf<Team[]>();
    expectTypeOf<RowOf<typeof app.entries>>().toMatchTypeOf<Entry>();
    const forwardHop = app.entries.hopTo("people");
    const reverseHop = app.people.hopTo("entriesViaPeople");
    expectTypeOf<RowOf<typeof forwardHop>>().toEqualTypeOf<Person>();
    expectTypeOf<RowOf<typeof reverseHop>>().toEqualTypeOf<RowOf<typeof app.entries>>();
    expect(JSON.parse(forwardHop._build()).hops).toEqual(["people"]);
    expect(JSON.parse(reverseHop._build()).hops).toEqual(["entriesViaPeople"]);
    expect(
      JSON.parse(translateQuery(reverse._build(), app.wasmSchema)).array_subqueries,
    ).toMatchObject([
      {
        column_name: "entriesViaCategories",
        table: "entries",
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
  });
});

if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
  // @ts-expect-error only the declared addressRelation alias is available
  app.entries.include({ addresses: true });
  // @ts-expect-error only the declared reverse alias is available
  app.people.include({ entriesViaAddresses: true });
  // @ts-expect-error only the explicitly declared people alias is available
  app.entries.include({ persons: true });
  // @ts-expect-error the declared app preserves the exact table graph
  app.entries.hopTo("persons");
}
