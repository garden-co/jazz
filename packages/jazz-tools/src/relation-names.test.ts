import { describe, expect, expectTypeOf, it } from "vitest";
import {
  forwardRelationName,
  relationInflectionWords,
  type ForwardRelationName,
} from "./relation-names.js";
import { schema as s } from "./index.js";
import { analyzeRelations } from "./codegen/relation-analyzer.js";
import { translateQuery } from "./runtime/query-adapter.js";
import { definePermissions } from "./permissions/index.js";

const corpus = {
  personIds: "people",
  peopleIds: "people",
  child_ids: "children",
  childrenIds: "children",
  categoryIds: "categories",
  categoriesIds: "categories",
  statusIds: "statuses",
  statusesIds: "statuses",
  analysisIds: "analyses",
  analysesIds: "analyses",
  equipmentIds: "equipment",
  newsIds: "news",
  ownerPersonIds: "ownerPeople",
  owner_person_ids: "owner_people",
  PersonIds: "People",
  PERSON_ids: "PEOPLE",
  CATEGORY_ids: "CATEGORIES",
  CategoryIds: "Categories",
  ownerCHILDIds: "ownerCHILDREN",
  mouseIds: "mice",
  gooseIds: "geese",
  toothIds: "teeth",
  footIds: "feet",
  aliasIds: "aliases",
  busIds: "buses",
  informationIds: "information",
  softwareIds: "software",
  dataIds: "data",
  mediaIds: "media",
  "category\nIds": "category\ns",
  "box\nIds": "box\ns",
  seriesIds: "series",
  speciesIds: "species",
  fishIds: "fish",
  sheepIds: "sheep",
  boxIds: "boxes",
  classIds: "classes",
  dishIds: "dishes",
  churchIds: "churches",
  buzzIds: "buzzes",
  boyIds: "boys",
  keyIds: "keys",
  toyIds: "toys",
  queryIds: "queries",
  KyIds: "Kies",
  ownerIds: "owners",
  ownersIds: "owners",
  status: "status",
  analysis: "analysis",
  child: "child",
  personId: "person",
  person_id: "person",
  Ids: "",
  _ids: "",
  Id: "",
  _id: "",
  "": "",
  xIds: "xes",
  yIds: "ys",
  YIds: "YS",
  constructorIds: "constructors",
  toStringIds: "toStrings",
  PeRsOnIds: "PeRsOns",
  éIds: "és",
  "😀Ids": "😀S",
} as const;
type Expected = { [K in keyof typeof corpus]: ForwardRelationName<K> };
expectTypeOf<typeof corpus>().toMatchTypeOf<Expected>();
expectTypeOf<Expected>().toMatchTypeOf<typeof corpus>();
expectTypeOf<ForwardRelationName<string>>().toEqualTypeOf<string>();
expectTypeOf<ForwardRelationName<"personIds" | "categoryIds">>().toEqualTypeOf<
  "people" | "categories"
>();

describe("bounded relation names", () => {
  it("keeps dictionary suffixes unambiguous", () => {
    const keys = Object.keys(relationInflectionWords);
    for (const key of keys) {
      expect(keys.filter((candidate) => key.endsWith(candidate))).toEqual([key]);
    }
  });

  it.each(Object.entries(corpus))("maps %s to %s at runtime and in types", (input, expected) => {
    expect(forwardRelationName(input)).toBe(expected);
  });

  it("uses the same aliases for includes, reverse relations, and permission hops", () => {
    const app = s.defineApp({
      people: s.table({ name: s.string() }),
      groups: s.table({ personIds: s.array(s.ref("people")) }),
    });
    expect(JSON.parse(app.groups.include({ people: true })._build()).includes).toEqual({
      people: true,
    });
    expect(JSON.parse(app.people.include({ groupsViaPeople: true })._build()).includes).toEqual({
      groupsViaPeople: true,
    });
    const relations = analyzeRelations(app.wasmSchema);
    expect(relations.get("groups")).toMatchObject([
      { name: "people", fromColumn: "personIds", isArray: true },
    ]);
    expect(relations.get("people")).toMatchObject([
      { name: "groupsViaPeople", toColumn: "personIds" },
    ]);
    const permissions = definePermissions(app, ({ policy }) => [
      policy.groups.allowRead.where(policy.exists(policy.groups.hopTo("people"))),
      policy.people.allowRead.where(policy.exists(policy.people.hopTo("groupsViaPeople"))),
    ]);
    expect(permissions.groups!.select?.using).toMatchObject({
      type: "ExistsRel",
      rel: {
        Project: {
          input: {
            Join: {
              left: { TableScan: { table: "groups" } },
              right: { TableScan: { table: "people", alias: "__hop_0" } },
              on: [
                {
                  left: { scope: "groups", column: "personIds" },
                  right: { scope: "__hop_0", column: "id" },
                },
              ],
            },
          },
        },
      },
    });
    expect(permissions.people!.select?.using).toMatchObject({
      type: "ExistsRel",
      rel: {
        Project: {
          input: {
            Join: {
              left: { TableScan: { table: "people" } },
              right: { TableScan: { table: "groups", alias: "__hop_0" } },
              on: [
                {
                  left: { scope: "people", column: "id" },
                  right: { scope: "__hop_0", column: "personIds" },
                },
              ],
            },
          },
        },
      },
    });
    expect(
      JSON.parse(translateQuery(app.groups.include({ people: true })._build(), app.wasmSchema))
        .array_subqueries,
    ).toMatchObject([
      {
        column_name: "people",
        table: "people",
        inner_column: "id",
        outer_column: "groups.personIds",
      },
    ]);
    expect(
      JSON.parse(
        translateQuery(app.people.include({ groupsViaPeople: true })._build(), app.wasmSchema),
      ).array_subqueries,
    ).toMatchObject([
      {
        column_name: "groupsViaPeople",
        table: "groups",
        inner_column: "personIds",
        outer_column: "people.id",
      },
    ]);
    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error the old type-only misspelling is not a relation
      app.groups.include({ persons: true });
      // @ts-expect-error reverse names follow the same convention
      app.people.include({ groupsViaPersons: true });
    }
  });

  it("rejects normalized aliases that collide with a column or another relation", () => {
    expect(() =>
      s.defineApp({
        people: s.table({ name: s.string() }),
        groups: s.table({ personIds: s.array(s.ref("people")), people: s.string() }),
      }),
    ).toThrow(/relation name "people".*collides/);
    expect(() =>
      s.defineApp({
        people: s.table({ name: s.string() }),
        groups: s.table({
          personIds: s.array(s.ref("people")),
          peopleIds: s.array(s.ref("people")),
        }),
      }),
    ).toThrow(/relation name "people" is ambiguous/);
  });
});
