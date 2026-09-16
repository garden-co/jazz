import { describe, expect, it } from "vitest";
import { schema as s } from "./index.js";
import { analyzeRelations } from "./codegen/relation-analyzer.js";
import { translateQuery } from "./runtime/query-adapter.js";
import { definePermissions } from "./permissions/index.js";

describe("declared relation names", () => {
  it("uses the same aliases for includes, reverse relations, and permission hops", () => {
    const app = s.defineApp({
      people: s.table({ name: s.string() }, { groupsViaPeople: s.reverse("groups", "people") }),
      groups: s.table({ personIds: s.array(s.uuid()) }, { people: s.rel("people", "personIds") }),
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
      // @ts-expect-error undeclared reverse alias
      app.people.include({ groupsViaPersons: true });
    }
  });
});
