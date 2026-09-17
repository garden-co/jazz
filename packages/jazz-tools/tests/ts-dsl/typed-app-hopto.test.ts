import { describe, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";

const schema = {
  org: s.table(
    {
      name: s.string(),
      slug: s.string(),
    },
    { todoViaOrg: s.reverse("todo", "orgRelation") },
  ),
  todo: s.table(
    {
      title: s.string(),
      completed: s.boolean(),
      org: s.uuid(),
    },
    { orgRelation: s.rel("org", "org") },
  ),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

describe("hopTo type inference", () => {
  it("infers the destination table's row type after a forward hop", () => {
    const todoOrg = app.todo.hopTo("orgRelation");
    type OrgRow = s.RowOf<typeof todoOrg>;
    expectTypeOf<OrgRow>().toEqualTypeOf<{ id: string; name: string; slug: string }>();
  });

  it("uses the destination table's where and select types after a forward hop", () => {
    const todoOrg = app.todo.hopTo("orgRelation").where({ slug: "core-team" }).select("name");
    type SelectedOrgRow = s.RowOf<typeof todoOrg>;
    expectTypeOf<SelectedOrgRow>().toEqualTypeOf<{ id: string; name: string }>();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error source-table columns are not filterable after hopTo
      app.todo.hopTo("orgRelation").where({ title: "source title" });

      // @ts-expect-error source-table columns are not selectable after hopTo
      app.todo.hopTo("orgRelation").select("completed");
    }
  });
});
