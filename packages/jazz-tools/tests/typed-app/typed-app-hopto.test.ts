import { describe, expectTypeOf, it } from "vitest";
import type { RowOf } from "../../src/index.js";
import { app, type Org } from "./apps/hopto/jazz.generated.js";

describe("hopTo type inference", () => {
  it("infers the destination table's row type after a forward hop", () => {
    const todoOrg = app.todo.hopTo("orgRelation");
    type OrgRow = RowOf<typeof todoOrg>;
    expectTypeOf<OrgRow>().toEqualTypeOf<{ id: string; name: string; slug: string }>();
    expectTypeOf<OrgRow>().toMatchTypeOf<Org>();
  });

  it("uses the destination table's where and select types after a forward hop", () => {
    const todoOrg = app.todo.hopTo("orgRelation").where({ slug: "core-team" }).select("name");
    type SelectedOrgRow = RowOf<typeof todoOrg>;
    expectTypeOf<SelectedOrgRow>().toEqualTypeOf<{ id: string; name: string }>();

    if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
      // @ts-expect-error source-table columns are not filterable after hopTo
      app.todo.hopTo("orgRelation").where({ title: "source title" });

      // @ts-expect-error source-table columns are not selectable after hopTo
      app.todo.hopTo("orgRelation").select("completed");
    }
  });
});
