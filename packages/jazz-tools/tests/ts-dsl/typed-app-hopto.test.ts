import { describe, expectTypeOf, it } from "vitest";
import { schema as s } from "../../src/index.js";

const schema = {
  org: s.table({
    name: s.string(),
  }),
  todo: s.table({
    title: s.string(),
    completed: s.boolean(),
    org: s.ref("org"),
  }),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

describe("hopTo type inference", () => {
  it("infers the destination table's row type after a forward hop", () => {
    const todoOrg = app.todo.hopTo("org");
    type OrgRow = s.RowOf<typeof todoOrg>;
    expectTypeOf<OrgRow>().toEqualTypeOf<{ id: string; name: string }>();
  });

  it("does not expose destination-table chain methods on the hop result", () => {
    const todoOrg = app.todo.hopTo("org");
    // Until the runtime lowers post-hop clauses to the destination, these
    // chain methods would type-check against destination columns while the
    // runtime applied them to the source. The hop result therefore omits
    // them so callers can't accidentally write conditions that the runtime
    // would silently apply to the wrong table.
    expectTypeOf(todoOrg).not.toHaveProperty("where");
    expectTypeOf(todoOrg).not.toHaveProperty("select");
    expectTypeOf(todoOrg).not.toHaveProperty("include");
    expectTypeOf(todoOrg).not.toHaveProperty("orderBy");
    expectTypeOf(todoOrg).not.toHaveProperty("limit");
    expectTypeOf(todoOrg).not.toHaveProperty("offset");
    expectTypeOf(todoOrg).not.toHaveProperty("requireIncludes");
    expectTypeOf(todoOrg).not.toHaveProperty("hopTo");
  });

  it("still exposes _build / _serializeRelation / gather for hop-then-traverse patterns", () => {
    const todoOrg = app.todo.hopTo("org");
    expectTypeOf(todoOrg._build).toBeFunction();
    expectTypeOf(todoOrg._serializeRelation).toBeFunction();
    expectTypeOf(todoOrg.gather).toBeFunction();
  });
});
