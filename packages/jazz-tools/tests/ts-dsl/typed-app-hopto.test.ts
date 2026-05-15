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
});
