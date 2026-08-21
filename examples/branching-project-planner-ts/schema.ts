import { schema as s } from "jazz-tools";

const schema = {
  // Branches are application data. Jazz assigns no lifecycle semantics to them.
  scenarios: s.table({
    name: s.string(),
    base_scenario_id: s.ref("scenarios").optional(),
    status: s.enum(["open", "approved", "archived"]),
  }),

  // The reference is also the ordinary immutable column that qualifies history.
  tasks: s
    .table({
      scenario_id: s.ref("scenarios"),
      title: s.string(),
      estimate: s.int(),
    })
    .branchBy("scenario_id"),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Scenario = s.RowOf<typeof app.scenarios>;
export type Task = s.RowOf<typeof app.tasks>;
