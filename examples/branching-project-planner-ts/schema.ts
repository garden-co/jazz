import { schema as s } from "jazz-tools";

const schema = {
  // Branches are application data. Jazz assigns no lifecycle semantics to them.
  scenarios: s.table(
    {
      name: s.string(),
      base_scenario_id: s.uuid().optional(),
      status: s.enum(["open", "approved", "archived"]),
    },
    {
      base_scenario: s.rel("scenarios", "base_scenario_id"),
      scenariosViaBase_scenario: s.reverse("scenarios", "base_scenario"),
      tasksViaScenario: s.reverse("tasks", "scenario"),
    },
  ),

  // The reference is also the ordinary immutable column that qualifies history.
  tasks: s
    .table(
      {
        scenario_id: s.uuid(),
        title: s.string(),
        estimate: s.int(),
      },
      { scenario: s.rel("scenarios", "scenario_id") },
    )
    .branchBy("scenario_id"),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export type Scenario = s.RowOf<typeof app.scenarios>;
export type Task = s.RowOf<typeof app.tasks>;
