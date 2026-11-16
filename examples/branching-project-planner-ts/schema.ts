import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  // Branches are application data. Jazz assigns no lifecycle semantics to them.
  scenarios: s.table(
    {
      name: s.string(),
      base_scenario_id: s.uuid().optional(),
      base_snapshot_ref: s.string().optional(),
      status: s.enum("open", "approved", "archived"),
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
});

const { definePermissions } = schema;

export const permissions = definePermissions({
  scenarios: {
    read: true,
    insert: true,
    update: true,
    delete: true,
  },
  tasks: {
    read: true,
    insert: (task) => task.scenario.where({ status: "open" }),
    update: (task) => task.scenario.where({ status: "open" }),
    delete: (task) => task.scenario.where({ status: "open" }),
  },
});
