import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => {
  policy.scenarios.allowRead.where({});
  policy.scenarios.allowInsert.where({});
  policy.scenarios.allowUpdate.whereOld({}).whereNew({});
  policy.scenarios.allowDelete.where({});

  policy.tasks.allowRead.where({});
  policy.tasks.allowInsert.where((task) =>
    policy.scenarios.exists.where({ id: task.scenario_id, status: "open" }),
  );
  policy.tasks.allowUpdate
    .whereOld((task) => policy.scenarios.exists.where({ id: task.scenario_id, status: "open" }))
    .whereNew((task) => policy.scenarios.exists.where({ id: task.scenario_id, status: "open" }));
  policy.tasks.allowDelete.where((task) =>
    policy.scenarios.exists.where({ id: task.scenario_id, status: "open" }),
  );
});
