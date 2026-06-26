import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });

  // Branches are normal rows: you can see/create your own.
  policy.branches.allowRead.where({ owner_id: session.user_id });
  policy.branches.allowInsert.where({ owner_id: session.user_id });

  // Data written inside a branch is governed by these rules. `$branch` exposes
  // the backing branch row, and access is deny-by-default: a session can only
  // touch branch data if it can read the backing branch row AND a rule matches.
  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ owner_id: $branch.owner_id });
    branchPolicy.todos.allowInsert.where({ owner_id: session.user_id });
    branchPolicy.todos.allowUpdate.where({ owner_id: session.user_id });
    branchPolicy.todos.allowDelete.where({ owner_id: session.user_id });
  });
});
