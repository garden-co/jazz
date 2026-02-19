import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, either, both, allowedTo, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
  policy.todos.allowInsert.where({ owner_id: session.user_id }),
  policy.todos.allowUpdate
    .whereOld(both({ owner_id: session.user_id }).and({ done: false }))
    .whereNew({ owner_id: session.user_id }),
  policy.todos.allowDelete.where({ owner_id: session.user_id }),

  policy.todos.allowRead.where(either({ done: false }).or(allowedTo.read("project"))),
  policy.todos.allowUpdate
    .whereOld(both(allowedTo.update("project")).and({ done: false }))
    .whereNew(allowedTo.update("project")),
]);
