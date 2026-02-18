import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app";

export default definePermissions(app, ({ policy, session }) => [
  policy.todos.allowRead.where({ owner_id: session.user_id }),
  policy.todos.allowInsert.where({ owner_id: session.user_id }),
  policy.todos.allowUpdate
    .whereOld({ owner_id: session.user_id })
    .whereNew({ owner_id: session.user_id }),
  policy.todos.allowDelete.where({ owner_id: session.user_id }),
]);
