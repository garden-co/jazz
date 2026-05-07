// permissions.ts
import { schema as s2 } from "jazz-tools";

// schema.ts
import { schema as s } from "jazz-tools";
var schema = {
  projects: s.table({
    name: s.string()
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
    owner_id: s.string(),
    parentId: s.ref("todos").optional(),
    projectId: s.ref("projects").optional()
  })
};
var app = s.defineApp(schema);

// permissions.ts
var permissions_default = s2.definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({});
  policy.todos.allowInsert.where({ owner_id: session.user_id });
  policy.todos.allowUpdate.whereOld({ owner_id: session.user_id }).whereNew({ owner_id: session.user_id });
  policy.todos.allowDelete.where({ owner_id: session.user_id });
});
export {
  permissions_default as default
};
