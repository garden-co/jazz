import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    "tasks": {
      // Column "status" gains enum variant "done": existing rows are unchanged, so it needs no step.
    },
  },
  fromHash: "8744a9e92b14",
  toHash: "23b3cde5819a",
  from: {
  "projectMembers": s.table({
    "projectId": s.uuid(),
    "user_id": s.uuid(),
  }, {
    "project": s.rel("projects", "projectId"),
  }),
  "projects": s.table({
    "name": s.string(),
    "owner_id": s.uuid(),
  }, {
    "tasks": s.reverse("tasks", "project"),
    "members": s.reverse("projectMembers", "project"),
  }),
  "tasks": s.table({
    "projectId": s.uuid(),
    "title": s.string(),
    "notes": s.string().optional(),
    "status": s.enum("todo", "doing"),
  }, {
    "project": s.rel("projects", "projectId"),
  })
},
  to: {
  "projectMembers": s.table({
    "projectId": s.uuid(),
    "user_id": s.uuid(),
  }, {
    "project": s.rel("projects", "projectId"),
  }),
  "projects": s.table({
    "name": s.string(),
    "owner_id": s.uuid(),
  }, {
    "tasks": s.reverse("tasks", "project"),
    "members": s.reverse("projectMembers", "project"),
  }),
  "tasks": s.table({
    "projectId": s.uuid(),
    "title": s.string(),
    "notes": s.string().optional(),
    "status": s.enum("todo", "doing", "done"),
  }, {
    "project": s.rel("projects", "projectId"),
  })
},
});
