import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    "tasks": {
      "details": m.renameFrom("notes"),
    },
  },
  fromHash: "23b3cde5819a",
  toHash: "a7a14112c289",
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
    "status": s.enum("todo", "doing", "done"),
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
    "details": s.string().optional(),
    "status": s.enum("todo", "doing", "done"),
  }, {
    "project": s.rel("projects", "projectId"),
  })
},
});
