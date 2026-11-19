import { schema as s, migration as m } from "jazz-tools";

export default m.defineMigration({
  migrate: {
    "tasks": {
      "notes": m.add.string({ default: null }),
    },
  },
  fromHash: "2bbf4f7546b9",
  toHash: "8744a9e92b14",
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
    "status": s.enum("todo", "doing"),
  }, {
    "project": s.rel("projects", "projectId"),
  })
},
});
