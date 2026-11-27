import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  projects: s.table({
    name: s.string(),
  }),
  tasks: s.table(
    {
      projectId: s.uuid(),
      title: s.string().encrypted({ equality: true }),
      notes: s.string().optional().encrypted(),
    },
    { project: s.rel("projects", "projectId") },
    { space: "project" },
  ),
});

const { definePermissions, isMember, session } = schema;

export const permissions = definePermissions({
  projects: {
    // isMember: the signed-in user has been added to this project, so they get its key.
    read: (project) => isMember("projects", project.id),
    insert: { "$createdBy.account": session.user },
    update: { $createdBy: session.user },
  },
  tasks: {
    read: (task) => isMember("projects", task.projectId),
    insert: (task) => isMember("projects", task.projectId),
  },
});
