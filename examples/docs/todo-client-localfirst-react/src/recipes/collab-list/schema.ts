import { schema as s } from "jazz-tools";

// #region schema
export const schema = s.defineSchema({
  projectMembers: s.table(
    { projectId: s.uuid(), user_id: s.uuid() },
    {
      project: s.rel("projects", "projectId"),
    },
  ),
  projects: s.table(
    { name: s.string() },
    {
      members: s.reverse("projectMembers", "project"),
      tasks: s.reverse("tasks", "project"),
    },
  ),
  tasks: s.table(
    { title: s.string(), done: s.boolean(), assignee_id: s.uuid().optional(), projectId: s.uuid() },
    {
      project: s.rel("projects", "projectId"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions, exists, allowedTo, session, anyOf } = schema;

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  projectMembers: {
    read: (projectMember) =>
      anyOf(projectMember.project.where(createdByMe), { user_id: session.user }),
    insert: (projectMember) => projectMember.project.where(createdByMe),
  },
  projects: {
    read: (project) =>
      anyOf(createdByMe, exists.projectMembers({ projectId: project.id, user_id: session.user })),
    insert: true,
    update: createdByMe,
  },
  tasks: {
    read: (task) => allowedTo.read(task.project),
    insert: (task) => allowedTo.read(task.project),
    update: (task) => allowedTo.read(task.project),
  },
});
// #endregion permissions
