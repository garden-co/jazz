// #region schema
import { schema as s } from "jazz-tools";

export const schema = s.defineSchema({
  comments: s.table(
    { body: s.string(), taskId: s.uuid() },
    {
      task: s.rel("tasks", "taskId"),
    },
  ),
  projects: s.table(
    { name: s.string() },
    {
      tasks: s.reverse("tasks", "project"),
    },
  ),
  tasks: s.table(
    { title: s.string(), done: s.boolean(), projectId: s.uuid() },
    {
      comments: s.reverse("comments", "task"),
      project: s.rel("projects", "projectId"),
    },
  ),
});
// #endregion schema

// #region permissions
const { definePermissions, allowedTo, session } = schema;

const createdByMe = { $createdBy: session.user };

export const permissions = definePermissions({
  comments: {
    read: (comment) => allowedTo.read(comment.task),
    insert: (comment) => allowedTo.read(comment.task),
    update: createdByMe,
    delete: createdByMe,
  },
  projects: {
    read: createdByMe,
    insert: true,
    update: createdByMe,
    delete: createdByMe,
  },
  tasks: {
    read: (task) => allowedTo.read(task.project),
    insert: (task) => allowedTo.read(task.project),
    update: (task) => allowedTo.update(task.project),
    delete: (task) => allowedTo.delete(task.project),
  },
});
// #endregion permissions
