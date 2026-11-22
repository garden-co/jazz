import { schema as s } from "jazz-tools";
export const schema = s.defineSchema({
  projects: s.table({ name: s.string(), owner_id: s.uuid() }, { tasks: s.reverse("tasks", "project"), members: s.reverse("projectMembers", "project") }),
  projectMembers: s.table({ projectId: s.uuid(), user_id: s.uuid() }, { project: s.rel("projects", "projectId") }),
  tasks: s.table({ projectId: s.uuid(), title: s.string(), details: s.string().optional(), status: s.enum("todo", "doing", "done") }, { project: s.rel("projects", "projectId") }),
});

const { definePermissions, exists, session, anyOf } = schema;

const ownerIdIsMe = { owner_id: session.user };

const isOwnerId = (projectId: s.Uuid) => exists.projects({ id: projectId, owner_id: session.user });

const rule1 = (projectId: s.Uuid) =>
  anyOf(isOwnerId(projectId), exists.projectMembers({ projectId, user_id: session.user }));

export const permissions = definePermissions({
  projectMembers: {
    read: (projectMember) => rule1(projectMember.projectId),
    insert: (projectMember) => isOwnerId(projectMember.projectId),
    update: false,
    delete: false,
  },
  projects: {
    read: (project) => rule1(project.id),
    insert: ownerIdIsMe,
    update: ownerIdIsMe,
    delete: false,
  },
  tasks: {
    read: (task) => rule1(task.projectId),
    insert: (task) => rule1(task.projectId),
    update: (task) => rule1(task.projectId),
    delete: false,
  },
});
