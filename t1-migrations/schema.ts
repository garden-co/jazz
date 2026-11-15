import { schema as s } from "jazz-tools";
import { migrations } from "./migrations/index.js";
const schema = {
  projects: s.table({ name: s.string(), owner_id: s.uuid() }, { tasks: s.reverse("tasks", "project"), members: s.reverse("projectMembers", "project") }),
  projectMembers: s.table({ projectId: s.uuid(), user_id: s.uuid() }, { project: s.rel("projects", "projectId") }),
  tasks: s.table({ projectId: s.uuid(), title: s.string(), details: s.string().optional(), status: s.enum("todo", "doing", "done") }, { project: s.rel("projects", "projectId") }),
};
export const app = s.defineApp(schema, { migrations });
