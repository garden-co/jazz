import { co, z } from "jazz-tools";

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
});
type Task = co.loaded<typeof Task>;

const Project = co.map({
  name: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
});
type Project = co.loaded<typeof Project>;

export { Task, Project };
