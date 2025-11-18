import { co, z } from "jazz-tools";

export const Task = co.map({
  title: z.string(),
  description: z.string().optional(),
  completed: z.boolean().optional(),
});

export const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
});

export const MyAppAccount = co.account({
  profile: co.profile(),
  root: co.map({}),
});
