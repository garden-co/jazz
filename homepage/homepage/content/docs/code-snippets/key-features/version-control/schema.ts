import { co, z } from "jazz-tools";

export const Task = co.map({
  title: z.string(),
});
export const Project = co.map({
  title: z.string(),
  tasks: co.list(Task),
  priority: z.string(),
});

export const MyAccount = co.account({
  root: co.map({
    value: z.string(),
  }),
  profile: co.profile(),
});
