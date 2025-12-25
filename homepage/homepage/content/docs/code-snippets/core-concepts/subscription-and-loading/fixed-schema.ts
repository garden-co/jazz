import { co, z } from "jazz-tools";

export const Task = co.map({
  title: z.string(),
  description: co.plainText().optional(),
  completed: z.boolean().optional(),
});

// #region FixedSchema
// Don't resolve the circular reference here...
const ShallowProject = co.map({
  name: z.string(),
  tasks: co.list(Task),
  get createdBy() {
    return MyAppAccount;
  },
});

// Do it separately here
export const Project = ShallowProject.resolved({
  createdBy: true,
});

export const MyAppAccount = co.account({
  profile: co.profile(),
  root: co.map({}),
  // And use the shallowly loaded project here too
  get projects(): co.List<typeof ShallowProject> {
    return co.list(ShallowProject);
  },
});
// #endregion
