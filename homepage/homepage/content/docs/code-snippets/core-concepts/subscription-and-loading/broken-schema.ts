import { co, z } from "jazz-tools";

export const Task = co.map({
  title: z.string(),
  description: co.plainText().optional(),
  completed: z.boolean().optional(),
});

// #region BrokenSchema
// @ts-expect-error This is intentionally broken
export const Project = co
  .map({
    name: z.string(),
    tasks: co.list(Task),
    get createdBy() {
      // As MyAppAccount refers back to Project, this creates a circular reference that TypeScript cannot resolve.
      return MyAppAccount;
    },
  })
  .resolved({
    createdBy: true,
  });

export const MyAppAccount = co.account({
  profile: co.profile(),
  root: co.map({}),
  // @ts-expect-error This is intentionally broken
  get projects(): co.List<typeof Project> {
    return co.list(Project);
  },
});
// #endregion
