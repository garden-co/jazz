import { co, z } from "jazz-tools";

// #region BrokenSchema
export const Task = co.map({
  title: z.string(),
  description: co.plainText().optional(),
  completed: z.boolean().optional(),
  // @ts-expect-error This is intentionally broken
  get project() {
    return Project;
  },
});

// @ts-expect-error This is intentionally broken
export const Project = co
  .map({
    name: z.string(),
    tasks: co.list(Task),
  })
  // @ts-expect-error This is intentionally broken
  .resolved({
    tasks: true,
  });
// #endregion
