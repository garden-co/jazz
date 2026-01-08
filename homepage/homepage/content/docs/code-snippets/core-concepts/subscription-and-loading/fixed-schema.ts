import { co, z } from "jazz-tools";

// #region FixedSchema
export const Task = co.map({
  title: z.string(),
  description: co.plainText().optional(),
  completed: z.boolean().optional(),
  get project() {
    // Use a shallowly loaded project reference here
    return ShallowProject;
  },
});

// Which you create here
export const ShallowProject = co.map({
  name: z.string(),
  tasks: co.list(Task),
});

// And then resolve separately here
export const Project = ShallowProject.resolved({
  tasks: true,
});
// #endregion
