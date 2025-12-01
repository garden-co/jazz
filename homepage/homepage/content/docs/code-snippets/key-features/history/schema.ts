import { co, z } from "jazz-tools";
// #region Basic
export const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "completed"]),
});
export type Task = co.loaded<typeof Task>;
// #endregion
