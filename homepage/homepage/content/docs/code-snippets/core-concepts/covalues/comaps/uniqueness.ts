import { co, z, Group, ID } from "jazz-tools";
const Task = co.map({
  text: z.string(),
});
const Project = co.map({});
const project = Project.create({});

// #region FailLoading
// This will not work as `learning-jazz` is not a CoValue ID
const myTask = await Task.load("learning-jazz");
// #endregion

// #region GetOrCreateUnique
await Task.getOrCreateUnique({
  value: {
    text: "Let's learn some Jazz!",
  },
  unique: "learning-jazz",
  owner: project.$jazz.owner,
});
// #endregion
