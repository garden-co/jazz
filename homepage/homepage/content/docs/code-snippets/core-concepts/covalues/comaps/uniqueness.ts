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

// #region Create
// Given the project owner, myTask will have always the same id
Task.create(
  {
    text: "Let's learn some Jazz!",
  },
  {
    unique: "learning-jazz",
    owner: project.$jazz.owner, // Different owner, different id
  },
);
// #endregion

// #region LoadUnique
const learnJazzTask = await Task.loadUnique(
  "learning-jazz",
  project.$jazz.owner.$jazz.id,
);
// #endregion

// #region UpsertUnique
await Task.upsertUnique({
  value: {
    text: "Let's learn some Jazz!",
  },
  unique: "learning-jazz",
  owner: project.$jazz.owner,
});
// #endregion
