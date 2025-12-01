// #region Basic
import { Group } from "jazz-tools";
import { TodoProject, ListOfTasks } from "./schema";

const project = TodoProject.create(
  {
    title: "New Project",
    tasks: ListOfTasks.create([], Group.create()),
  },
  Group.create(),
);
// #endregion

// #region ImplicitPublic
const group = Group.create().makePublic();
const publicProject = TodoProject.create(
  {
    title: "New Project",
    tasks: [], // Permissions are inherited, so the tasks list will also be public
  },
  group,
);
// #endregion
