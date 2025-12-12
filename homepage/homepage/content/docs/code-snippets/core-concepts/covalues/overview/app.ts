// #region Basic
import { co } from "jazz-tools";
import { TodoProject, ListOfTasks } from "./schema";

const project = TodoProject.create(
  {
    title: "New Project",
    tasks: ListOfTasks.create([], co.group().create()),
  },
  co.group().create(),
);
// #endregion

// #region ImplicitPublic
const group = co.group().create().makePublic();
const publicProject = TodoProject.create(
  {
    title: "New Project",
    tasks: [], // Permissions are inherited, so the tasks list will also be public
  },
  group,
);
// #endregion
