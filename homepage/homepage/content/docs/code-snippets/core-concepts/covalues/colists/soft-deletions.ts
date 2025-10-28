import { co, z } from "jazz-tools";

// #region SoftDeletion
const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
  deleted: z.optional(z.boolean()), // [!code ++]
});
type Task = typeof Task;

const ListOfTasks = co.list(Task);
type ListOfTasks = typeof ListOfTasks;

export function getCurrentTasks(list: co.loaded<ListOfTasks, { $each: true }>) {
  return list.filter((task): task is co.loaded<Task> => !task.deleted);
}

async function main() {
  const myTaskList = ListOfTasks.create([]);
  myTaskList.$jazz.push({
    title: "Tomatoes",
    status: "todo",
    deleted: false,
  });
  myTaskList.$jazz.push({
    title: "Cucumbers",
    status: "todo",
    deleted: true,
  });
  myTaskList.$jazz.push({
    title: "Carrots",
    status: "todo",
  });

  const activeTasks = getCurrentTasks(myTaskList);
  console.log(activeTasks.map((task) => task.title));
  // Output: ["Tomatoes", "Carrots"]
}
// #endregion
