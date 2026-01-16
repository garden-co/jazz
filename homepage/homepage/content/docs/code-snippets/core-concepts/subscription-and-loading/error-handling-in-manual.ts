import { Task, Project } from "./schema";
const taskId = "";

// #region ErrorHandling
const unsubscribe = Task.subscribe(taskId, {
  onError: (err) => console.error("Can't access the task data, error: ", err.$jazz.loadingState),
}, (updatedTask) => {
  console.log("Updated task:", updatedTask);
});

// Always clean up when finished
unsubscribe();
// #endregion