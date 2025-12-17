import { Task, Project } from "./schema";
const taskId = "";

// #region ErrorHandling
const unsubscribe = Task.subscribe(taskId, {
  onUnauthorized: (err) => console.error(err),
  onUnavailable: (err) => console.error(err)
}, (updatedTask) => {
  console.log("Updated task:", updatedTask);
});

// Always clean up when finished
unsubscribe();
// #endregion