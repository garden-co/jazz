import { Task, Project } from "./schema";
const taskId = "";

// #region ErrorHandling
const unsubscribe = Task.subscribe(taskId, (task) => {
  if (!task.$isLoaded) {
    // Handle error states
    console.error("Error loading task:", task.$jazz.loadingState);
    return;
  }
  console.log("Updated task:", task);
});

// Always clean up when finished
unsubscribe();
// #endregion