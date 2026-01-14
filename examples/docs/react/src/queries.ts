import type { WasmDatabaseLike } from "@jazz/client";
import { app } from "./generated/client.js";

// This file shows non-React query patterns

//#region query-builder-basic
// Query builder pattern - build queries without executing
const activeTasks = app.tasks.where({ isCompleted: false });

// Add more conditions
const highPriorityActive = app.tasks.where({
  isCompleted: false,
  priority: "high",
});

// Include related data
const tasksWithProject = app.tasks
  .where({ status: "open" })
  .with({ project: true });
//#endregion

//#region subscribe-all
function subscribeToTasks(db: WasmDatabaseLike) {
  // Subscribe to query results
  const unsubscribe = app.tasks
    .where({ isCompleted: false })
    .subscribeAll(db, (tasks) => {
      console.log("Active tasks:", tasks);
    });

  // Clean up when done
  return unsubscribe;
}
//#endregion

//#region subscribe-one
function subscribeToTask(db: WasmDatabaseLike, taskId: string) {
  // Subscribe to a single row
  const unsubscribe = app.tasks.subscribe(db, taskId, (task) => {
    if (task) {
      console.log("Task updated:", task.title);
    } else {
      console.log("Task not found or deleted");
    }
  });

  return unsubscribe;
}
//#endregion

//#region filtering-examples
// Simple equality
const openTasks = app.tasks.where({ status: "open" });

// Multiple conditions (AND)
const urgentOpenTasks = app.tasks.where({
  status: "open",
  priority: "high",
});

// Filter by reference
function getProjectTasks(projectId: string) {
  return app.tasks.where({ project: projectId });
}

// Filter by related data existence
function getUserAssignedTasks(userId: string) {
  return app.tasks.where({ assignee: userId });
}
//#endregion

//#region includes-examples
// Include single reference
const tasksWithProjects = app.tasks.with({ project: true });

// Include multiple references
const tasksWithDetails = app.tasks.with({
  project: true,
  assignee: true,
});

// Include nested references
const tasksWithFullDetails = app.tasks.with({
  project: { owner: true },
  assignee: true,
  TaskTags: { tag: true },
  Comments: { author: true },
});
//#endregion

export {
  activeTasks,
  highPriorityActive,
  tasksWithProject,
  subscribeToTasks,
  subscribeToTask,
  openTasks,
  urgentOpenTasks,
  getProjectTasks,
  getUserAssignedTasks,
  tasksWithProjects,
  tasksWithDetails,
  tasksWithFullDetails,
};
