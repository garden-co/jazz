import type { WasmDatabaseLike } from "@jazz/client";
import { app } from "./generated/client.js";

//#region create-task
function createTask(db: WasmDatabaseLike, projectId: string) {
  const taskId = app.tasks.create(db, {
    title: "Complete project documentation",
    description: "Write comprehensive docs for all modules",
    status: "open",
    priority: "high",
    project: projectId,
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });

  console.log("Created task with ID:", taskId);
  return taskId;
}
//#endregion

//#region update-task
function updateTask(db: WasmDatabaseLike, taskId: string) {
  // Update single field
  app.tasks.update(db, taskId, {
    isCompleted: true,
  });

  // Update multiple fields
  app.tasks.update(db, taskId, {
    title: "Updated title",
    description: "Updated description",
    updatedAt: BigInt(Date.now()),
  });
}
//#endregion

//#region delete-task
function deleteTask(db: WasmDatabaseLike, taskId: string) {
  app.tasks.delete(db, taskId);
}
//#endregion

//#region create-related-data
function createProjectWithTasks(db: WasmDatabaseLike, ownerId: string) {
  // Create a project first
  const projectId = app.projects.create(db, {
    name: "My Project",
    color: "blue",
    owner: ownerId,
  });

  // Create a task referencing the project
  const taskId = app.tasks.create(db, {
    title: "First task",
    status: "open",
    priority: "medium",
    project: projectId, // Reference the project
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });

  // Create a tag
  const tagId = app.tags.create(db, {
    name: "bug",
    color: "red",
  });

  // Link task and tag (many-to-many via join table)
  app.tasktags.create(db, {
    task: taskId,
    tag: tagId,
  });

  return { projectId, taskId, tagId };
}
//#endregion

export { createTask, updateTask, deleteTask, createProjectWithTasks };
