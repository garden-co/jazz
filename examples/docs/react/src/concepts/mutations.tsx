import { useJazz, useOne } from "@jazz/react";
import { app } from "../generated/client.js";

// #region create-task
function CreateTask() {
  const db = useJazz();

  const handleCreate = () => {
    app.tasks.create(db, {
      title: "Buy groceries",
      status: "open",
      priority: "medium",
      project: "01JGXYZ123ABC456DEF789GHIJ",
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });
  };

  return <button onClick={handleCreate}>Add Task</button>;
}
// #endregion

// #region create-returns-id
function createTaskReturnsId() {
  const db = useJazz();

  const taskId = app.tasks.create(db, {
    title: "New task",
    status: "open",
    priority: "medium",
    project: "01JGXYZ123ABC456DEF789GHIJ",
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });
  console.log(taskId); // "01JGXYZ123ABC456DEF789GHIJ"
}
// #endregion

// #region toggle-task
function ToggleTask({ taskId }: { taskId: string }) {
  const db = useJazz();
  const [task] = useOne(app.tasks, taskId);

  const handleToggle = () => {
    app.tasks.update(db, taskId, {
      isCompleted: !task?.isCompleted,
      updatedAt: BigInt(Date.now()),
    });
  };

  return (
    <label>
      <input
        type="checkbox"
        checked={task?.isCompleted}
        onChange={handleToggle}
      />
      {task?.title}
    </label>
  );
}
// #endregion

// #region delete-task
function DeleteTask({ taskId }: { taskId: string }) {
  const db = useJazz();

  const handleDelete = () => {
    app.tasks.delete(db, taskId);
  };

  return <button onClick={handleDelete}>Delete</button>;
}
// #endregion

// #region instant-updates
function instantUpdates() {
  const db = useJazz();

  // This is synchronous - no await needed
  app.tasks.create(db, {
    title: "Task 1",
    status: "open",
    priority: "medium",
    project: "01JGXYZ123ABC456DEF789GHIJ",
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });
  app.tasks.create(db, {
    title: "Task 2",
    status: "open",
    priority: "medium",
    project: "01JGXYZ123ABC456DEF789GHIJ",
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });

  // Both tasks are already in local state
  // UI updates immediately via subscriptions
}
// #endregion

// #region with-references
function withReferences() {
  const db = useJazz();
  const userId = "01JGXYZ123ABC456DEF789GHIJ";

  // Reference by ID
  app.tasks.create(db, {
    title: "Review PR",
    status: "open",
    priority: "high",
    project: "01JGXYZ123ABC456DEF789GHIJ",
    assignee: userId,
    createdAt: BigInt(Date.now()),
    updatedAt: BigInt(Date.now()),
    isCompleted: false,
  });
}
// #endregion

export {
  CreateTask,
  createTaskReturnsId,
  ToggleTask,
  DeleteTask,
  instantUpdates,
  withReferences,
};
