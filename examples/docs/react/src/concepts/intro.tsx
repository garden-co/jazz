import type { WasmDatabaseLike } from "@jazz/client";
import { useAll } from "@jazz/react";
import { app } from "../generated/client.js";

// #region core-idea
function TaskListExample({ db }: { db: WasmDatabaseLike }) {
  // Subscribe to tasks - data is local, updates are instant
  const [tasks] = useAll(app.tasks.where({ isCompleted: false }));

  // Create a task - no API call, instant UI update
  const addTask = () => {
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

  return (
    <div>
      <button onClick={addTask}>Add Task</button>
      <ul>
        {tasks?.map((task) => (
          <li key={task.id}>{task.title}</li>
        ))}
      </ul>
    </div>
  );
}
// #endregion

export { TaskListExample };
