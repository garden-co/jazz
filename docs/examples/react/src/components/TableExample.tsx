import { useAll, useJazz } from "@jazz/react";
import { app } from "../generated/client.js";

//#region working-with-tables
export function TaskListExample() {
  // Subscribe to all incomplete tasks
  const [tasks, loading] = useAll(app.tasks.where({ isCompleted: false }));

  const db = useJazz();

  const createTask = (projectId: string) => {
    app.tasks.create(db, {
      title: "New task",
      status: "open",
      priority: "medium",
      project: projectId,
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });
  };

  if (loading) return <div>Loading...</div>;

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}
//#endregion
