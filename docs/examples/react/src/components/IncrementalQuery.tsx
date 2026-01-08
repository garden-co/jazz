import { useAll } from "@jazz/react";
import { app } from "../generated/client.js";

//#region incremental-query-hook
export function useFilteredTasks(completed: boolean) {
  // Subscribe to query - automatically updates when tasks change
  const [tasks, loading] = useAll(
    app.tasks.where({ isCompleted: completed })
  );

  return { tasks, loading };
}
//#endregion

//#region incremental-query-component
export function ActiveTasks() {
  const { tasks, loading } = useFilteredTasks(false);

  if (loading) return <div>Loading...</div>;

  return (
    <ul>
      {tasks.map(task => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}
//#endregion
