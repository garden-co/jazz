import { useAll } from "jazz-tools/react";
import { app } from "./recipes/collab-list/jazz.generated.js";

// #region collab-subscribe
export function ProjectTasks({ projectId }: { projectId: string }) {
  const {
    data: tasks,
    isLoading,
    error,
    db,
  } = useAll(app.tasks.where({ projectId, done: false }).orderBy("$createdAt", "desc"));

  function addTask(title: string) {
    db.insert(app.tasks, { title, done: false, projectId });
  }

  function completeTask(taskId: string) {
    db.update(app.tasks, taskId, { done: true });
  }

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Something went wrong!</p>;

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>
          <button onClick={() => completeTask(task.id)}>Done</button>
          {task.title}
        </li>
      ))}
    </ul>
  );
}
// #endregion collab-subscribe
