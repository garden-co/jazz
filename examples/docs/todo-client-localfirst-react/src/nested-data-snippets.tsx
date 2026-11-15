import { useAll, useDb, useSession } from "jazz-tools/react";
import { app } from "./recipes/nested-data/jazz.generated.js";

// #region nested-query
export function ProjectTasks({ projectId }: { projectId: string }) {
  const {
    data: tasks,
    isLoading,
    error,
  } = useAll(app.tasks.where({ projectId }).orderBy("$createdAt", "desc"));

  if (isLoading) return <p>Loading…</p>;
  if (error) return <p>Something went wrong!</p>;

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}
// #endregion nested-query

// #region nested-insert
export function CreateProject() {
  const db = useDb();
  const session = useSession();

  async function handleCreate() {
    const { value: project } = await db.insert(app.projects, {
      name: "Website redesign",
    });

    await db.insert(app.tasks, {
      title: "Design homepage",
      done: false,
      projectId: project.id,
    });
  }

  return <button onClick={handleCreate}>New project</button>;
}
// #endregion nested-insert
