import { useAll } from "@jazz/react";
import { app } from "../generated/client.js";

//#region task-list-basic
export function TaskList() {
  const [tasks, loading] = useAll(app.tasks.where({ isCompleted: false }));

  if (loading) {
    return <div>Loading tasks...</div>;
  }

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>
          <h3>{task.title}</h3>
          {task.description && <p>{task.description}</p>}
        </li>
      ))}
    </ul>
  );
}
//#endregion

//#region task-list-with-mutations
export function TaskListWithMutations() {
  const [tasks, loading, mutate] = useAll(
    app.tasks.where({ isCompleted: false }),
  );

  if (loading) return <div>Loading...</div>;

  return (
    <div>
      <button
        onClick={() =>
          mutate.create({
            title: "New Task",
            status: "open",
            priority: "medium",
            project: "project-id",
            createdAt: BigInt(Date.now()),
            updatedAt: BigInt(Date.now()),
            isCompleted: false,
          })
        }
      >
        Add Task
      </button>
      <ul>
        {tasks.map((task) => (
          <li key={task.id}>
            {task.title}
            <button
              onClick={() => mutate.update(task.id, { isCompleted: true })}
            >
              Complete
            </button>
            <button onClick={() => mutate.delete(task.id)}>Delete</button>
          </li>
        ))}
      </ul>
    </div>
  );
}
//#endregion

//#region task-list-with-includes
export function TaskListWithIncludes() {
  const [tasks, loading] = useAll(
    app.tasks.where({ status: "open" }).with({
      project: true,
      assignee: true,
      TaskTags: { tag: true },
    }),
  );

  if (loading) return <div>Loading...</div>;

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>
          <h3>{task.title}</h3>
          <span>Project: {task.project?.name}</span>
          <span>Assignee: {task.assignee?.name ?? "Unassigned"}</span>
          <span>
            Tags: {task.TaskTags?.map((tt) => tt.tag?.name).join(", ")}
          </span>
        </li>
      ))}
    </ul>
  );
}
//#endregion
