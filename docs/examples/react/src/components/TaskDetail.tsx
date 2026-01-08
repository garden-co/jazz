import { useOne } from "@jazz/react";
import { app } from "../generated/client.js";

//#region task-detail
interface TaskDetailProps {
  taskId: string;
}

export function TaskDetail({ taskId }: TaskDetailProps) {
  const [task, loading, mutate] = useOne(app.tasks, taskId);

  if (loading) {
    return <div>Loading task...</div>;
  }

  if (!task) {
    return <div>Task not found</div>;
  }

  return (
    <article>
      <h2>{task.title}</h2>
      {task.description && <p>{task.description}</p>}
      <div>
        Status: {task.isCompleted ? "Completed" : "Active"}
      </div>
      <button onClick={() => mutate.update({ isCompleted: !task.isCompleted })}>
        Toggle Complete
      </button>
      <button onClick={() => mutate.delete()}>
        Delete Task
      </button>
    </article>
  );
}
//#endregion

//#region task-detail-with-includes
export function TaskDetailWithIncludes({ taskId }: TaskDetailProps) {
  const [task, loading] = useOne(
    app.tasks.with({
      project: { owner: true },
      assignee: true,
      Comments: { author: true }
    }),
    taskId
  );

  if (loading) return <div>Loading...</div>;
  if (!task) return <div>Task not found</div>;

  return (
    <article>
      <h2>{task.title}</h2>
      <p>Project: {task.project?.name} (by {task.project?.owner?.name})</p>
      <p>Assigned to: {task.assignee?.name ?? "Unassigned"}</p>

      <h3>Comments</h3>
      <ul>
        {task.Comments?.map(comment => (
          <li key={comment.id}>
            <strong>{comment.author?.name}:</strong> {comment.content}
          </li>
        ))}
      </ul>
    </article>
  );
}
//#endregion
