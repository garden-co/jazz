import { useAll, useOne } from "@jazz/react";
import { app } from "../generated/client.js";

// #region use-all-basic
function TaskList() {
  const [tasks, loading] = useAll(app.tasks);

  if (loading) return <div>Loading...</div>;

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}
// #endregion

// #region use-one-basic
function TaskDetail({ taskId }: { taskId: string }) {
  const [task, loading] = useOne(app.tasks, taskId);

  if (loading) return <div>Loading...</div>;
  if (!task) return <div>Not found</div>;

  return <h1>{task.title}</h1>;
}
// #endregion

// #region filtering
function FilteredTasks({ userId }: { userId: string }) {
  // All incomplete tasks
  const [incompleteTasks] = useAll(app.tasks.where({ isCompleted: false }));

  // Tasks assigned to a specific user
  const [myTasks] = useAll(app.tasks.where({ assignee: userId }));

  return (
    <div>
      <h2>Incomplete: {incompleteTasks?.length}</h2>
      <h2>My tasks: {myTasks?.length}</h2>
    </div>
  );
}
// #endregion

// #region with-includes
function TaskListWithAssignee() {
  // Load tasks with their assignee
  const [tasks] = useAll(app.tasks.with({ assignee: true }));

  // Now task.assignee is a full User object, not just an ID
  return (
    <ul>
      {tasks?.map((task) => (
        <li key={task.id}>
          {task.title} - {task.assignee?.name}
        </li>
      ))}
    </ul>
  );
}
// #endregion

// #region chained-where-with
function ChainedQuery() {
  const [tasks] = useAll(
    app.tasks.where({ isCompleted: false }).with({ assignee: true }),
  );

  return (
    <ul>
      {tasks?.map((task) => (
        <li key={task.id}>
          {task.title} - {task.assignee?.name ?? "Unassigned"}
        </li>
      ))}
    </ul>
  );
}
// #endregion

export {
  TaskList,
  TaskDetail,
  FilteredTasks,
  TaskListWithAssignee,
  ChainedQuery,
};
