import { schema as s } from "jazz-tools";
import { useAll, useDb } from "jazz-tools/react";

// #region collab-schema
const schema = {
  projects: s.table({
    name: s.string(),
  }),
  tasks: s.table({
    title: s.string(),
    done: s.boolean(),
    assignee_id: s.uuid().optional(),
    projectId: s.ref("projects"),
  }),
  projectMembers: s.table({
    projectId: s.ref("projects"),
    user_id: s.uuid(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
// #endregion collab-schema

// #region collab-permissions
s.definePermissions(app, ({ policy, anyOf, allowedTo, session }) => {
  // Projects: creator and members
  policy.projects.allowRead.where((project) =>
    anyOf([
      { "$createdBy.account": session.user.account },
      policy.projectMembers.exists.where({
        projectId: project.id,
        user_id: session.user.account,
      }),
    ]),
  );
  policy.projects.allowInsert.always();
  policy.projects.allowUpdate.where({ "$createdBy.account": session.user.account });

  // Tasks: inherit from project
  policy.tasks.allowRead.where(allowedTo.read("projectId"));
  policy.tasks.allowInsert.where(allowedTo.read("projectId"));
  policy.tasks.allowUpdate.where(allowedTo.read("projectId"));

  // Members: only the creator can manage
  policy.projectMembers.allowInsert.where((member) =>
    policy.projects.exists.where({
      id: member.projectId,
      "$createdBy.account": session.user.account,
    }),
  );
  policy.projectMembers.allowRead.where((member) =>
    anyOf([
      policy.projects.exists.where({
        id: member.projectId,
        "$createdBy.account": session.user.account,
      }),
      { user_id: session.user.account },
    ]),
  );
});
// #endregion collab-permissions

// #region collab-subscribe
export function ProjectTasks({ projectId }: { projectId: string }) {
  const db = useDb();
  const {
    data: tasks,
    isLoading,
    error,
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
