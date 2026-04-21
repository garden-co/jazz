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
    assignee_id: s.string().optional(),
    projectId: s.ref("projects"),
  }),
  projectMembers: s.table({
    projectId: s.ref("projects"),
    user_id: s.string(),
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
      { $createdBy: session.user_id },
      policy.projectMembers.exists.where({
        projectId: project.id,
        user_id: session.user_id,
      }),
    ]),
  );
  policy.projects.allowInsert.always();
  policy.projects.allowUpdate.where({ $createdBy: session.user_id });

  // Tasks: inherit from project
  policy.tasks.allowRead.where(allowedTo.read("projectId"));
  policy.tasks.allowInsert.where(allowedTo.read("projectId"));
  policy.tasks.allowUpdate.where(allowedTo.read("projectId"));

  // Members: only the creator can manage
  policy.projectMembers.allowInsert.where((member) =>
    policy.projects.exists.where({
      id: member.projectId,
      $createdBy: session.user_id,
    }),
  );
  policy.projectMembers.allowRead.where((member) =>
    anyOf([
      policy.projects.exists.where({
        id: member.projectId,
        $createdBy: session.user_id,
      }),
      { user_id: session.user_id },
    ]),
  );
});
// #endregion collab-permissions

// #region collab-subscribe
export function ProjectTasks({ projectId }: { projectId: string }) {
  const db = useDb();
  const tasks = useAll(app.tasks.where({ projectId, done: false }).orderBy("$createdAt", "desc"));

  function addTask(title: string) {
    db.insert(app.tasks, { title, done: false, projectId });
  }

  function completeTask(taskId: string) {
    db.update(app.tasks, taskId, { done: true });
  }

  if (!tasks) return <p>Loading…</p>;

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
