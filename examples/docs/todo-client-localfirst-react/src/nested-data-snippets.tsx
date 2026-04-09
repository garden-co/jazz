import { schema as s } from "jazz-tools";
import { useAll, useDb, useSession } from "jazz-tools/react";

// #region nested-schema
const schema = {
  projects: s.table({
    name: s.string(),
  }),
  tasks: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects"),
  }),
  comments: s.table({
    body: s.string(),
    taskId: s.ref("tasks"),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
// #endregion nested-schema

// #region nested-permissions
s.definePermissions(app, ({ policy, allowedTo, session }) => {
  // Projects: only the creator
  policy.projects.allowRead.where({ $createdBy: session.user_id });
  policy.projects.allowInsert.always();
  policy.projects.allowUpdate.where({ $createdBy: session.user_id });
  policy.projects.allowDelete.where({ $createdBy: session.user_id });

  // Tasks: inherit from project
  policy.tasks.allowRead.where(allowedTo.read("projectId"));
  policy.tasks.allowInsert.where(allowedTo.read("projectId"));
  policy.tasks.allowUpdate.where(allowedTo.update("projectId"));
  policy.tasks.allowDelete.where(allowedTo.delete("projectId"));

  // Comments: inherit from task
  policy.comments.allowRead.where(allowedTo.read("taskId"));
  policy.comments.allowInsert.where(allowedTo.read("taskId"));
  policy.comments.allowUpdate.where({ $createdBy: session.user_id });
  policy.comments.allowDelete.where({ $createdBy: session.user_id });
});
// #endregion nested-permissions

// #region nested-query
export function ProjectTasks({ projectId }: { projectId: string }) {
  const tasks = useAll(app.tasks.where({ projectId }).orderBy("$createdAt", "desc"));

  if (!tasks) return <p>Loading…</p>;

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
    const project = db.insert(app.projects, {
      name: "Website redesign",
    });

    db.insert(app.tasks, {
      title: "Design homepage",
      done: false,
      projectId: project.id,
    });
  }

  return <button onClick={handleCreate}>New project</button>;
}
// #endregion nested-insert
