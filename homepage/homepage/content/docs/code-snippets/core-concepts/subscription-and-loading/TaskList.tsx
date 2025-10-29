import { co } from "jazz-tools";
import { Project } from "./schema";

type ProjectWithTasks = co.loaded<
  typeof Project,
  {
    tasks: {
      $each: true;
    };
  }
>;

// In case the project prop isn't loaded as required, TypeScript will warn
function TaskList({ project }: { project: ProjectWithTasks }) {
  // TypeScript knows tasks are loaded, so this is type-safe
  return (
    <ul>
      {project.tasks.map((task) => (
        <li key={task.$jazz.id}>{task.title}</li>
      ))}
    </ul>
  );
}
