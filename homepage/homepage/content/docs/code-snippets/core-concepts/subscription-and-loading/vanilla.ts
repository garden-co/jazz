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

// In case the project isn't loaded as required, TypeScript will warn
function TaskList({ project }: { project: ProjectWithTasks }) {
  // TypeScript knows tasks are loaded, so this is type-safe
  const taskElements = project.tasks.map(task => {
    const taskEl = document.createElement('li');
    taskEl.id = task.$jazz.id;
    taskEl.innerText = task.title;
    return taskEl;
  })
  document.querySelector('ul#task-list')?.replaceChildren(...taskElements);
}