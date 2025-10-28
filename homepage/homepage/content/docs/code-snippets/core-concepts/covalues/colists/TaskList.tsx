// [!code hide:4]
const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});
import { co, z } from "jazz-tools";
const ListOfTasks = co.list(Task);

// React example
function TaskList({ tasks }: { tasks: co.loaded<typeof ListOfTasks> }) {
  return (
    <ul>
      {tasks.map((task) =>
        task.$isLoaded ? (
          <li key={task.$jazz.id}>
            {task.title} - {task.status}
          </li>
        ) : null,
      )}
    </ul>
  );
}
