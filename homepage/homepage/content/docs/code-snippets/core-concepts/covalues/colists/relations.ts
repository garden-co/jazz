import { co, z } from "jazz-tools";

const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),

  get project(): co.Optional<typeof Project> {
    return co.optional(Project);
  },
});

const ListOfTasks = co.list(Task);

const Project = co.map({
  name: z.string(),

  get tasks(): co.List<typeof Task> {
    return ListOfTasks;
  },
});

const project = Project.create({
  name: "Garden Project",
  tasks: ListOfTasks.create([]),
});

const task = Task.create({
  title: "Plant seedlings",
  status: "todo",
  project: project, // Add a reference to the project
});

// Add a task to a garden project
project.tasks.$jazz.push(task);

// Access the project from the task
console.log(task.project); // { name: "Garden Project", tasks: [task] }
