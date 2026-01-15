import { co } from "jazz-tools";
import { Task, Project } from "./schema";
const taskId = "";

// #region ManualSubscription
// Subscribe by ID
// @ts-expect-error Redeclared
const unsubscribe = Task.subscribe(taskId, {}, (updatedTask) => {
  console.log("Updated task:", updatedTask);
});

// Always clean up when finished
unsubscribe();
// #endregion

// #region SubscriptionInstanceMethod
const myTask = Task.create({
  title: "My new task",
});

// Subscribe using $jazz.subscribe
// @ts-expect-error Redeclared
const unsubscribe = myTask.$jazz.subscribe((updatedTask) => {
  console.log("Updated task:", updatedTask);
});

// Always clean up when finished
unsubscribe();
// #endregion

// #region EnsureLoaded
async function completeAllTasks(projectId: string) {
  // Load the project
  const project = await Project.load(projectId, { resolve: true });
  if (!project.$isLoaded) return;

  // Ensure tasks are deeply loaded
  const loadedProject = await project.$jazz.ensureLoaded({
    resolve: {
      tasks: {
        $each: true,
      },
    },
  });

  // Now we can safely access and modify tasks
  loadedProject.tasks.forEach((task, i) => {
    task.$jazz.set("title", `Task ${i}`);
  });
}
// #endregion

const projectId = "";
// #region ResolvedHelper
const TaskWithDescription = Task.resolved({
  description: true,
});
const ProjectWithTasks = Project.resolved({
  tasks: {
    // Use `.resolveQuery` to get the resolve query from a schema and compose it in other queries
    $each: TaskWithDescription.resolveQuery,
  },
});

// .load() will use the resolve query from the schema
const project = await ProjectWithTasks.load(projectId);
if (!project.$isLoaded) throw new Error("Project not found or not accessible");
// Both the tasks and the descriptions are loaded
project.tasks[0].description; // CoPlainText
// #endregion
