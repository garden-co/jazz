import { co, z } from "jazz-tools";
import { endAsyncEvent } from "react-native/Libraries/Performance/Systrace";
const projectId = "";
export {};

// #region Basic
const Task = co.map({
  title: z.string(),
  description: co.plainText(),
  get subtasks() {
    return co.list(Task);
  },
});

const Project = co.map({
  name: z.string(),
  tasks: co.list(Task),
});

const project = await Project.load(projectId);
if (!project.$isLoaded) throw new Error("Project not found or not accessible");

// This will be loaded
project.name; // string

// This *may not be loaded*, and *may not be accessible*
project.tasks; // MaybeLoaded<ListOfTasks>

const projectWithTasksShallow = await Project.load(projectId, {
  resolve: {
    tasks: true,
  },
});
if (!projectWithTasksShallow.$isLoaded)
  throw new Error("Project not found or not accessible");

// This list of tasks will be shallowly loaded
projectWithTasksShallow.tasks; // ListOfTasks
// We can access the properties of the shallowly loaded list
projectWithTasksShallow.tasks.length; // number
// This *may not be loaded*, and *may not be accessible*
projectWithTasksShallow.tasks[0]; // MaybeLoaded<Task>
// #endregion

// #region Each
const projectWithTasks = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: true,
    },
  },
});
if (!projectWithTasks.$isLoaded)
  throw new Error("Project not found or not accessible");

// The task will be loaded
projectWithTasks.tasks[0]; // Task
// Primitive fields are always loaded
projectWithTasks.tasks[0].title; // string
// References on the Task may not be loaded
projectWithTasks.tasks[0].subtasks; // MaybeLoaded<ListOfTasks>
// CoTexts are CoValues too
projectWithTasks.tasks[0].description; // MaybeLoaded<CoPlainText>
// #endregion

// #region Deep
const projectDeep = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: {
        subtasks: {
          $each: true,
        },
        description: true,
      },
    },
  },
});
if (!projectDeep.$isLoaded)
  throw new Error("Project not found or not accessible");

// Primitive fields are always loaded
projectDeep.tasks[0].subtasks[0].title; // string

// The description will be loaded as well
projectDeep.tasks[0].description; // CoPlainText
// #endregion

const taskId = "";

// #region Unauthorized
// If permissions on description are restricted:
const task = await Task.load(taskId, {
  resolve: { description: true },
});
task.$isLoaded; // false
task.$jazz.loadingState; // "unauthorized"
// #endregion

// #region NoCatch
// One task in the list has restricted permissions
const projectWithUnauthorizedTasks = await Project.load(projectId, {
  resolve: { tasks: { $each: true } },
});

project.$isLoaded; // false
project.$jazz.loadingState; // "unauthorized"
// #endregion

// #region ShallowNoCatch
// One task in the list has restricted permissions
const shallowlyLoadedProjectWithUnauthorizedTasks = await Project.load(
  projectId,
  {
    resolve: true,
  },
);
if (!project.$isLoaded) throw new Error("Project not found or not accessible");

// Assuming the user has permissions on the project, this load will succeed, even if the user cannot load one of the tasks in the list
project.$isLoaded; // true
// Tasks may not be loaded since we didn't request them
project.tasks.$isLoaded; // may be false
//#endregion

// #region SkipInaccessible
// Inaccessible tasks will not be loaded, but the project will
const projectWithInaccessibleSkipped = await Project.load(projectId, {
  resolve: { tasks: { $each: { $onError: "catch" } } },
});

if (!project.$isLoaded) {
  throw new Error("Project not found or not accessible");
}

if (!project.tasks.$isLoaded) {
  throw new Error("Task List not found or not accessible");
}

project.tasks[0].$isLoaded; // true
project.tasks[1].$isLoaded; // true
project.tasks[2].$isLoaded; // false (caught by $onError)
// #endregion

// #region NestedInaccessible
// Inaccessible tasks will not be loaded, but the project will
const projectWithNestedInaccessibleSkipped = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: {
        description: true,
        $onError: "catch",
      },
    },
  },
});

if (!project.$isLoaded) {
  throw new Error("Project not found or not accessible");
}

project.tasks[0].$isLoaded; // true
project.tasks[1].$isLoaded; // true
project.tasks[2].$isLoaded; // false (caught by $onError)
// #endregion

// #region NestedInaccessible
const projectWithNoCatchOnTaskError = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: {
        description: { $onError: "catch" },
      },
    },
  },
});

// The load fails because task[2] is inaccessible and no $onError caught it.
project.$isLoaded; // false
// #endregion

// #region MultipleCatch
const projectWithMultipleCatches = await Project.load(projectId, {
  resolve: {
    tasks: {
      $each: {
        description: { $onError: "catch" }, // catch errors loading task descriptions
        $onError: "catch", // catch errors loading tasks too
      },
    },
  },
});

project.$isLoaded; // true
project.tasks[0].$isLoaded; // true
// @ts-expect-error Not exposed on co.texts
project.tasks[0].description.$isLoaded; // true
project.tasks[1].$isLoaded; // true
// @ts-expect-error Not exposed on co.texts
project.tasks[1].description.$isLoaded; // false (caught by the inner handler)
project.tasks[2].$isLoaded; // false (caught by the outer handler)
// #endregion
