import { co, z, setDefaultValidationMode } from "jazz-tools";

setDefaultValidationMode("strict");

/** Walkthrough: Defining the data model with CoJSON
 *
 *  Here, we define our main data model of tasks, lists of tasks and projects
 *  using CoJSON's collaborative map and list types, CoMap & CoList.
 *
 *  CoMap values and CoLists items can contain:
 *  - arbitrary immutable JSON
 *  - other CoValues
 **/

/** An individual task which collaborators can tick or rename */
export const Task = co
  .map({
    done: z.boolean(),
    text: co.plainText(),
    version: z.literal(1),
  })
  .withMigration((task) => {
    if (!task.version) {
      // In the Task v0 schema, the text field was a string.
      // Since some tasks with string text fields were created before we added the version field,
      // we need to check if the text field is a string or a reference to a CoValue.
      // If it's a string, we migrate it to plainText.
      const textRef = task.$jazz.refs.text;
      if (!textRef) {
        // The conversion is done automatically when assigning the string value to the plainText field
        task.$jazz.set("text", task.text);
      }
      task.$jazz.set("version", 1);
    }
  });

export const TaskWithText = Task.resolved({
  text: true,
});
export type TaskWithText = co.loaded<typeof TaskWithText>;

/** Our top level object: a project with a title, referencing a list of tasks */
export const TodoProject = co.map({
  title: z.string(),
  tasks: co.list(Task),
});

export const TodoProjectWithTasks = TodoProject.resolved({
  tasks: {
    $each: TaskWithText.resolveQuery,
  },
});

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const TodoAccountRoot = co.map({
  projects: co.list(TodoProject),
});

export const TodoAccount = co
  .account({
    profile: co.profile(),
    root: TodoAccountRoot,
  })
  .withMigration(async (account) => {
    /** The account migration is run on account creation and on every log-in.
     *  You can use it to set up the account root and any other initial CoValues you need.
     */
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", {
        projects: [],
      });
    }
  });

export const TodoAccountWithProjects = TodoAccount.resolved({
  root: { projects: { $each: { $onError: "catch" } } },
});

/** Walkthrough: Continue with ./2_main.tsx */
