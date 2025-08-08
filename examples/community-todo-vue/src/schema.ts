import { CoPlainText, co, z } from "jazz-tools";

/** An individual task which collaborators can tick or rename */
export const Task = co
  .map({
    done: z.boolean(),
    text: co.plainText(),
    version: z.literal(1),
  })
  .withMigration((task) => {
    if (!task.version) {
      // Cast to the v1 version
      const task_v1 = task.castAs(Task_V1);

      // Check if the task text field is a string or an id
      // if it's a string migrate to plaintext
      // We need to do this check because some tasks with plainText have been created before we added the version field
      if (!task_v1.text.startsWith("co_z")) {
        task.text = CoPlainText.create(task_v1.text, task._owner);
      }
      task.version = 1;
    }
  });

const Task_V1 = co.map({
  done: z.boolean(),
  text: z.string(),
});

/** Our top level object: a project with a title, referencing a list of tasks */
export const TodoProject = co.map({
  title: z.string(),
  tasks: co.list(Task),
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
    if (account.root === undefined) {
      account.root = TodoAccountRoot.create({
        projects: co.list(TodoProject).create([], { owner: account }),
      });
    }
  });
