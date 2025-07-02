import { co, z } from "jazz-tools";

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
export const Task = co.map({
  done: z.boolean(),
  text: co.plainText(),
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
  profilingEnabled: z.boolean(),
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
        profilingEnabled: false,
      });
    }
  });

/** Walkthrough: Continue with ./2_main.tsx */
