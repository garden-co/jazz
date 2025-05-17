import {
  Account,
  CoList,
  CoMap,
  CoPlainText,
  Profile,
  coField,
} from "jazz-tools";

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
export class Task extends CoMap {
  done = coField.boolean;
  text = coField.ref(CoPlainText);
}

export class ListOfTasks extends CoList.Of(coField.ref(Task)) {}

/** Our top level object: a project with a title, referencing a list of tasks */
export class TodoProject extends CoMap {
  title = coField.string;
  tasks = coField.ref(ListOfTasks);
}

export class ListOfProjects extends CoList.Of(coField.ref(TodoProject)) {}

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export class TodoAccountRoot extends CoMap {
  projects = coField.ref(ListOfProjects);
}

export class TodoAccount extends Account {
  profile = coField.ref(Profile);
  root = coField.ref(TodoAccountRoot);

  /** The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate() {
    if (!this._refs.root) {
      this.root = TodoAccountRoot.create({
        projects: ListOfProjects.create([]),
      });
    }
  }
}

/** Walkthrough: Continue with ./2_main.tsx */
