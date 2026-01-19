import { co, z } from "jazz-tools";

/** An individual task which collaborators can tick or rename */
export const Task = co.map({
  done: z.boolean(),
  text: co.plainText(),
});

/** Our top level object: a project with a title, referencing a list of tasks */
export const TodoProject = co
  .map({
    title: z.string(),
    tasks: co.list(Task),
  })
  .withPermissions({
    onCreate: (newGroup) => {
      newGroup.makePublic();
    },
  });
