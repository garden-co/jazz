/**
 * The rows a query names, as the framework hooks see them: one query with
 * forward, array and reverse includes over the committed `basic` app, whose
 * types come from its generated bindings. Each framework's typecheck file
 * asserts that its hooks hand back exactly these rows.
 */
import { app } from "../apps/basic/jazz.generated.js";

export { app };

export const todosWithIncludes = app.todos
  .include({ project: true, assignees: true, owner: true })
  .requireIncludes();

export const projectsWithTodos = app.projects.include({
  todosViaProject: app.todos.include({ owner: true }),
});

type User = { id: string; name: string; friendsIds: string[] };

/** A todo with its project, assignees and owner. */
export type TodoWithIncludes = {
  id: string;
  title: string;
  done: boolean;
  tags: string[];
  projectId: string;
  ownerId: string | null;
  assigneesIds: string[];
  project: { id: string; name: string };
  assignees: User[];
  // A nullable reference stays nullable when includes are required.
  owner: User | null;
};

/** A project with its todos, each with its owner. */
export type ProjectWithTodos = {
  id: string;
  name: string;
  todosViaProject: Array<{
    id: string;
    title: string;
    done: boolean;
    tags: string[];
    projectId: string;
    ownerId: string | null;
    assigneesIds: string[];
    owner: User | null;
  }>;
};
