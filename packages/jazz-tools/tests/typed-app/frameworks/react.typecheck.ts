// Include types survive React's useAll / useOne (and their suspense forms).
// Type-level only: nothing here runs.
import { expectTypeOf } from "vitest";
import type { RowOf } from "../../../src/index.js";
import { useAll, useAllSuspense, useOne, useOneSuspense } from "../../../src/react/index.js";
import {
  projectsWithTodos,
  todosWithIncludes,
  type ProjectWithTodos,
  type TodoWithIncludes,
} from "./include-types.js";

expectTypeOf<RowOf<typeof todosWithIncludes>>().toEqualTypeOf<TodoWithIncludes>();
expectTypeOf<RowOf<typeof projectsWithTodos>>().toEqualTypeOf<ProjectWithTodos>();

export function ReactHooks() {
  const todos = useAll(todosWithIncludes);
  expectTypeOf(todos.data).toEqualTypeOf<TodoWithIncludes[] | undefined>();
  if (todos.data) {
    expectTypeOf(todos.data[0]!.project.name).toEqualTypeOf<string>();
    expectTypeOf(todos.data[0]!.assignees).toEqualTypeOf<TodoWithIncludes["assignees"]>();
  }

  const projects = useAll(projectsWithTodos);
  expectTypeOf(projects.data).toEqualTypeOf<ProjectWithTodos[] | undefined>();

  const todo = useOne(todosWithIncludes);
  expectTypeOf(todo.data).toEqualTypeOf<TodoWithIncludes | null | undefined>();

  const project = useOne(projectsWithTodos);
  expectTypeOf(project.data).toEqualTypeOf<ProjectWithTodos | null | undefined>();

  const suspended = useAllSuspense(todosWithIncludes);
  expectTypeOf(suspended.data).toEqualTypeOf<TodoWithIncludes[]>();
  const suspendedOne = useOneSuspense(projectsWithTodos);
  expectTypeOf(suspendedOne.data).toEqualTypeOf<ProjectWithTodos | null>();
  return null;
}
