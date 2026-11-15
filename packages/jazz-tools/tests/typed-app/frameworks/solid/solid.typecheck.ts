// Include types survive Solid's useAll / useOne.
// Type-level only: nothing here runs.
import { expectTypeOf } from "vitest";
import { useAll } from "../../../../src/solid/use-all.js";
import { useOne } from "../../../../src/solid/use-one.js";
import {
  projectsWithTodos,
  todosWithIncludes,
  type ProjectWithTodos,
  type TodoWithIncludes,
} from "../include-types.js";

export function solidHooks() {
  const todos = useAll(() => ({ query: todosWithIncludes }));
  expectTypeOf(todos.data).toEqualTypeOf<TodoWithIncludes[] | undefined>();
  if (todos.data) expectTypeOf(todos.data[0]!.project.name).toEqualTypeOf<string>();

  const projects = useAll(() => ({ query: projectsWithTodos }));
  expectTypeOf(projects.data).toEqualTypeOf<ProjectWithTodos[] | undefined>();

  const todo = useOne(() => ({ query: todosWithIncludes }));
  expectTypeOf(todo.data).toEqualTypeOf<TodoWithIncludes | null | undefined>();

  const project = useOne(() => ({ query: projectsWithTodos }));
  expectTypeOf(project.data).toEqualTypeOf<ProjectWithTodos | null | undefined>();
}
