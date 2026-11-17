// Include types survive Vue's useAll / useOne / useOneSuspense.
// Type-level only: nothing here runs.
import { expectTypeOf } from "vitest";
import { useAll, useOne, useOneSuspense } from "../../../src/vue/index.js";
import {
  projectsWithTodos,
  todosWithIncludes,
  type ProjectWithTodos,
  type TodoWithIncludes,
} from "./include-types.js";

export async function vueHooks() {
  const todos = useAll(todosWithIncludes);
  expectTypeOf(todos.data.value).toEqualTypeOf<TodoWithIncludes[] | undefined>();
  // Getters and refs keep the row type too.
  const fromGetter = useAll(() => projectsWithTodos);
  expectTypeOf(fromGetter.data.value).toEqualTypeOf<ProjectWithTodos[] | undefined>();

  const todo = useOne(todosWithIncludes);
  expectTypeOf(todo.data.value).toEqualTypeOf<TodoWithIncludes | null | undefined>();
  if (todo.data.value) expectTypeOf(todo.data.value.project.name).toEqualTypeOf<string>();

  const project = await useOneSuspense(projectsWithTodos);
  expectTypeOf(project.data.value).toEqualTypeOf<ProjectWithTodos | null | undefined>();
}
