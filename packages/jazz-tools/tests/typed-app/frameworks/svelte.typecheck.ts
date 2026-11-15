// Include types survive Svelte's QuerySubscription / QuerySubscriptionOne.
// Type-level only: nothing here runs.
import { expectTypeOf } from "vitest";
import {
  QuerySubscription,
  QuerySubscriptionOne,
} from "../../../src/svelte/query-subscription.svelte.js";
import {
  projectsWithTodos,
  todosWithIncludes,
  type ProjectWithTodos,
  type TodoWithIncludes,
} from "./include-types.js";

export function svelteSubscriptions() {
  const todos = new QuerySubscription(todosWithIncludes);
  expectTypeOf(todos.current).toEqualTypeOf<TodoWithIncludes[] | undefined>();
  if (todos.current) expectTypeOf(todos.current[0]!.project.name).toEqualTypeOf<string>();

  const projects = new QuerySubscription(() => projectsWithTodos);
  expectTypeOf(projects.current).toEqualTypeOf<ProjectWithTodos[] | undefined>();

  const todo = new QuerySubscriptionOne(todosWithIncludes);
  expectTypeOf(todo.current).toEqualTypeOf<TodoWithIncludes | null | undefined>();

  const project = new QuerySubscriptionOne(() => projectsWithTodos);
  expectTypeOf(project.current).toEqualTypeOf<ProjectWithTodos | null | undefined>();
}
