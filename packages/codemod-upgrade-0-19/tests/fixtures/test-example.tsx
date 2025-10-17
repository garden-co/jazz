// @ts-nocheck

// Test file demonstrating Jazz hook patterns that will be migrated
// This file shows examples of what the codemod will transform

import {
  useCoStateWithSelector,
  useAccountWithSelector,
} from "jazz-tools/react";
import { co, Account, z } from "jazz-tools";

const TodoItem = co.map({
  text: z.string(),
  done: z.boolean(),
});

const TodoList = co.list(TodoItem);

const MyAccount = co
  .account({
    root: co.map({ todos: TodoList }),
    profile: co.profile(),
  })
  .withMigration(async (account) => {
    if (!account.$jazz.has("root")) {
      account.$jazz.set("root", { todos: [] });
    }
  });

// Example 1: useCoStateWithSelector should become useCoState
function ExampleCoState({ todoId }: { todoId: string }) {
  const todo = useCoStateWithSelector(TodoItem, todoId, {
    resolve: { text: true, done: true },
    select: (todo) => (todo.$isLoaded ? todo : undefined),
  });

  return (
    <div>
      <span>{todo?.text}</span>
      <input type="checkbox" checked={todo?.done} />
    </div>
  );
}

// Example 2: useAccountWithSelector should become useAccount
function ExampleAccount() {
  const todos = useAccountWithSelector(MyAccount, {
    resolve: { root: { todos: { $each: { $onError: null } } } },
    select: (me) => me?.root.todos,
  });

  return (
    <ul>
      {todos?.map((todo) => (
        <li key={todo.$jazz.id}>{todo.text}</li>
      ))}
    </ul>
  );
}

// Example 3: Multiple usages in same component
function ComplexComponent({ itemId }: { itemId: string }) {
  const item = useCoStateWithSelector(TodoItem, itemId, {
    resolve: { text: true },
    select: (item) => item,
  });

  const account = useAccountWithSelector(MyAccount, {
    resolve: { root: true },
    select: (me) => me,
  });

  return (
    <div>
      <h1>{item?.text}</h1>
      <p>Owner: {account?.root?.owner}</p>
    </div>
  );
}

export { ExampleCoState, ExampleAccount, ComplexComponent };
