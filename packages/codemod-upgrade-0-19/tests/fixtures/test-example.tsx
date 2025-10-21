// @ts-nocheck

// Test file demonstrating Jazz hook patterns that will be migrated
// This file shows examples of what the codemod will transform

import {
  useAccount,
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

// $onError: null should become $onError: 'catch'
function ExampleOnErrorNull() {
  const todo = useAccount(MyAccount, todoId, {
    resolve: { root: { todos: { $each: { $onError: null } } } },
  });

  return <div>{todo?.text}</div>;
}

// useCoStateWithSelector should become useCoState
function ExampleCoState({ todoId }: { todoId: string }) {
  // Should be transformed to useCoState
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

// useAccountWithSelector should become useAccount
function ExampleAccount() {
  // Should be transformed to useAccount
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

// Multiple usages in same component
function ComplexComponent({ itemId }: { itemId: string }) {
  // Should be transformed to useCoState
  const item = useCoStateWithSelector(TodoItem, itemId, {
    resolve: { text: true },
    select: (item) => item,
  });

  // Should be transformed to useAccount
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

// Old useAccount pattern with destructuring { me, agent, logOut }
function OldUseAccountPattern() {
  // const me = useAccount();
  // const agent = useAgent();
  // const logOut = useLogOut();
  const { me, agent, logOut } = useAccount();

  return (
    <div>
      <p>User: {me?.profile?.name}</p>
      <button onClick={logOut}>Log out</button>
      <pre>{JSON.stringify(agent, null, 2)}</pre>
    </div>
  );
}

// Old useAccount pattern with only some properties
function PartialUseAccountPattern() {
  // const me = useAccount();
  // const logOut = useLogOut();
  const { me, logOut } = useAccount();

  return (
    <div>
      <p>User: {me?.profile?.name}</p>
      <button onClick={logOut}>Log out</button>
    </div>
  );
}

// Old useAccount pattern with only me
function OnlyMePattern() {
  // const me = useAccount();
  const { me } = useAccount();

  return <div>{me?.profile?.name}</div>;
}

// Old useAccount pattern with aliasing
function AliasedPattern() {
  // const currentUser = useAccount();
  // const myAgent = useAgent();
  const { me: currentUser, agent: myAgent } = useAccount();

  return (
    <div>
      <p>{currentUser?.profile?.name}</p>
      <pre>{JSON.stringify(myAgent, null, 2)}</pre>
    </div>
  );
}

// Old useAccount non-destructured pattern with account, agent and logOut
function NoDestructuringWithAccountAgentLogOut() {
  // const account = useAccount();
  // const agent = useAgent();
  // const logOut = useLogOut();
  const account = useAccount();

  return (
    <div>
      {/* account?.profile?.name */}
      <p>{account.me?.profile?.name}</p>
      {/* logOut */}
      <button onClick={account.logOut}>Log out</button>
      {/* agent */}
      <pre>{JSON.stringify(account.agent, null, 2)}</pre>
    </div>
  );
}

export {
  ExampleOnErrorNull,
  ExampleCoState,
  ExampleAccount,
  ComplexComponent,
  OldUseAccountPattern,
  PartialUseAccountPattern,
  OnlyMePattern,
  AliasedPattern,
  NoDestructuringPattern,
  NoDestructuringWithAccountAgentLogOut,
};
