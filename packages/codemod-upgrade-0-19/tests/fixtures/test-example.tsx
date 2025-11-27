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
  const { me } = useAccount(MyAccount, {
    resolve: { root: { todos: { $each: { $onError: null } } } },
  });

  return <div>{me?.root.todos[0]?.text}</div>;
}

// useCoStateWithSelector should become useCoState
function ExampleCoState({ todoId }: { todoId: string }) {
  // Should be transformed to useCoState
  const todo = useCoStateWithSelector(TodoItem, todoId, {
    resolve: { text: true, done: true },
    select: (todo) => todo,
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

// Hook without existing selector
function HookWithoutExistingSelector() {
  // const account = useAccount(MyAccount, {
  //   resolve: { profile: true },
  //   select: (account) => account.$isLoaded
  //    ? account
  //    : account.$jazz.loadingState === "loading"
  //      ? undefined
  //      : null
  // });
  const account = useAccount(MyAccount, {
    resolve: { profile: true },
  });

  return <div>{account?.profile?.name}</div>;
}

// Hook with existing expression selector
function HookWithExistingExpressionSelector() {
  // const profileName = useAccount(MyAccount, {
  //   resolve: { profile: true },
  //   select: (account) => account.$isLoaded
  //    ? account.profile.name
  //    : account.$jazz.loadingState === "loading"
  //      ? undefined
  //      : null
  // });
  const profileName = useAccountWithSelector(MyAccount, {
    resolve: { profile: true },
    select: (account) => account?.profile?.name,
  });

  return <div>{profileName}</div>;
}

// Hook with existing block selector
function HookWithExistingBlockSelector() {
  // Skip migration for block body selectors
  const profileName = useAccountWithSelector(MyAccount, {
    resolve: { profile: true },
    select: (account) => {
      return account?.profile.name;
    },
  });

  return <div>{profileName}</div>;
}

// Hook with no options argument at all
function HookWithNoOptions() {
  // Should add: useAccount() -> useAccount(undefined, { select: (me) => ... })
  const me = useAccount();

  return <div>{me?.profile?.name}</div>;
}

// Hook with schema but no options
function HookWithSchemaNoOptions() {
  // Should add: useAccount(MyAccount) -> useAccount(undefined, { select: (account) => ... })
  const account = useAccount(MyAccount);

  return <div>{account?.profile?.name}</div>;
}

// Nullability check on MaybeLoaded value
async function MaybeLoadedCoValueIfCheck() {
  const account = await Account.load("account-id", {
    resolve: { profile: true },
  });

  // if (!account.$isLoaded)
  if (!account) {
    return "Loading...";
  }

  return account.profile.name;
}

// Nullability check on optional MaybeLoaded value
async function OptionalMaybeLoadedCoValueIfCheck({
  accountId,
}: {
  accountId?: string;
}) {
  const optionalAccount = accountId
    ? await Account.load("account-id", {
        resolve: { profile: true },
      })
    : undefined;

  // if (!optionalAccount?.$isLoaded)
  if (!optionalAccount) {
    return "Loading...";
  }

  return optionalAccount.profile.name;
}

// Nullability check on nested MaybeLoaded value
async function MaybeLoadedNestedCoValueIfCheck() {
  const account = await Account.load("account-id", {
    resolve: true,
  });

  if (!account) {
    return "Loading...";
  }

  // if (!account.profile.$isLoaded)
  if (!account.profile) {
    return "Loading...";
  }

  return account.profile.name;
}

// If with || operator
async function MaybeLoadedOrCheck() {
  const account = await Account.load("account-id", {
    resolve: { profile: true },
  });
  const account2 = await Account.load("account-id-2", {
    resolve: { profile: true },
  });

  // Should transform: if (!account.$isLoaded || !account2.$isLoaded)
  if (!account || !account2) {
    return "Loading...";
  }

  return account.profile.name;
}

// If with && operator
async function MaybeLoadedAndCheck() {
  const account = await Account.load("account-id", {
    resolve: { profile: true },
  });
  const account2 = await Account.load("account-id-2");

  // Should transform: if (account.$isLoaded && account2.$isLoaded)
  if (account && account2) {
    return account.profile.name;
  }

  return "Loading...";
}

// Complex if with mixed operators
async function MaybeLoadedMixedCheck() {
  const account = await Account.load("account-id");
  const account2 = await Account.load("account-id-2");
  const isAdmin = true;

  // Should transform: if (account.$isLoaded && (account2.$isLoaded || isAdmin))
  if (account && (account2 || isAdmin)) {
    return "Loaded";
  }

  return "Loading...";
}

// Complex if with negation and &&
async function MaybeLoadedNegationAndCheck() {
  const account = await Account.load("account-id");
  const account2 = await Account.load("account-id-2");

  // Should transform: if (!account.$isLoaded && account2.$isLoaded)
  if (!account && account2) {
    return "Account not loaded but account2 is";
  }

  return "Other state";
}

// Complex if with parentheses
async function MaybeLoadedParenthesesCheck() {
  const account = await Account.load("account-id");
  const account2 = await Account.load("account-id-2");
  const ready = true;

  // Should transform: if ((account.$isLoaded && account2.$isLoaded) || ready)
  if ((account && account2) || ready) {
    return "Ready or loaded";
  }

  return "Not ready";
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
  HookWithoutExistingSelector,
  HookWithExistingExpressionSelector,
  HookWithExistingBlockSelector,
  HookWithNoOptions,
  HookWithSchemaNoOptions,
  MaybeLoadedCoValueIfCheck,
  OptionalMaybeLoadedCoValueIfCheck,
  MaybeLoadedNestedCoValueIfCheck,
  MaybeLoadedOrCheck,
  MaybeLoadedAndCheck,
  MaybeLoadedMixedCheck,
  MaybeLoadedNegationAndCheck,
  MaybeLoadedParenthesesCheck,
};
