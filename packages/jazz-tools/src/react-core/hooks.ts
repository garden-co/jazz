import { useSyncExternalStoreWithSelector } from "use-sync-external-store/shim/with-selector";
import React, {
  useCallback,
  useContext,
  useRef,
  useSyncExternalStore,
} from "react";

import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  CoValue,
  CoValueClassOrSchema,
  CoValueLoadingState,
  CoValueUnloadedState,
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  Loaded,
  MaybeLoaded,
  Unloaded,
  ResolveQuery,
  ResolveQueryStrict,
  SubscriptionScope,
  coValueClassFromCoValueClassOrSchema,
  createUnloadedCoValue,
  type BranchDefinition,
} from "jazz-tools";
import { JazzContext, JazzContextManagerContext } from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";
import { CoValueSubscription } from "./types.js";

export function useJazzContext<Acc extends Account>() {
  const value = useContext(JazzContext) as JazzContextType<Acc>;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useJazzContextManager<Acc extends Account>() {
  const value = useContext(JazzContextManagerContext) as JazzContextManager<
    Acc,
    {}
  >;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useAuthSecretStorage() {
  const value = useContext(JazzContextManagerContext);

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this useAuthSecretStorage.",
    );
  }

  return value.getAuthSecretStorage();
}

export function useIsAuthenticated() {
  const authSecretStorage = useAuthSecretStorage();

  return useSyncExternalStore(
    useCallback(
      (callback) => {
        return authSecretStorage.onUpdate(callback);
      },
      [authSecretStorage],
    ),
    () => authSecretStorage.isAuthenticated,
    () => authSecretStorage.isAuthenticated,
  );
}

export function useCoValueSubscription<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  id: string | undefined | null,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
    unstable_branch?: BranchDefinition;
  },
) {
  const contextManager = useJazzContextManager();

  const createSubscription = () => {
    if (!id) {
      return {
        subscription: null,
        contextManager,
        id,
        Schema,
      };
    }

    if (options?.unstable_branch?.owner === null) {
      return {
        subscription: null,
        contextManager,
        id,
        Schema,
      };
    }

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(
      node,
      options?.resolve ?? true,
      id,
      {
        ref: coValueClassFromCoValueClassOrSchema(Schema),
        optional: true,
      },
      false,
      false,
      options?.unstable_branch,
    );

    return {
      subscription,
      contextManager,
      id,
      Schema,
      branchName: options?.unstable_branch?.name,
      branchOwnerId: options?.unstable_branch?.owner?.$jazz.id,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  const branchName = options?.unstable_branch?.name;
  const branchOwnerId = options?.unstable_branch?.owner?.$jazz.id;

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.id !== id ||
      subscription.Schema !== Schema ||
      subscription.branchName !== branchName ||
      subscription.branchOwnerId !== branchOwnerId
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, id, contextManager, branchName, branchOwnerId]);

  return subscription.subscription as CoValueSubscription<S, R>;
}

function getSubscriptionValue<C extends CoValue>(
  subscription: SubscriptionScope<C> | null,
): MaybeLoaded<C> {
  if (!subscription) {
    return createUnloadedCoValue("", CoValueLoadingState.UNAVAILABLE);
  }
  const value = subscription.getCurrentValue();
  if (typeof value === "string") {
    return createUnloadedCoValue(subscription.id, value);
  }
  return value;
}

function useGetCurrentValue<C extends CoValue>(
  subscription: SubscriptionScope<C> | null,
) {
  const previousValue = useRef<MaybeLoaded<CoValue> | undefined>(undefined);

  return useCallback(() => {
    const currentValue = getSubscriptionValue(subscription);
    // Avoid re-renders if the value is not loaded and didn't change
    if (
      previousValue.current !== undefined &&
      previousValue.current.$jazz.id === currentValue.$jazz.id &&
      !previousValue.current.$isLoaded &&
      !currentValue.$isLoaded &&
      previousValue.current.$jazz.loadingState ===
        currentValue.$jazz.loadingState
    ) {
      return previousValue.current as MaybeLoaded<C>;
    }
    previousValue.current = currentValue;
    return currentValue;
  }, [subscription]);
}

/**
 * React hook for subscribing to CoValues and handling loading states.
 *
 * This hook provides a convenient way to subscribe to CoValues and automatically
 * handles the subscription lifecycle (subscribe on mount, unsubscribe on unmount).
 * It also supports deep loading of nested CoValues through resolve queries.
 *
 * The {@param options.select} function allows returning only specific parts of the CoValue data,
 * which helps reduce unnecessary re-renders by narrowing down the returned data.
 * Additionally, you can provide a custom {@param options.equalityFn} to further optimize
 * performance by controlling when the component should re-render based on the selected data.
 *
 * @returns The loaded CoValue, or an {@link Unloaded} value. Use `$isLoaded` to check whether the
 * CoValue is loaded, or use {@link MaybeLoaded.$jazz.loadingState} to get the detailed loading state.
 * If a selector function is provided, returns the result of the selector function.
 *
 * @example
 * ```tsx
 * // Select only specific fields to reduce re-renders
 * const Project = co.map({
 *   name: z.string(),
 *   description: z.string(),
 *   tasks: co.list(Task),
 *   lastModified: z.date(),
 * });
 *
 * function ProjectTitle({ projectId }: { projectId: string }) {
 *   // Only re-render when the project name changes, not other fields
 *   const projectName = useCoState(
 *     Project,
 *     projectId,
 *     {
 *       select: (project) => !project.$isLoading ? project.name : "Loading...",
 *     }
 *   );
 *
 *   return <h1>{projectName}</h1>;
 * }
 * ```
 *
 * @example
 * ```tsx
 * // Deep loading with resolve queries
 * const Project = co.map({
 *   name: z.string(),
 *   tasks: co.list(Task),
 *   owner: TeamMember,
 * });
 *
 * function ProjectView({ projectId }: { projectId: string }) {
 *   const project = useCoState(Project, projectId, {
 *     resolve: {
 *       tasks: { $each: true },
 *       owner: true,
 *     },
 *   });
 *
 *   if (!project.$isLoaded) {
 *     switch (project.$jazz.loadingState) {
 *       case "unauthorized":
 *         return "Project not accessible";
 *       case "unavailable":
 *         return "Project not found";
 *       case "unloaded":
 *         return "Loading project...";
 *     }
 *   }
 *
 *   return (
 *     <div>
 *       <h1>{project.name}</h1>
 *       <p>Owner: {project.owner.name}</p>
 *       <ul>
 *         {project.tasks.map((task) => (
 *           <li key={task.id}>{task.title}</li>
 *         ))}
 *       </ul>
 *     </div>
 *   );
 * }
 * ```
 *
 * @example
 * ```tsx
 * // Using with optional references and error handling
 * const Task = co.map({
 *   title: z.string(),
 *   assignee: co.optional(TeamMember),
 *   subtasks: co.list(Task),
 * });
 *
 * function TaskDetail({ taskId }: { taskId: string }) {
 *   const task = useCoState(Task, taskId, {
 *     resolve: {
 *       assignee: true,
 *       subtasks: { $each: { $onError: 'catch' } },
 *     },
 *   });
 *
 *   if (!task.$isLoaded) {
 *     switch (task.$jazz.loadingState) {
 *       case "unauthorized":
 *         return "Task not accessible";
 *       case "unavailable":
 *         return "Task not found";
 *       case "unloaded":
 *         return "Loading task...";
 *     }
 *   }
 *
 *   return (
 *     <div>
 *       <h2>{task.title}</h2>
 *       {task.assignee && <p>Assigned to: {task.assignee.name}</p>}
 *       <ul>
 *         {task.subtasks.map((subtask, index) => (
 *           subtask.$isLoaded ? <li key={subtask.id}>{subtask.title}</li> : <li key={index}>Inaccessible subtask</li>
 *         ))}
 *       </ul>
 *     </div>
 *   );
 * }
 * ```
 *
 * @example
 * ```tsx
 * // Use custom equality function for complex data structures
 * const TaskList = co.list(Task);
 *
 * function TaskCount({ listId }: { listId: string }) {
 *   const taskStats = useCoState(
 *     TaskList,
 *     listId,
 *     {
 *       resolve: { $each: true },
 *       select: (tasks) => {
 *         if (!tasks.$isLoaded) return { total: 0, completed: 0 };
 *         return {
 *           total: tasks.length,
 *           completed: tasks.filter(task => task.completed).length,
 *         };
 *       },
 *       // Custom equality to prevent re-renders when stats haven't changed
 *       equalityFn: (a, b) => a.total === b.total && a.completed === b.completed,
 *     }
 *   );
 *
 *   return (
 *     <div>
 *       {taskStats.completed} of {taskStats.total} tasks completed
 *     </div>
 *   );
 * }
 * ```
 *
 * For more examples, see the [subscription and deep loading](https://jazz.tools/docs/react/using-covalues/subscription-and-loading) documentation.
 */
export function useCoState<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
>(
  /** The CoValue schema or class constructor */
  Schema: S,
  /** The ID of the CoValue to subscribe to. If `undefined`, returns an `unavailable` value */
  id: string | undefined,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load */
    resolve?: ResolveQueryStrict<S, R>;
    /** Select which value to return */
    select?: (value: MaybeLoaded<Loaded<S, R>>) => TSelectorReturn;
    /** Equality function to determine if the selected value has changed, defaults to `Object.is` */
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
    /**
     * Create or load a branch for isolated editing.
     *
     * Branching lets you take a snapshot of the current state and start modifying it without affecting the canonical/shared version.
     * It's a fork of your data graph: the same schema, but with diverging values.
     *
     * The checkout of the branch is applied on all the resolved values.
     *
     * @param name - A unique name for the branch. This identifies the branch
     *   and can be used to switch between different branches of the same CoValue.
     * @param owner - The owner of the branch. Determines who can access and modify
     *   the branch. If not provided, the branch is owned by the current user.
     *
     * For more info see the [branching](https://jazz.tools/docs/react/using-covalues/version-control) documentation.
     */
    unstable_branch?: BranchDefinition;
  },
): TSelectorReturn {
  const subscription = useCoValueSubscription(Schema, id, options);
  const getCurrentValue = useGetCurrentValue(subscription);

  const value = useSyncExternalStoreWithSelector<
    MaybeLoaded<Loaded<S, R>>,
    TSelectorReturn
  >(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    getCurrentValue,
    getCurrentValue,
    options?.select ?? ((value) => value as TSelectorReturn),
    options?.equalityFn ?? Object.is,
  );

  return value;
}

export function useSubscriptionSelector<
  S extends CoValueClassOrSchema,
  R extends ResolveQuery<S>,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
>(
  subscription: CoValueSubscription<S, R>,
  options?: {
    select?: (value: MaybeLoaded<Loaded<S, R>>) => TSelectorReturn;
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
  },
) {
  const getCurrentValue = useGetCurrentValue(subscription);

  return useSyncExternalStoreWithSelector<
    MaybeLoaded<Loaded<S, R>>,
    TSelectorReturn
  >(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    getCurrentValue,
    getCurrentValue,
    options?.select ?? ((value) => value as TSelectorReturn),
    options?.equalityFn ?? Object.is,
  );
}

export function useAccountSubscription<
  S extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
    unstable_branch?: BranchDefinition;
  },
) {
  const contextManager = useJazzContextManager();

  const createSubscription = () => {
    const agent = getCurrentAccountFromContextManager(contextManager);

    if (agent.$type$ === "Anonymous") {
      return {
        subscription: null,
        contextManager,
        agent,
      };
    }

    // We don't need type validation here, since it's mostly to help users on public API
    const resolve: any = options?.resolve ?? true;

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(
      node,
      resolve,
      agent.$jazz.id,
      {
        ref: coValueClassFromCoValueClassOrSchema(Schema),
        optional: true,
      },
      false,
      false,
      options?.unstable_branch,
    );

    return {
      subscription,
      contextManager,
      Schema,
      branchName: options?.unstable_branch?.name,
      branchOwnerId: options?.unstable_branch?.owner?.$jazz.id,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  const branchName = options?.unstable_branch?.name;
  const branchOwnerId = options?.unstable_branch?.owner?.$jazz.id;

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.Schema !== Schema ||
      subscription.branchName !== options?.unstable_branch?.name ||
      subscription.branchOwnerId !== options?.unstable_branch?.owner?.$jazz.id
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, contextManager, branchName, branchOwnerId]);

  return subscription.subscription as CoValueSubscription<S, R>;
}

/**
 * React hook for accessing the current user's account and authentication state.
 * 
 * This hook provides access to the current user's account profile and root data,
 * along with authentication utilities. It automatically handles subscription to
 * the user's account data and provides a logout function.
 * 
 * @returns An object containing:
 * - `me`: The account data, or an {@link Unloaded} value. Use `$isLoaded` to check whether the
 * CoValue is loaded, or use {@link MaybeLoaded.$jazz.loadingState} to get the detailed loading state.
 * - `logOut`: Function to log out the current user

 * @example
 * ```tsx
 * // Deep loading with resolve queries
 * function ProjectListWithDetails() {
 *   const { me } = useAccount(MyAppAccount, {
 *     resolve: {
 *       profile: true,
 *       root: {
 *         myProjects: {
 *           $each: {
 *             tasks: true,
 *           },
 *         },
 *       },
 *     },
 *   });
 * 
 *   if (!me.$isLoaded) {
 *     switch (me.$jazz.loadingState) {
 *       case "unauthorized":
 *         return "Account not accessible";
 *       case "unavailable":
 *         return "Account not found";
 *       case "unloaded":
 *         return "Loading account...";
 *     }
 *   }
 * 
 *   return (
 *     <div>
 *       <h1>{me.profile.name}'s projects</h1>
 *       <ul>
 *         {me.root.myProjects.map((project) => (
 *           <li key={project.id}>
 *             {project.name} ({project.tasks.length} tasks)
 *           </li>
 *         ))}
 *       </ul>
 *     </div>
 *   );
 * }
 * ```
 * 
 */
export function useAccount<
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A> = true,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load from the account */
    resolve?: ResolveQueryStrict<A, R>;
    /**
     * Create or load a branch for isolated editing.
     *
     * Branching lets you take a snapshot of the current state and start modifying it without affecting the canonical/shared version.
     * It's a fork of your data graph: the same schema, but with diverging values.
     *
     * The checkout of the branch is applied on all the resolved values.
     *
     * @param name - A unique name for the branch. This identifies the branch
     *   and can be used to switch between different branches of the same CoValue.
     * @param owner - The owner of the branch. Determines who can access and modify
     *   the branch. If not provided, the branch is owned by the current user.
     *
     * For more info see the [branching](https://jazz.tools/docs/react/using-covalues/version-control) documentation.
     */
    unstable_branch?: BranchDefinition;
  },
): {
  me: MaybeLoaded<Loaded<A, R>>;
  logOut: () => void;
} {
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const subscription = useAccountSubscription(AccountSchema, options);
  const getCurrentValue = useGetCurrentValue(subscription);

  const value = React.useSyncExternalStore<MaybeLoaded<Loaded<A, R>>>(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    getCurrentValue,
    getCurrentValue,
  );

  return {
    me: value,
    logOut: contextManager.logOut,
  };
}

/**
 * React hook for accessing the current agent. An agent can either be:
 * - an Authenticated Account, if the user is logged in
 * - an Anonymous Account, if the user didn't log in
 * - or an anonymous agent, if in guest mode
 *
 * The agent can be used as the `loadAs` parameter for load and subscribe methods.
 */
export function useAgent<A extends AccountClass<Account> | AnyAccountSchema>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
): AnonymousJazzAgent | Loaded<A, true> {
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const agent = getCurrentAccountFromContextManager(contextManager);
  return agent;
}

/**
 * React hook for accessing the current user's account with selective data extraction and custom equality checking.
 *
 * This hook extends `useAccount` by allowing you to select only specific parts of the account data
 * through a selector function, which helps reduce unnecessary re-renders by narrowing down the
 * returned data. Additionally, you can provide a custom equality function to further optimize
 * performance by controlling when the component should re-render based on the selected data.
 *
 * The hook automatically handles the subscription lifecycle and supports deep loading of nested
 * CoValues through resolve queries, just like `useAccount`.
 *
 * @returns The result of the selector function applied to the loaded account data
 *
 * @example
 * ```tsx
 * // Select only specific fields to reduce re-renders
 * const MyAppAccount = co.account({
 *   profile: co.profile(),
 *   root: co.map({
 *     name: z.string(),
 *     email: z.string(),
 *     lastLogin: z.date(),
 *   }),
 * });
 *
 * function UserProfile({ accountId }: { accountId: string }) {
 *   // Only re-render when the profile name changes, not other fields
 *   const profileName = useAccountWithSelector(
 *     MyAppAccount,
 *     {
 *       resolve: {
 *         profile: true,
 *         root: true,
 *       },
 *       select: (account) => account.$isLoaded ? account.profile.name : "Loading...",
 *     }
 *   );
 *
 *   return <h1>{profileName}</h1>;
 * }
 * ```
 *
 * For more examples, see the [subscription and deep loading](https://jazz.tools/docs/react/using-covalues/subscription-and-loading) documentation.
 */
export function useAccountWithSelector<
  A extends AccountClass<Account> | AnyAccountSchema,
  TSelectorReturn,
  R extends ResolveQuery<A> = true,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Configuration for the subscription and selection */
  options: {
    /** Resolve query to specify which nested CoValues to load from the account */
    resolve?: ResolveQueryStrict<A, R>;
    /** Select which value to return from the account data */
    select: (account: MaybeLoaded<Loaded<A, R>>) => TSelectorReturn;
    /** Equality function to determine if the selected value has changed, defaults to `Object.is` */
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
    /**
     * Create or load a branch for isolated editing.
     *
     * Branching lets you take a snapshot of the current state and start modifying it without affecting the canonical/shared version.
     * It's a fork of your data graph: the same schema, but with diverging values.
     *
     * The checkout of the branch is applied on all the resolved values.
     *
     * @param name - A unique name for the branch. This identifies the branch
     *   and can be used to switch between different branches of the same CoValue.
     * @param owner - The owner of the branch. Determines who can access and modify
     *   the branch. If not provided, the branch is owned by the current user.
     *
     * For more info see the [branching](https://jazz.tools/docs/react/using-covalues/version-control) documentation.
     */
    unstable_branch?: BranchDefinition;
  },
): TSelectorReturn {
  const subscription = useAccountSubscription(AccountSchema, options);
  const getCurrentValue = useGetCurrentValue(subscription);

  return useSyncExternalStoreWithSelector<
    MaybeLoaded<Loaded<A, R>>,
    TSelectorReturn
  >(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    getCurrentValue,
    getCurrentValue,
    options.select,
    options.equalityFn ?? Object.is,
  );
}

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: string | undefined) {
  const context = useJazzContext();

  if (!("me" in context)) {
    throw new Error(
      "useInboxSender can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  const me = context.me;
  const inboxRef = useRef<Promise<InboxSender<I, O>> | undefined>(undefined);

  const sendMessage = useCallback(
    async (message: I) => {
      if (!inboxOwnerID) throw new Error("Inbox owner ID is required");

      if (!inboxRef.current) {
        const inbox = InboxSender.load<I, O>(inboxOwnerID, me);
        inboxRef.current = inbox;
      }

      let inbox = await inboxRef.current;

      // Regenerate the InboxSender if the inbox owner or current account changes
      if (inbox.owner.id !== inboxOwnerID || inbox.currentAccount !== me) {
        const req = InboxSender.load<I, O>(inboxOwnerID, me);
        inboxRef.current = req;
        inbox = await req;
      }

      return inbox.sendMessage(message);
    },
    [inboxOwnerID, me.$jazz.id],
  );

  return sendMessage;
}

/**
 * Hook that returns the current connection status to the Jazz sync server.
 *
 * @returns `true` when connected to the server, `false` when disconnected
 *
 * @remarks
 * On connection drop, this hook will return `false` only when Jazz detects the disconnection
 * after 5 seconds of not receiving a ping from the server.
 */
export function useSyncConnectionStatus() {
  const context = useJazzContext();

  const connected = useSyncExternalStore(
    useCallback(
      (callback) => {
        return context.addConnectionListener(callback);
      },
      [context],
    ),
    () => context.connected(),
    () => context.connected(),
  );

  return connected;
}
