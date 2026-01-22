import { useSyncExternalStoreWithSelector } from "use-sync-external-store/shim/with-selector";
import React, {
  useCallback,
  useContext,
  useMemo,
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
  ExportedCoValue,
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  Loaded,
  MaybeLoaded,
  NotLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  SchemaResolveQuery,
  SubscriptionScope,
  importContentPieces,
  captureStack,
  getUnloadedCoValueWithoutId,
  type BranchDefinition,
} from "jazz-tools";
import { JazzContext } from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";
import { CoValueSubscription } from "./types.js";
import { use } from "./use.js";

export function useJazzContext<Acc extends Account>() {
  const value = useContext(JazzContext) as JazzContextManager<Acc, {}>;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useJazzContextValue<Acc extends Account>() {
  const contextManager = useJazzContext<Acc>();

  const context = useSyncExternalStore(
    useCallback(
      (callback) => {
        return contextManager.subscribe(callback);
      },
      [contextManager],
    ),
    () => contextManager.getCurrentValue(),
    () => contextManager.getCurrentValue(),
  );

  if (!context) {
    throw new Error(
      "The JazzProvider is not initialized yet. This looks like a bug, please report it.",
    );
  }

  return context;
}

export function useAuthSecretStorage() {
  return useJazzContext().getAuthSecretStorage();
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
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  Schema: S,
  id: string | undefined | null,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
    unstable_branch?: BranchDefinition;
  },
  source?: string,
): CoValueSubscription<S, R> | null {
  const resolve = getResolveQuery(Schema, options?.resolve);
  const subscriptions = useCoValueSubscriptions(
    Schema,
    [id],
    resolve,
    options?.unstable_branch,
    source,
  );
  return (subscriptions[0] ?? null) as CoValueSubscription<S, R> | null;
}

/**
 * Tracked state for the entire subscriptions array.
 * If any of the dependencies change, the subscriptions are recreated.
 */
interface SubscriptionsState {
  subscriptions: (SubscriptionScope<CoValue> | null)[];
  schema: CoValueClassOrSchema;
  ids: readonly (string | undefined | null)[];
  resolve: ResolveQuery<any>;
  contextManager: ReturnType<typeof useJazzContext>;
  agent: AnonymousJazzAgent | Loaded<any, true>;
  branchName?: string;
  branchOwnerId?: string;
}

/**
 * Internal hook that manages an array of SubscriptionScope instances.
 *
 * - Uses a ref to track subscriptions by index
 * - Detects changes by comparing schema/ids/resolve/branch
 * - Creates new subscriptions via SubscriptionScopeCache.getOrCreate()
 * - Returns null for entries with undefined/null IDs or invalid branches
 */
function useCoValueSubscriptions(
  schema: CoValueClassOrSchema,
  ids: readonly (string | undefined | null)[],
  resolve: ResolveQuery<any>,
  branch?: BranchDefinition,
  source?: string,
): (SubscriptionScope<CoValue> | null)[] {
  const contextManager = useJazzContext();
  const agent = useAgent();

  const callerStack = useMemo(() => captureStack(), []);

  const createAllSubscriptions = (): SubscriptionsState => {
    const node = contextManager.getCurrentValue()!.node;
    const cache = contextManager.getSubscriptionScopeCache();

    const subscriptions = ids.map((id) => {
      if (id === undefined || id === null) {
        return null;
      }

      const subscription = cache.getOrCreate(
        node,
        schema,
        id,
        resolve,
        false,
        false,
        branch,
      );

      if (callerStack) {
        subscription.callerStack = callerStack;
      }

      // Track performance for root subscriptions
      subscription.trackLoadingPerformance(source ?? "unknown");

      return subscription;
    });

    return {
      subscriptions,
      schema,
      ids,
      resolve,
      contextManager,
      agent,
      branchName: branch?.name,
      branchOwnerId: branch?.owner?.$jazz.id,
    };
  };

  const stateRef = React.useRef<SubscriptionsState | null>(null);
  const newSubscriptions = createAllSubscriptions();

  const state = stateRef.current;

  // Avoid recreating the subscriptions array if all subscriptions are already cached
  const anySubscriptionChanged =
    newSubscriptions.subscriptions.length !== state?.subscriptions.length ||
    newSubscriptions.subscriptions.some(
      (newSubscriptions, index) =>
        newSubscriptions !== state.subscriptions[index],
    );

  if (anySubscriptionChanged) {
    stateRef.current = newSubscriptions;
  }

  return stateRef.current!.subscriptions;
}

function useImportCoValueContent<V>(
  id: string | undefined | null,
  content?: ExportedCoValue<V>,
) {
  const agent = useAgent();
  const preloadExecuted = useRef<typeof agent | null>(null);
  if (content && preloadExecuted.current !== agent && id) {
    if (content.id === id) {
      importContentPieces(content.contentPieces, agent);
    } else {
      console.warn("Preloaded value ID does not match the subscription ID");
    }

    preloadExecuted.current = agent;
  }
}

function useGetCurrentValue<C extends CoValue>(
  subscription: SubscriptionScope<C> | null,
) {
  return useCallback(() => {
    if (!subscription) {
      return getUnloadedCoValueWithoutId(CoValueLoadingState.UNAVAILABLE);
    }

    return subscription.getCurrentValue();
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
 * @returns The loaded CoValue, or an {@link NotLoaded} value. Use `$isLoaded` to check whether the
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
 *       case "loading":
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
 *       case "loading":
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
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
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
    preloaded?: ExportedCoValue<Loaded<S, R>>;
  },
): TSelectorReturn {
  useImportCoValueContent(id, options?.preloaded);
  const subscription = useCoValueSubscription(
    Schema,
    id,
    options,
    `useCoState`,
  );
  return useSubscriptionSelector(subscription, options);
}

export function useSuspenseCoState<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = Loaded<S, R>,
>(
  /** The CoValue schema or class constructor */
  Schema: S,
  /** The ID of the CoValue to subscribe to */
  id: string,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load */
    resolve?: ResolveQueryStrict<S, R>;
    /** Select which value to return */
    select?: (value: Loaded<S, R>) => TSelectorReturn;
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
    preloaded?: ExportedCoValue<Loaded<S, R>>;
  },
): TSelectorReturn {
  useImportCoValueContent(id, options?.preloaded);

  const subscription = useCoValueSubscription(
    Schema,
    id,
    options,
    "useSuspenseCoState",
  );

  if (!subscription) {
    throw new Error("Subscription not found");
  }

  use(subscription.getCachedPromise());

  return useSubscriptionSelector(subscription, options);
}

/**
 * Returns a subscription's current value.
 * Allows to optionally select a subset of the subscription's value.
 *
 * This is the single-value counterpart to {@link useSubscriptionsSelector}.
 * Keeping it separate for performance reasons.
 */
export function useSubscriptionSelector<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  // Selector input can be an already loaded or a maybe-loaded value,
  // depending on whether a suspense hook is used or not, respectively.
  TSelectorInput = MaybeLoaded<Loaded<S, R>>,
  TSelectorReturn = TSelectorInput,
>(
  subscription: CoValueSubscription<S, R>,
  options?: {
    select?: (value: TSelectorInput) => TSelectorReturn;
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
  },
): TSelectorReturn {
  const getCurrentValue = useGetCurrentValue(subscription);

  return useSyncExternalStoreWithSelector(
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
    options?.select ?? ((value) => value as unknown as TSelectorReturn),
    options?.equalityFn ?? Object.is,
  );
}

export function useAccountSubscription<
  S extends AccountClass<Account> | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  Schema: S,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
    unstable_branch?: BranchDefinition;
  },
  source?: string,
) {
  const contextManager = useJazzContext();

  // Capture stack trace at hook call time
  const callerStack = useMemo(() => captureStack(), []);

  const createSubscription = () => {
    const agent = getCurrentAccountFromContextManager(contextManager);

    if (agent.$type$ === "Anonymous") {
      return {
        subscription: null,
        contextManager,
        agent,
      };
    }

    const resolve = getResolveQuery(Schema, options?.resolve);

    const node = contextManager.getCurrentValue()!.node;
    const cache = contextManager.getSubscriptionScopeCache();
    const subscription = cache.getOrCreate(
      node,
      Schema,
      agent.$jazz.id,
      resolve,
      false,
      false,
      options?.unstable_branch,
    );

    // Set callerStack on returned subscription after retrieval
    if (callerStack) {
      subscription.callerStack = callerStack;
    }

    // Track performance for root subscriptions
    subscription.trackLoadingPerformance(source ?? "unknown");

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
      // No need to manually destroy - cache handles cleanup via SubscriptionScope lifecycle
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      // No need to manually destroy - cache handles cleanup via SubscriptionScope lifecycle
      setSubscription(createSubscription());
    });
  }, [Schema, contextManager, branchName, branchOwnerId]);

  return subscription.subscription as CoValueSubscription<S, R>;
}

/**
 * React hook for accessing the current user's account.
 *
 * This hook provides access to the current user's account profile and root data.
 * It automatically handles the subscription lifecycle and supports deep loading of nested
 * CoValues through resolve queries.
 *
 * The {@param options.select} function allows returning only specific parts of the account data,
 * which helps reduce unnecessary re-renders by narrowing down the returned data.
 * Additionally, you can provide a custom {@param options.equalityFn} to further optimize
 * performance by controlling when the component should re-render based on the selected data.
 *
 * @returns The account data, or an {@link NotLoaded} value. Use `$isLoaded` to check whether the
 * CoValue is loaded, or use {@link MaybeLoaded.$jazz.loadingState} to get the detailed loading state.
 * If a selector function is provided, returns the result of the selector function.
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
 *   const profileName = useAccount(
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
 * @example
 * ```tsx
 * // Deep loading with resolve queries
 * function ProjectListWithDetails() {
 *   const me = useAccount(MyAppAccount, {
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
 *       case "loading":
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
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<A> = SchemaResolveQuery<A>,
  TSelectorReturn = MaybeLoaded<Loaded<A, R>>,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load from the account */
    resolve?: ResolveQueryStrict<A, R>;
    /** Select which value to return from the account data */
    select?: (account: MaybeLoaded<Loaded<A, R>>) => TSelectorReturn;
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
  const subscription = useAccountSubscription(
    AccountSchema,
    options,
    "useAccount",
  );
  return useSubscriptionSelector(subscription, options);
}

export function useSuspenseAccount<
  A extends AccountClass<Account> | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<A> = SchemaResolveQuery<A>,
  TSelectorReturn = Loaded<A, R>,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load from the account */
    resolve?: ResolveQueryStrict<A, R>;
    /** Select which value to return from the account data */
    select?: (account: Loaded<A, R>) => TSelectorReturn;
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
  const subscription = useAccountSubscription(
    AccountSchema,
    options,
    "useSuspenseAccount",
  );

  if (!subscription) {
    throw new Error(
      "Subscription not found, are you using useSuspenseAccount in guest mode?",
    );
  }

  use(subscription.getCachedPromise());

  return useSubscriptionSelector(subscription, options);
}

/**
 * Returns a function for logging out of the current account.
 */
export function useLogOut(): () => void {
  const contextManager = useJazzContext();
  return contextManager.logOut;
}

/**
 * React hook for accessing the current agent. An agent can either be:
 * - an Authenticated Account, if the user is logged in
 * - an Anonymous Account, if the user didn't log in
 * - or an anonymous agent, if in guest mode
 *
 * The agent can be used as the `loadAs` parameter for load and subscribe methods.
 */
export function useAgent<
  A extends AccountClass<Account> | AnyAccountSchema = typeof Account,
>(): AnonymousJazzAgent | Loaded<A, true> {
  const contextManager = useJazzContext<InstanceOfSchema<A>>();

  const getCurrentValue = () =>
    getCurrentAccountFromContextManager(contextManager) as
      | AnonymousJazzAgent
      | Loaded<A, true>;

  return React.useSyncExternalStore(
    useCallback(
      (callback) => {
        return contextManager.subscribe(callback);
      },
      [contextManager],
    ),
    getCurrentValue,
    getCurrentValue,
  );
}

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: string | undefined) {
  const context = useJazzContextValue();

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
  const context = useJazzContextValue();

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

function getResolveQuery(
  Schema: CoValueClassOrSchema,
  // We don't need type validation here, since this is an internal API
  resolveQuery?: ResolveQuery<any>,
): ResolveQuery<any> {
  if (resolveQuery) {
    return resolveQuery;
  }
  // Check the schema is a CoValue schema (and not a CoValue class)
  if ("resolveQuery" in Schema) {
    return Schema.resolveQuery;
  }
  return true;
}

/**
 * Internal hook that suspends until all values are loaded.
 *
 * - Creates a Promise.all from individual getCachedPromise() calls
 * - Returns Promise.resolve(null) for null subscriptions (undefined/null IDs)
 * - Suspends via the use() hook until all values are loaded
 */
function useSuspendUntilLoaded(
  subscriptions: (SubscriptionScope<CoValue> | null)[],
): void {
  const combinedPromise = useMemo(() => {
    const promises = subscriptions.map((sub) => {
      if (!sub) {
        // For null subscriptions (undefined/null IDs), resolve immediately with null
        return Promise.resolve(null);
      }
      return sub.getCachedPromise();
    });

    return Promise.all(promises);
  }, [subscriptions]);

  use(combinedPromise);
}

/**
 * Internal hook that uses useSyncExternalStore to subscribe to multiple SubscriptionScopes.
 *
 * - Creates a combined subscribe function that subscribes to all scopes
 * - Returns an array of current values from each scope
 * - Maintains stable references for unchanged values
 *
 * @param subscriptions - Array of SubscriptionScope instances (or null for skipped entries)
 * @returns Array of loaded CoValues (or null for skipped entries)
 */
function useSubscriptionsSelector<
  T extends CoValue[] | MaybeLoaded<CoValue>[],
  // Selector input can be an already loaded or a maybe-loaded value,
  // depending on whether a suspense hook is used or not, respectively.
  TSelectorInput = T[number],
  TSelectorReturn = TSelectorInput,
>(
  subscriptions: SubscriptionScope<CoValue>[],
  options?: {
    select?: (value: TSelectorInput) => TSelectorReturn;
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
  },
): TSelectorReturn[] {
  // Combined subscribe function that subscribes to all scopes
  const subscribe = useCallback(
    (callback: () => void) => {
      const unsubscribes = subscriptions.map((sub) => sub.subscribe(callback));

      return () => {
        unsubscribes.forEach((unsub) => unsub());
      };
    },
    [subscriptions],
  );

  // Cache current values to avoid infinite loops
  const cachedCurrentValuesRef = useRef<T>([] as unknown as T);
  const getCurrentValues = useCallback(() => {
    const newValues = subscriptions.map((sub) => sub.getCurrentValue());

    // Check if values have changed by comparing each element
    const cached = cachedCurrentValuesRef.current;
    const hasChanged =
      cached.length !== newValues.length ||
      newValues.some((value, index) => value !== cached[index]);

    if (hasChanged) {
      cachedCurrentValuesRef.current = newValues as T;
    }

    return cachedCurrentValuesRef.current as unknown as TSelectorInput[];
  }, [subscriptions]);

  const selectFn = useMemo(() => {
    if (!options?.select) {
      return (values: TSelectorInput[]) =>
        values as unknown as TSelectorReturn[];
    }
    return (values: TSelectorInput[]) =>
      values.map((value) => options.select!(value));
  }, [options?.select]);

  const elementEqualityFn = useMemo(
    () => options?.equalityFn ?? Object.is,
    [options?.equalityFn],
  );
  const equalityFn = useMemo(() => {
    return (a: TSelectorReturn[], b: TSelectorReturn[]) =>
      a.length === b.length &&
      a.every((value, index) => elementEqualityFn(value, b[index]));
  }, [elementEqualityFn]);

  return useSyncExternalStoreWithSelector(
    subscribe,
    getCurrentValues,
    getCurrentValues,
    selectFn,
    equalityFn,
  );
}

/**
 * Subscribe to multiple CoValues with unified Suspense handling.
 *
 * This hook accepts a list of CoValue IDs and returns an array of loaded values,
 * suspending until all values are available.
 *
 * @param Schema - The CoValue schema or class constructor
 * @param ids - Array of CoValue IDs to subscribe to
 * @param options - Optional configuration, including resolve query
 * @returns An array of loaded CoValues in the same order as the input IDs
 */
export function useSuspenseCoStates<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = Loaded<S, R>,
>(
  Schema: S,
  ids: readonly string[],
  options?: {
    /** Resolve query to specify which nested CoValues to load */
    resolve?: ResolveQueryStrict<S, R>;
    /** Select which value to return. Applies to each element individually. */
    select?: (value: Loaded<S, R>) => TSelectorReturn;
    /** Equality function to determine if a selected value has changed, defaults to `Object.is` */
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
): TSelectorReturn[] {
  const resolve = getResolveQuery(Schema, options?.resolve);
  const subscriptionScopes = useCoValueSubscriptions(
    Schema,
    ids,
    resolve,
    options?.unstable_branch,
    "useSuspenseCoStates",
  ) as SubscriptionScope<CoValue>[];
  useSuspendUntilLoaded(subscriptionScopes);
  return useSubscriptionsSelector(subscriptionScopes, options);
}

/**
 * Subscribe to multiple CoValues without Suspense.
 *
 * This hook accepts a list of CoValue IDs and returns an array of maybe-loaded values.
 * Unlike `useSuspenseCoStates`, this hook does not suspend and returns loading/unavailable
 * states that can be checked via the `$isLoaded` property.
 *
 * @param Schema - The CoValue schema or class constructor
 * @param ids - Array of CoValue IDs to subscribe to
 * @param options - Optional configuration, including resolve query
 * @returns An array of MaybeLoaded CoValues in the same order as the input IDs
 *
 * @example
 * ```typescript
 * const [project1, project2] = useCoStates(
 *   ProjectSchema,
 *   [projectId1, projectId2],
 *   { resolve: { assignee: true } }
 * );
 *
 * if (!project1.$isLoaded || !project2.$isLoaded) {
 *   return <Loading />;
 * }
 * ```
 */
export function useCoStates<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
>(
  Schema: S,
  ids: readonly string[],
  options?: {
    /** Resolve query to specify which nested CoValues to load */
    resolve?: ResolveQueryStrict<S, R>;
    /** Select which value to return. Applies to each element individually. */
    select?: (value: MaybeLoaded<Loaded<S, R>>) => TSelectorReturn;
    /** Equality function to determine if a selected value has changed, defaults to `Object.is` */
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
): TSelectorReturn[] {
  const resolve = getResolveQuery(Schema, options?.resolve);
  const subscriptionScopes = useCoValueSubscriptions(
    Schema,
    ids,
    resolve,
    options?.unstable_branch,
    "useCoStates",
  ) as SubscriptionScope<CoValue>[];
  return useSubscriptionsSelector(subscriptionScopes, options);
}
