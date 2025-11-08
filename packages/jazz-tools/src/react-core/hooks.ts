import { useSyncExternalStoreWithSelector } from "use-sync-external-store/shim/with-selector";
import React, {
  useCallback,
  useContext,
  useEffect,
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
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  Loaded,
  MaybeLoaded,
  NotLoaded,
  ResolveQuery,
  SchemaResolveQuery,
  SubscriptionScope,
  coValueClassFromCoValueClassOrSchema,
  createUnloadedCoValue,
} from "jazz-tools";
import { JazzContext, JazzContextManagerContext } from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";
import {
  CoValueSubscription,
  UseCoValueOptions,
  UseSubscriptionOptions,
  UseSubscriptionSelectorOptions,
  CoValueRef,
  MaybeLoadedCoValueRef,
} from "./types.js";

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
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  Schema: S,
  id: string | undefined | null,
  options?: UseSubscriptionOptions<S, R>,
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

    const resolve = getResolveQuery(Schema, options?.resolve);

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(
      node,
      resolve,
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
  options?: UseCoValueOptions<S, R, TSelectorReturn>,
): TSelectorReturn {
  const subscription = useCoValueSubscription(Schema, id, options);

  return useSubscriptionSelector<S, R, TSelectorReturn>(subscription, options);
}

// Fallback selector with stable reference for useSyncExternalStoreWithSelector
function identitySelector(value: any) {
  return value;
}

/**
 * React hook for selecting data from a subscription object.
 *
 * This hook is used to select and transform data from a subscription object returned by
 * {@link useCoValueSubscription} or {@link useAccountSubscription}. It allows you to
 * extract only the specific parts of the subscribed data that you need, which helps
 * reduce unnecessary re-renders by narrowing down the returned data.
 *
 * The {@param options.select} function allows returning only specific parts of the subscription data.
 * Additionally, you can provide a custom {@param options.equalityFn} to further optimize
 * performance by controlling when the component should re-render based on the selected data.
 *
 * @returns The selected data from the subscription. If no selector is provided, returns the
 * full subscription value. If a selector function is provided, returns the result of the selector function.
 *
 * @example
 * ```tsx
 * // Select only specific fields from a subscription
 * const Project = co.map({
 *   name: z.string(),
 *   description: z.string(),
 *   tasks: co.list(Task),
 *   lastModified: z.date(),
 * });
 *
 * function ProjectTitle({ projectId }: { projectId: string }) {
 *   const subscription = useCoValueSubscription(Project, projectId);
 *
 *   // Only re-render when the project name changes, not other fields
 *   const projectName = useSubscriptionSelector(subscription, {
 *     select: (project) => project.$isLoaded ? project.name : "Loading...",
 *   });
 *
 *   return <h1>{projectName}</h1>;
 * }
 * ```
 *
 * @example
 * ```tsx
 * // Use custom equality function for complex data structures
 * const TaskList = co.list(Task);
 *
 * function TaskCount({ listId }: { listId: string }) {
 *   const subscription = useCoValueSubscription(TaskList, listId, {
 *     resolve: { $each: true },
 *   });
 *
 *   const taskStats = useSubscriptionSelector(subscription, {
 *     select: (tasks) => {
 *       if (!tasks.$isLoaded) return { total: 0, completed: 0 };
 *       return {
 *         total: tasks.length,
 *         completed: tasks.filter(task => task.completed).length,
 *       };
 *     },
 *     // Custom equality to prevent re-renders when stats haven't changed
 *     equalityFn: (a, b) => a.total === b.total && a.completed === b.completed,
 *   });
 *
 *   return (
 *     <div>
 *       {taskStats.completed} of {taskStats.total} tasks completed
 *     </div>
 *   );
 * }
 * ```
 */
export function useSubscriptionSelector<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
>(
  subscription: CoValueSubscription<S, R>,
  options?: UseSubscriptionSelectorOptions<S, R, TSelectorReturn>,
) {
  const getCurrentValue = useGetCurrentValue(subscription);

  return useSyncExternalStoreWithSelector<
    MaybeLoaded<Loaded<S, R>>,
    TSelectorReturn
  >(
    useCallback(
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
    options?.select ?? identitySelector,
    options?.equalityFn ?? Object.is,
  );
}

export function useAccountSubscription<
  S extends AccountClass<Account> | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(Schema: S, options?: UseSubscriptionOptions<S, R>) {
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

    const resolve = getResolveQuery(Schema, options?.resolve);

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
  options?: UseCoValueOptions<A, R, TSelectorReturn>,
): TSelectorReturn {
  const subscription = useAccountSubscription(AccountSchema, options);

  return useSubscriptionSelector<A, R, TSelectorReturn>(subscription, options);
}

export function useSubscriptionRef<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<S> = SchemaResolveQuery<S>,
>(
  subscription: CoValueSubscription<S, R>,
  options?: {
    onUpdate?: (value: MaybeLoaded<Loaded<S, R>>) => void;
  },
): CoValueRef<MaybeLoaded<Loaded<S, R>>> {
  const subscriptionRef = useRef(subscription);
  const coValueRef = useRef<MaybeLoaded<Loaded<S, R>>>(undefined);
  const sendUpdateRef = useRef(options?.onUpdate);

  const getCurrentCoValue = useCallback(() => {
    if (coValueRef.current !== undefined) {
      return coValueRef.current;
    }

    const currentCoValue = getSubscriptionValue(subscriptionRef.current);
    coValueRef.current = currentCoValue;
    return currentCoValue;
  }, []);

  useEffect(() => {
    sendUpdateRef.current = options?.onUpdate;
  }, [options?.onUpdate]);

  useEffect(() => {
    subscriptionRef.current = subscription;
    coValueRef.current = getSubscriptionValue(subscription);

    if (!subscription) {
      return () => {};
    }

    return subscription.subscribe((value) => {
      const updatedValue =
        value.type === CoValueLoadingState.LOADED
          ? value.value
          : createUnloadedCoValue(value.id ?? "", value.type);

      coValueRef.current = updatedValue;
      sendUpdateRef.current?.(updatedValue);
    });
  }, [subscription]);

  // Use getter function to ensure `.current` cannot be re-assigned by the user
  return Object.defineProperty(
    {} as CoValueRef<MaybeLoaded<Loaded<S, R>>>,
    "current",
    {
      get: getCurrentCoValue,
    },
  );
}

/**
 * React hook that returns a ref containing the latest value of a CoValue.
 *
 * This hook provides a React ref that always contains the most up-to-date value of a CoValue.
 * Unlike {@link useCoState}, this hook should **not** be used for any data needed for rendering,
 * as changes to the ref do not trigger re-renders. Instead, use this hook for accessing CoValue
 * data in event handlers, callbacks, or other imperative code where you need the latest value
 * without causing re-renders.
 *
 * The ref is automatically updated whenever the CoValue changes, but these updates do not
 * trigger component re-renders. This hook is ideal for accessing the `$jazz` object when you
 * want to edit a CoValue without having to return the `$jazz` object or its functions through
 * {@link useCoState}, which would complicate controlling the reactivity of the component.
 *
 * @returns A React ref object containing the latest CoValue data. The ref's `current` property
 * will be updated automatically as the CoValue changes.
 *
 * @example
 * ```tsx
 * // Access CoValue data in event handlers without subscribing to it for rendering
 * const Task = co.map({
 *   title: z.string(),
 *   completed: z.boolean(),
 *   metadata: co.map({
 *     createdAt: z.date(),
 *     tags: co.list(z.string()),
 *     priority: z.number(),
 *   }),
 * });
 *
 * function TaskItem({ taskId }: { taskId: string }) {
 *   // Only subscribe to the fields we need for rendering
 *   const task = useCoState(Task, taskId, {
 *     select: (task) => task.$isLoaded ? {
 *       title: task.title,
 *       completed: task.completed,
 *     } : null,
 *     equalityFn: (a, b) => a?.title === b?.title && a?.completed === b?.completed,
 *   });
 *
 *   // Use ref to access metadata without causing re-renders when it changes
 *   const taskRef = useCoValueRef(Task, taskId, {
 *     resolve: { metadata: true },
 *   });
 *
 *   const handleAnalytics = () => {
 *     // Access metadata that we don't render
 *     const currentTask = taskRef.current;
 *     if (currentTask.$isLoaded) {
 *       analytics.track('task_clicked', {
 *         taskId: currentTask.id,
 *         priority: currentTask.metadata.priority,
 *         tags: currentTask.metadata.tags,
 *         createdAt: currentTask.metadata.createdAt,
 *       });
 *     }
 *   };
 *
 *   if (!task) return <div>Loading...</div>;
 *
 *   return (
 *     <div onClick={handleAnalytics}>
 *       <h3>{task.title}</h3>
 *       <input
 *         type="checkbox"
 *         checked={task.completed}
 *         onChange={(e) => {
 *           const currentTask = taskRef.current;
 *           if (currentTask.$isLoaded) {
 *             currentTask.$jazz.set("completed", e.target.checked);
 *           }
 *         }}
 *       />
 *     </div>
 *   );
 * }
 * ```
 */
export function useCoValueRef<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
>(
  /** The CoValue schema or class constructor */
  Schema: S,
  /** The ID of the CoValue to subscribe to. If `undefined`, returns the result of selector called with `null` */
  id: string | undefined,
  /** Optional configuration for the subscription */
  options?: UseSubscriptionOptions<S, R>,
): CoValueRef<MaybeLoaded<Loaded<S, R>>> {
  const subscription = useCoValueSubscription(Schema, id, options);

  return useSubscriptionRef(subscription);
}

/**
 * React hook that returns a ref containing the latest value of the current user's account.
 *
 * This hook provides a React ref that always contains the most up-to-date value of the current
 * user's profile and root data. Unlike {@link useAccount}, this hook should **not** be used for
 * any data needed for rendering, as changes to the ref do not trigger re-renders. Instead, use
 * this hook for accessing account data in event handlers, callbacks, or other imperative code
 * where you need the latest value without causing re-renders.
 *
 * The ref is automatically updated whenever the account data changes, but these updates do not
 * trigger component re-renders. This hook is ideal for accessing the `$jazz` object when you
 * want to edit the account without causing re-renders if the account data is not used for
 * rendering.
 *
 * @returns A React ref object containing the latest account data. The ref's `current` property
 * will be updated automatically as the account changes.
 *
 * @example
 * ```tsx
 * // Access account data in event handlers without subscribing to it for rendering
 * const MyAppAccount = co.account({
 *   profile: co.profile(),
 *   root: co.map({
 *     projects: co.list(Project),
 *     settings: co.map({
 *       defaultProjectTemplate: z.string(),
 *       autoSave: z.boolean(),
 *     }),
 *   }),
 * });
 *
 * function CreateProjectButton() {
 *   // Only subscribe to profile name for rendering
 *   const profileName = useAccount(MyAppAccount, {
 *     resolve: { profile: true },
 *     select: (me) => me.$isLoaded ? me.profile.name : 'Guest',
 *   });
 *
 *   // Use ref to access projects and settings without subscribing for rendering
 *   const meRef = useAccountRef(MyAppAccount, {
 *     resolve: {
 *       root: {
 *         projects: true,
 *         settings: true,
 *       },
 *     },
 *   });
 *
 *   const handleCreateProject = () => {
 *     const currentAccount = meRef.current;
 *     if (currentAccount.$isLoaded) {
 *       // Use settings that we don't render
 *       const template = currentAccount.root.settings.defaultProjectTemplate;
 *
 *       const newProject = Project.create({
 *         name: 'New Project',
 *         template: template,
 *         owner: currentAccount.id,
 *       });
 *
 *       currentAccount.root.projects.$jazz.push(newProject);
 *     }
 *   };
 *
 *   return (
 *     <button onClick={handleCreateProject}>
 *       Create Project as {profileName}
 *     </button>
 *   );
 * }
 * ```
 */
export function useAccountRef<
  A extends AccountClass<Account> | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<A> = SchemaResolveQuery<A>,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Optional configuration for the subscription */
  options?: UseSubscriptionOptions<A, R>,
): CoValueRef<MaybeLoaded<Loaded<A, R>>> {
  const subscription = useAccountSubscription(AccountSchema, options);

  return useSubscriptionRef(subscription);
}

/**
 * Returns both a reactive state and a ref for the same CoValue.
 *
 * This hook combines `useCoState` and `useCoValueRef` to provide:
 * - **State**: Reactive value that triggers re-renders only when selected fields change
 * - **Ref**: Non-reactive reference for accessing the full CoValue in callbacks/event handlers
 *
 * This is useful when you need to:
 * - Display specific CoValue data in your component (using state with a selector)
 * - Edit other parts of the CoValue in event handlers without causing re-renders (using ref.$jazz)
 *
 * @example
 * ```tsx
 * // Render count of items in a list while using ref to append without re-rendering
 * const TodoItem = co.map({
 *   text: z.string(),
 *   completed: z.boolean(),
 * });
 *
 * const TodoList = co.map({
 *   name: z.string(),
 *   items: co.list(TodoItem),
 * });
 *
 * function TodoListComponent({ listId }: { listId: string }) {
 *   const [listName, listRef] = useCoStateAndRef(TodoList, listId, {
 *     select: (list) => user.$isLoaded ? list.name : undefined,
 *   });
 *
 *   const handleAddItem = (text: string) => {
 *     const currentList = listRef.current;
 *     if (currentList.$isLoaded) {
 *       const newItem = TodoItem.create({
 *         text,
 *         completed: false,
 *       });
 *       currentList.items.$jazz.push(newItem);
 *     }
 *   };
 *
 *   if (listName === undefined) {
 *     return <div>Loading...</div>;
 *   }
 *
 *   return (
 *     <div>
 *       <h2>Add Item to {listName}</h2>
 *       <button onClick={() => handleAddItem('New task')}>
 *         Add Item
 *       </button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useCoStateAndRef<
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
  options?: UseCoValueOptions<S, R, TSelectorReturn>,
): [TSelectorReturn, MaybeLoadedCoValueRef<Loaded<S, R>>] {
  const subscription = useCoValueSubscription(Schema, id, options);
  const [isLoaded, setIsLoaded] = React.useState(
    () => getSubscriptionValue(subscription).$isLoaded,
  );

  const onUpdate = useCallback((value: MaybeLoaded<Loaded<S, R>>) => {
    setIsLoaded(value.$isLoaded);
  }, []);

  const ref = useSubscriptionRef(subscription, {
    onUpdate,
  });

  return [
    useSubscriptionSelector<S, R, TSelectorReturn>(subscription, options),
    Object.defineProperty(ref, "$isLoaded", {
      value: isLoaded,
    }) as MaybeLoadedCoValueRef<Loaded<S, R>>,
  ];
}

/**
 * Returns both a reactive state and a ref for the current account.
 *
 * This hook combines `useAccount` and `useAccountRef` to provide:
 * - **State**: Reactive value that triggers re-renders only when selected fields change
 * - **Ref**: Non-reactive reference for accessing the full account in callbacks/event handlers
 *
 * This is useful when you need to:
 * - Display specific account data in your component (using state with a selector)
 * - Edit other account properties or use the $jazz API in event handlers without causing re-renders
 *
 * @example
 * ```tsx
 * function AccountSettings() {
 *   // Only re-render when profile.name changes
 *   const [profileName, accountRef] = useAccountAndRef(Account, {
 *     resolve: { profile: true },
 *     select: (account) => account.$isLoaded ? account.profile.name : undefined,
 *   });
 *
 *   if (profileName === undefined) {
 *     return <div>Loading account...</div>;
 *   }
 *
 *   return (
 *     <div>
 *       <h1>Welcome, {profileName}</h1>
 *       <button
 *         onClick={() => {
 *           const currentAccount = accountRef.current;
 *           if (currentAccount.$isLoaded) {
 *             const currentStatus = currentAccount.profile.status;
 *             currentAccount.profile.$jazz.set("status", currentStatus === "active" ? "inactive" : "active");
 *           }
 *         }}
 *       >
 *         Toggle Status
 *       </button>
 *     </div>
 *   );
 * }
 * ```
 */
export function useAccountAndRef<
  A extends AccountClass<Account> | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  const R extends ResolveQuery<A> = SchemaResolveQuery<A>,
  TSelectorReturn = MaybeLoaded<Loaded<A, R>>,
>(
  /** The account schema to use. Defaults to the base Account schema */
  AccountSchema: A = Account as unknown as A,
  /** Optional configuration for the subscription */
  options?: UseCoValueOptions<A, R, TSelectorReturn>,
): [TSelectorReturn, CoValueRef<MaybeLoaded<Loaded<A, R>>>] {
  const subscription = useAccountSubscription(AccountSchema, options);
  const [isLoaded, setIsLoaded] = React.useState(
    () => getSubscriptionValue(subscription).$isLoaded,
  );

  const onUpdate = useCallback((value: MaybeLoaded<Loaded<A, R>>) => {
    setIsLoaded(value.$isLoaded);
  }, []);

  const ref = useSubscriptionRef(subscription, {
    onUpdate,
  });

  return [
    useSubscriptionSelector<A, R, TSelectorReturn>(subscription, options),
    Object.defineProperty(ref, "$isLoaded", {
      value: isLoaded,
    }) as MaybeLoadedCoValueRef<Loaded<A, R>>,
  ];
}

/**
 * Returns a function for logging out of the current account.
 */
export function useLogOut(): () => void {
  const contextManager = useJazzContextManager();
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
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const agent = getCurrentAccountFromContextManager(contextManager);
  return agent as AnonymousJazzAgent | Loaded<A, true>;
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
