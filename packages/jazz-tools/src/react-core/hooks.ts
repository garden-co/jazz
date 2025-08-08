import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  CoValue,
  CoValueClassOrSchema,
  coValueClassFromCoValueClassOrSchema,
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  SubscriptionScope,
} from "jazz-tools";
import React, {
  useCallback,
  useContext,
  useRef,
  useSyncExternalStore,
} from "react";
import { JazzContext, JazzContextManagerContext } from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";

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

function useCoValueSubscription<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  id: string | undefined | null,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
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

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(
      node,
      options?.resolve ?? true,
      id,
      {
        ref: coValueClassFromCoValueClassOrSchema(Schema),
        optional: true,
      },
    );

    return {
      subscription,
      contextManager,
      id,
      Schema,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.id !== id ||
      subscription.Schema !== Schema
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, id, contextManager]);

  return subscription.subscription;
}

/**
 * React hook for subscribing to CoValues and handling loading states.
 *
 * This hook provides a convenient way to subscribe to CoValues and automatically
 * handles the subscription lifecycle (subscribe on mount, unsubscribe on unmount).
 * It also supports deep loading of nested CoValues through resolve queries.
 *
 * @returns The loaded CoValue, or `undefined` if loading, or `null` if not found/not accessible
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
 *   if (!project) {
 *     return project === null
 *       ? "Project not found or not accessible"
 *       : "Loading project...";
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
 *       subtasks: { $each: { $onError: null } },
 *     },
 *   });
 *
 *   if (!task) {
 *     return task === null
 *       ? "Task not found or not accessible"
 *       : "Loading task...";
 *   }
 *
 *   return (
 *     <div>
 *       <h2>{task.title}</h2>
 *       {task.assignee && <p>Assigned to: {task.assignee.name}</p>}
 *       <ul>
 *         {task.subtasks.map((subtask, index) => (
 *           subtask ? <li key={subtask.id}>{subtask.title}</li> : <li key={index}>Inaccessible subtask</li>
 *         ))}
 *       </ul>
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
>(
  /** The CoValue schema or class constructor */
  Schema: S,
  /** The ID of the CoValue to subscribe to. If `undefined`, returns `null` */
  id: string | undefined,
  /** Optional configuration for the subscription */
  options?: {
    /** Resolve query to specify which nested CoValues to load */
    resolve?: ResolveQueryStrict<S, R>;
  },
): Loaded<S, R> | undefined | null {
  const subscription = useCoValueSubscription(Schema, id, options);

  const value = React.useSyncExternalStore<Loaded<S, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    () => (subscription ? subscription.getCurrentValue() : null),
    () => (subscription ? subscription.getCurrentValue() : null),
  );

  return value;
}

function useAccountSubscription<
  S extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
  },
) {
  const contextManager = useJazzContextManager();

  const createSubscription = () => {
    const agent = getCurrentAccountFromContextManager(contextManager);

    if (agent._type === "Anonymous") {
      return {
        subscription: null,
        contextManager,
        agent,
      };
    }

    // We don't need type validation here, since it's mostly to help users on public API
    const resolve: any = options?.resolve ?? true;

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(node, resolve, agent.id, {
      ref: coValueClassFromCoValueClassOrSchema(Schema),
      optional: true,
    });

    return {
      subscription,
      contextManager,
      Schema,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.Schema !== Schema
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, contextManager]);

  return subscription.subscription;
}

/**
 * React hook for accessing the current user's account and authentication state.
 * 
 * This hook provides access to the current user's account profile and root data,
 * along with authentication utilities. It automatically handles subscription to
 * the user's account data and provides a logout function.
 * 
 * @returns An object containing:
 * - `me`: The loaded account data, or `undefined` if loading, or `null` if not authenticated
 * - `agent`: The current agent (anonymous or authenticated user). Can be used as `loadAs` parameter for load and subscribe methods.
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
 *   if (!me) {
 *     return me === null
 *       ? <div>Failed to load your projects</div>
 *       : <div>Loading...</div>;
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
  },
): {
  me: Loaded<A, R> | undefined | null;
  agent: AnonymousJazzAgent | Loaded<A, true>;
  logOut: () => void;
} {
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const subscription = useAccountSubscription(AccountSchema, options);

  const agent = getCurrentAccountFromContextManager(contextManager);

  const value = React.useSyncExternalStore<Loaded<A, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    () => (subscription ? subscription.getCurrentValue() : null),
    () => (subscription ? subscription.getCurrentValue() : null),
  );

  return {
    me: value,
    agent,
    logOut: contextManager.logOut,
  };
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

      if (inbox.owner.id !== inboxOwnerID) {
        const req = InboxSender.load<I, O>(inboxOwnerID, me);
        inboxRef.current = req;
        inbox = await req;
      }

      return inbox.sendMessage(message);
    },
    [inboxOwnerID],
  );

  return sendMessage;
}
