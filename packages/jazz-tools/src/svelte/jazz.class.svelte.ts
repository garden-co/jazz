import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  BranchDefinition,
  CoValue,
  CoValueClassOrSchema,
  CoValueFromRaw,
  SchemaResolveQuery,
  InstanceOfSchema,
  Loaded,
  MaybeLoaded,
  NotLoaded,
  ResolveQuery,
  ResolveQueryStrict,
} from "jazz-tools";
import {
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
  createUnloadedCoValue,
  CoValueLoadingState,
} from "jazz-tools";
import { untrack } from "svelte";
import { createSubscriber } from "svelte/reactivity";
import { useIsAuthenticated } from "./auth/useIsAuthenticated.svelte.js";
import { getJazzContext } from "./jazz.svelte";

type CoStateOptions<
  V extends CoValueClassOrSchema,
  R extends ResolveQuery<V>,
> = {
  resolve?: ResolveQueryStrict<V, R>;
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
   * For more info see the [branching](https://jazz.tools/docs/svelte/using-covalues/version-control) documentation.
   */
  unstable_branch?: BranchDefinition;
};

type CoStateId = string | undefined | null;

export class CoState<
  V extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<V> = SchemaResolveQuery<V>,
> {
  #value: MaybeLoaded<Loaded<V, R>> = createUnloadedCoValue(
    "",
    CoValueLoadingState.LOADING,
  );
  #previousValue: MaybeLoaded<CoValue> | undefined = undefined;
  #ctx = getJazzContext<InstanceOfSchema<AccountClass<Account>>>();
  #id: CoStateId;
  #subscribe: () => void;
  #update = () => {};
  #options: CoStateOptions<V, R> | undefined;

  constructor(
    Schema: V,
    id: CoStateId | (() => CoStateId),
    options?: CoStateOptions<V, R> | (() => CoStateOptions<V, R>),
  ) {
    this.#id = $derived.by(typeof id === "function" ? id : () => id);
    this.#options = $derived.by(
      typeof options === "function" ? options : () => options,
    );

    this.#subscribe = createSubscriber((update) => {
      this.#update = update;
    });

    $effect.pre(() => {
      const ctx = this.#ctx.current;
      const id = this.#id;
      const options = this.#options;

      return untrack(() => {
        if (!ctx || !id) {
          return this.update(
            createUnloadedCoValue(id ?? "", CoValueLoadingState.UNAVAILABLE),
          );
        }
        const agent = "me" in ctx ? ctx.me : ctx.guest;
        const resolve = getResolveQuery(Schema, options?.resolve);

        const unsubscribe = subscribeToCoValue(
          coValueClassFromCoValueClassOrSchema(Schema),
          id,
          {
            // @ts-expect-error The resolve query type isn't compatible with the coValueClassFromCoValueClassOrSchema conversion
            resolve,
            loadAs: agent,
            onUnavailable: () => {
              this.update(
                createUnloadedCoValue(id, CoValueLoadingState.UNAVAILABLE),
              );
            },
            onUnauthorized: () => {
              this.update(
                createUnloadedCoValue(id, CoValueLoadingState.UNAUTHORIZED),
              );
            },
            syncResolution: true,
            unstable_branch: options?.unstable_branch,
          },
          (value) => {
            this.update(value as Loaded<V, R>);
          },
        );

        return () => {
          unsubscribe();
        };
      });
    });
  }

  update(value: MaybeLoaded<Loaded<V, R>>) {
    if (shouldSkipUpdate(value, this.#value)) {
      return;
    }
    this.#previousValue = value;
    this.#value = value;
    this.#update();
  }

  get current() {
    this.#subscribe();
    return this.#value;
  }
}

export class AccountCoState<
  A extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<A> = SchemaResolveQuery<A>,
> {
  #value: MaybeLoaded<Loaded<A, R>> = createUnloadedCoValue(
    "",
    CoValueLoadingState.LOADING,
  );
  #ctx = getJazzContext<InstanceOfSchema<A>>();
  #subscribe: () => void;
  #options: CoStateOptions<A, R> | undefined;
  #update = () => {};

  constructor(
    Schema: A,
    options?: CoStateOptions<A, R> | (() => CoStateOptions<A, R>),
  ) {
    this.#options = $derived.by(
      typeof options === "function" ? options : () => options,
    );

    this.#subscribe = createSubscriber((update) => {
      this.#update = update;
    });

    $effect.pre(() => {
      const ctx = this.#ctx.current;
      const options = this.#options;

      return untrack(() => {
        if (!ctx || !("me" in ctx)) {
          return this.update(
            createUnloadedCoValue("", CoValueLoadingState.UNAVAILABLE),
          );
        }

        const me = ctx.me;
        const resolve = getResolveQuery(Schema, options?.resolve);

        const unsubscribe = subscribeToCoValue(
          coValueClassFromCoValueClassOrSchema(Schema),
          me.$jazz.id,
          {
            resolve,
            loadAs: me,
            onUnavailable: () => {
              this.update(
                createUnloadedCoValue(
                  me.$jazz.id,
                  CoValueLoadingState.UNAVAILABLE,
                ),
              );
            },
            onUnauthorized: () => {
              this.update(
                createUnloadedCoValue(
                  me.$jazz.id,
                  CoValueLoadingState.UNAUTHORIZED,
                ),
              );
            },
            syncResolution: true,
            unstable_branch: options?.unstable_branch,
          },
          (value) => {
            this.update(value as Loaded<A, R>);
          },
        );

        return () => {
          unsubscribe();
        };
      });
    });
  }

  update(value: MaybeLoaded<Loaded<A, R>>) {
    if (shouldSkipUpdate(value, this.#value)) return;
    this.#value = value;
    this.#update();
  }

  logOut = () => {
    this.#ctx.current?.logOut();
  };

  get current() {
    this.#subscribe();

    return this.#value;
  }

  get agent() {
    if (!this.#ctx.current) {
      throw new Error("No context found");
    }

    return "me" in this.#ctx.current
      ? this.#ctx.current.me
      : this.#ctx.current.guest;
  }

  #isAuthenticated = useIsAuthenticated();

  get isAuthenticated() {
    return this.#isAuthenticated.current;
  }
}

function shouldSkipUpdate(
  newValue: MaybeLoaded<CoValue>,
  previousValue: MaybeLoaded<CoValue>,
) {
  if (previousValue === newValue) return true;
  // Avoid re-renders if the value is not loaded and didn't change
  return (
    previousValue.$jazz.id === newValue.$jazz.id &&
    !previousValue.$isLoaded &&
    !newValue.$isLoaded &&
    previousValue.$jazz.loadingState === newValue.$jazz.loadingState
  );
}

/**
 * Class that provides the current connection status to the Jazz sync server.
 *
 * @returns `true` when connected to the server, `false` when disconnected
 *
 * @remarks
 * On connection drop, this will return `false` only when Jazz detects the disconnection
 * after 5 seconds of not receiving a ping from the server.
 */
export class SyncConnectionStatus {
  #ctx = getJazzContext<InstanceOfSchema<AccountClass<Account>>>();
  #subscribe: () => void;
  #update = () => {};

  constructor() {
    this.#subscribe = createSubscriber((update) => {
      this.#update = update;
    });

    $effect.pre(() => {
      const ctx = this.#ctx.current;

      return untrack(() => {
        if (!ctx) {
          return;
        }

        const unsubscribe = ctx.addConnectionListener(() => {
          this.#update();
        });

        return () => {
          unsubscribe();
        };
      });
    });
  }

  get current() {
    this.#subscribe();
    return this.#ctx.current?.connected() ?? false;
  }
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
