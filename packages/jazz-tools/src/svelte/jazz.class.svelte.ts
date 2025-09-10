import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  BranchDefinition,
  CoValueClassOrSchema,
  CoValueFromRaw,
  InstanceOfSchema,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
} from "jazz-tools";
import {
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
} from "jazz-tools";
import { untrack } from "svelte";
import { createSubscriber } from "svelte/reactivity";
import { useIsAuthenticated } from "./auth/useIsAuthenticated.svelte.js";
import { getJazzContext } from "./jazz.svelte";

export class CoState<
  V extends CoValueClassOrSchema,
  R extends ResolveQuery<V> = true,
> {
  #value: Loaded<V, R> | undefined | null = undefined;
  #ctx = getJazzContext<InstanceOfSchema<AccountClass<Account>>>();
  #id: string | undefined | null;
  #subscribe: () => void;
  #update = () => {};
  #options: { resolve?: ResolveQueryStrict<V, R>, unstable_branch?: BranchDefinition } | undefined;

  constructor(
    Schema: V,
    id: string | undefined | null | (() => string | undefined | null),
    options?: { resolve?: ResolveQueryStrict<V, R>, unstable_branch?: BranchDefinition } | (() => { resolve?: ResolveQueryStrict<V, R>, unstable_branch?: BranchDefinition }),
  ) {
    this.#id = $derived.by(typeof id === "function" ? id : () => id);
    this.#options = $derived.by(typeof options === "function" ? options : () => options);

    this.#subscribe = createSubscriber((update) => {
      this.#update = update;
    });

    $effect.pre(() => {
      const ctx = this.#ctx.current;
      const id = this.#id;
      const options = this.#options;

      return untrack(() => {
        if (!ctx || !id) {
          return this.update(undefined);
        }
        const agent = "me" in ctx ? ctx.me : ctx.guest;

        const unsubscribe = subscribeToCoValue(
          coValueClassFromCoValueClassOrSchema(Schema),
          id,
          {
            // @ts-expect-error The resolve query type isn't compatible with the coValueClassFromCoValueClassOrSchema conversion
            resolve: options?.resolve,
            loadAs: agent,
            onUnavailable: () => {
              this.update(null);
            },
            onUnauthorized: () => {
              this.update(null);
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

  update(value: Loaded<V, R> | undefined | null) {
    if (this.#value === value) return;
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
  R extends ResolveQuery<A> = true,
> {
  #value: Loaded<A, R> | undefined | null = undefined;
  #ctx = getJazzContext<InstanceOfSchema<A>>();
  #subscribe: () => void;
  #update = () => {};

  constructor(Schema: A, options?: { resolve?: ResolveQueryStrict<A, R> }) {
    this.#subscribe = createSubscriber((update) => {
      this.#update = update;
    });

    $effect.pre(() => {
      const ctx = this.#ctx.current;

      return untrack(() => {
        if (!ctx || !("me" in ctx)) {
          return this.update(undefined);
        }

        const me = ctx.me;

        const unsubscribe = subscribeToCoValue(
          coValueClassFromCoValueClassOrSchema(Schema),
          me.$jazz.id,
          {
            // @ts-expect-error The resolve query type isn't compatible with the coValueClassFromCoValueClassOrSchema conversion
            resolve: options?.resolve,
            loadAs: me,
            onUnavailable: () => {
              this.update(null);
            },
            onUnauthorized: () => {
              this.update(null);
            },
            syncResolution: true,
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

  update(value: Loaded<A, R> | undefined | null) {
    if (this.#value === value) return;
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
