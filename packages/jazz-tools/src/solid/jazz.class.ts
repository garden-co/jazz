import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  CoValueOrZodSchema,
  InstanceOfSchema,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
} from "jazz-tools";
import { anySchemaToCoSchema, subscribeToCoValue } from "jazz-tools";
import {
  Accessor,
  createEffect,
  createSignal,
  onCleanup,
  untrack,
} from "solid-js";
import { useJazzContext } from "./context/jazz.js";
import { useIsAuthenticated } from "./hooks/useIsAuthenticated.js";

type Nilable<T> = T | undefined | null;

export class CoState<
  V extends CoValueOrZodSchema,
  R extends ResolveQuery<V> = true,
> {
  #value: Loaded<V, R> | undefined | null = undefined;
  #ctx = useJazzContext<InstanceOfSchema<AccountClass<Account>>>();
  #listeners = new Set<() => void>();

  constructor(
    Schema: V,
    id: Nilable<string | Accessor<Nilable<string>>> = undefined,
    options?: { resolve?: ResolveQueryStrict<V, R> },
  ) {
    const _id = () => (typeof id === "function" ? id() : id);

    // Effect to handle subscriptions
    createEffect(() => {
      const ctx = this.#ctx();
      const currentId = _id();

      untrack(() => {
        if (!ctx || !currentId) {
          return this.update(undefined);
        }

        const agent = "me" in ctx ? ctx.me : ctx.guest;

        const unsubscribe = subscribeToCoValue(
          anySchemaToCoSchema(Schema),
          currentId,
          {
            // @ts-expect-error The resolve query type isn't compatible with the anySchemaToCoSchema conversion
            resolve: options?.resolve,
            loadAs: agent,
            onUnavailable: () => {
              this.update(null);
            },
            onUnauthorized: () => {
              this.update(null);
            },
            syncResolution: true,
          },
          (value) => {
            this.update(value as Loaded<V, R>);
          },
        );

        onCleanup(() => {
          unsubscribe();
        });
      });
    });
  }

  update(value: Loaded<V, R> | undefined | null) {
    if (this.#value === value) return;
    this.#value = value;
    // Notify all listeners
    this.#listeners.forEach((listener) => listener());
  }

  get current() {
    // Create a signal to track this access
    const [, setTrigger] = createSignal(0, { equals: false });

    // Add this component to listeners if not already added
    const listener = () => setTrigger(0);
    this.#listeners.add(listener);

    // Clean up listener when component unmounts
    onCleanup(() => {
      this.#listeners.delete(listener);
    });

    return this.#value;
  }
}

export class AccountCoState<
  A extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
  R extends ResolveQuery<A> = true,
> {
  #value: Nilable<Loaded<A, R>> = undefined;
  #ctx = useJazzContext<InstanceOfSchema<A>>();
  #isAuthenticated = useIsAuthenticated();
  #listeners = new Set<() => void>();

  constructor(Schema: A, options?: { resolve?: ResolveQueryStrict<A, R> }) {
    // Effect to handle subscriptions
    createEffect(() => {
      const ctx = this.#ctx();

      untrack(() => {
        if (!ctx || !("me" in ctx)) {
          return this.update(undefined);
        }

        const me = ctx.me;

        const unsubscribe = subscribeToCoValue(
          anySchemaToCoSchema(Schema),
          me.id,
          {
            // @ts-expect-error The resolve query type isn't compatible with the anySchemaToCoSchema conversion
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

        onCleanup(() => {
          unsubscribe();
        });
      });
    });
  }

  update(value: Loaded<A, R> | undefined | null) {
    if (this.#value === value) return;
    this.#value = value;
    // Notify all listeners
    this.#listeners.forEach((listener) => listener());
  }

  logOut = () => {
    this.#ctx()?.logOut();
  };

  get current() {
    // Create a signal to track this access
    const [, setTrigger] = createSignal(0, { equals: false });

    // Add this component to listeners if not already added
    const listener = () => setTrigger(0);
    this.#listeners.add(listener);

    // Clean up listener when component unmounts
    onCleanup(() => {
      this.#listeners.delete(listener);
    });

    return this.#value;
  }

  get agent() {
    const ctx = this.#ctx();
    if (!ctx) {
      throw new Error("No context found");
    }

    return "me" in ctx ? ctx.me : ctx.guest;
  }

  get isAuthenticated() {
    return this.#isAuthenticated();
  }
}
