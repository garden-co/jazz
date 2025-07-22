import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  InstanceOfSchema,
  JazzContextManager,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  SubscriptionScope,
  anySchemaToCoSchema,
} from "jazz-tools";
import {
  Accessor,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
} from "solid-js";
import { useJazzManager } from "../context/jazz.js";

export function getCurrentAccountFromContextManager<Acc extends Account>(
  contextManager: JazzContextManager<Acc, any>,
) {
  const context = contextManager.getCurrentValue();

  if (!context) {
    throw new Error("No context found");
  }

  return "me" in context ? context.me : context.guest;
}

type AccountSubscriptionParams<
  S extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<S>,
> = {
  readonly Schema: S;
  readonly options?: {
    readonly resolve?: ResolveQueryStrict<S, R>;
  };
};

const useAccountSubscription = <
  S extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<S>,
>(
  params: Accessor<AccountSubscriptionParams<S, R>>,
) => {
  const contextManager = useJazzManager();
  const [scope, setScope] = createSignal<SubscriptionScope<any>>();

  const createSubscription = () => {
    const agent = getCurrentAccountFromContextManager(contextManager());

    if (agent._type === "Anonymous") return undefined;

    const { Schema, options } = params();

    const resolve = options?.resolve ?? true;
    const node = contextManager().getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(node, resolve, agent.id, {
      ref: anySchemaToCoSchema(Schema),
      optional: true,
    });

    return subscription;
  };

  createEffect(() => {
    const subscription = createSubscription();

    setScope(subscription);

    const unsubscribe = contextManager().subscribe(() => {
      subscription?.destroy();
      setScope(createSubscription()); // create fresh subscription when context state changes
    });

    onCleanup(() => {
      subscription?.destroy();
      unsubscribe();
    });
  });

  return scope;
};

type UseAccountParams<
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A>,
> = {
  readonly AccountSchema: A;
  readonly options?: {
    readonly resolve?: ResolveQueryStrict<A, R>;
  };
};

type UseAccountResult<
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A>,
> = {
  readonly me: Accessor<Loaded<A, R> | undefined>;
  readonly agent: Accessor<AnonymousJazzAgent | Loaded<A, true>>;
  readonly logOut: () => void;
};

export const useAccount = <
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A> = true,
>(
  params: Accessor<UseAccountParams<A, R>>,
) => {
  const contextManager = useJazzManager<InstanceOfSchema<A>>();

  const subscription = useAccountSubscription(() => ({
    Schema: params().AccountSchema,
    options: params().options,
  }));

  const agent = createMemo(() =>
    getCurrentAccountFromContextManager(contextManager()),
  );
  const me = () => subscription()?.getCurrentValue() ?? undefined;
  const logOut = () => contextManager().logOut();

  return {
    me,
    agent,
    logOut,
  } as UseAccountResult<A, R>;
};
