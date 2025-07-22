import {
  CoValueOrZodSchema,
  ResolveQuery,
  ResolveQueryStrict,
  SubscriptionScope,
  anySchemaToCoSchema,
} from "jazz-tools";
import { Accessor, createEffect, createSignal, onCleanup } from "solid-js";
import { useJazzManager } from "../context/jazz.js";

type SubscriptionParams<
  S extends CoValueOrZodSchema,
  R extends ResolveQuery<S>,
> = {
  readonly Schema: S;
  readonly id: string | undefined | null;
  readonly options?: {
    readonly resolve?: ResolveQueryStrict<S, R>;
  };
};

const useCoValueSubscription = <
  S extends CoValueOrZodSchema,
  const R extends ResolveQuery<S>,
>(
  params: Accessor<SubscriptionParams<S, R>>,
) => {
  const contextManager = useJazzManager();
  const [scope, setScope] = createSignal<SubscriptionScope<any>>();

  const createSubscription = () => {
    const { id, Schema, options } = params();

    if (!id) return undefined;

    const node = contextManager().getCurrentValue()!.node;

    const subscriptionScope = new SubscriptionScope<any>(
      node,
      options?.resolve ?? true,
      id,
      {
        ref: anySchemaToCoSchema(Schema),
        optional: true,
      },
    );

    return subscriptionScope;
  };

  createEffect(() => {
    const subscription = createSubscription();

    setScope(subscription);

    const unsubscribe = contextManager().subscribe(() => {
      subscription?.destroy();
      setScope(createSubscription());
    });

    onCleanup(() => {
      unsubscribe();
      subscription?.destroy();
    });
  });

  return scope;
};

type UseCoStateParams<
  S extends CoValueOrZodSchema,
  R extends ResolveQuery<S>,
> = SubscriptionParams<S, R>;

export const useCoState = <
  S extends CoValueOrZodSchema,
  const R extends ResolveQuery<S>,
>(
  params: Accessor<UseCoStateParams<S, R>>,
) => {
  const subscription = useCoValueSubscription(params);

  const value = () => subscription()?.getCurrentValue() ?? undefined;

  return value;
};
