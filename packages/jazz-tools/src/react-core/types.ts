import {
  CoValueClassOrSchema,
  ResolveQuery,
  SubscriptionScope,
} from "jazz-tools";

declare const subscriptionTag: unique symbol;

export type CoValueSubscription<
  S extends CoValueClassOrSchema,
  R extends ResolveQuery<S>,
> =
  | (SubscriptionScope<any> & {
      [subscriptionTag]: {
        schema: S;
        resolve: R;
      };
    })
  | null;
