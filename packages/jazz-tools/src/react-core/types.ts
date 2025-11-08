import {
  type BranchDefinition,
  CoValueClassOrSchema,
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
  SchemaResolveQuery,
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

export interface UseSubscriptionOptions<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<S> = SchemaResolveQuery<S>,
> {
  /** Resolve query to specify which nested CoValues to load from the CoValue */
  resolve?: ResolveQueryStrict<S, R>;
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
}

export interface UseSubscriptionSelectorOptions<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
> {
  /** Select what data to return from the loaded or unloaded CoValue */
  select?: (value: MaybeLoaded<Loaded<S, R>>) => TSelectorReturn;
  /** Equality function to determine if the selected value has changed, defaults to `Object.is` */
  equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
}

export interface UseCoValueOptions<
  S extends CoValueClassOrSchema,
  // @ts-expect-error we can't statically enforce the schema's resolve query is a valid resolve query, but in practice it is
  R extends ResolveQuery<S> = SchemaResolveQuery<S>,
  TSelectorReturn = MaybeLoaded<Loaded<S, R>>,
> extends UseSubscriptionOptions<S, R>,
    UseSubscriptionSelectorOptions<S, R, TSelectorReturn> {}
