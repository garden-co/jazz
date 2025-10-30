import {
  Account,
  AnyZodOrCoValueSchema,
  BranchDefinition,
  CoFeed,
  Group,
  isAnyCoValueSchema,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  parseSubscribeRestArgs,
  ResolveQuery,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFeedSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { DefaultResolveQueryOfSchema } from "../typeConverters/DefaultResolveQueryOfSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withDefaultResolveQuery } from "../../schemaUtils.js";

export class CoFeedSchema<
  T extends AnyZodOrCoValueSchema,
  EagerlyLoaded extends boolean = false,
> implements CoreCoFeedSchema<T>
{
  collaborative = true as const;
  builtin = "CoFeed" as const;

  private isEagerlyLoaded: EagerlyLoaded = false as EagerlyLoaded;
  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will used by default.
   * @internal
   */
  get defaultResolveQuery(): DefaultResolveQuery<this> {
    if (!this.isEagerlyLoaded) {
      return false as DefaultResolveQuery<this>;
    }
    if (isAnyCoValueSchema(this.element) && this.element.defaultResolveQuery) {
      return {
        $each: this.element.defaultResolveQuery,
      } as DefaultResolveQuery<this>;
    }
    return true as DefaultResolveQuery<this>;
  }

  constructor(
    public element: T,
    private coValueClass: typeof CoFeed,
  ) {}

  create(
    init: CoFeedSchemaInit<T>,
    options?: { owner: Group } | Group,
  ): CoFeedInstance<T>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    init: CoFeedSchemaInit<T>,
    options?: { owner: Account | Group } | Account | Group,
  ): CoFeedInstance<T>;
  create(
    init: CoFeedSchemaInit<T>,
    options?: { owner: Account | Group } | Account | Group,
  ): CoFeedInstance<T> {
    return this.coValueClass.create(init as any, options) as CoFeedInstance<T>;
  }

  load<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error
    > = EagerlyLoaded extends false ? true : this["defaultResolveQuery"],
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<MaybeLoaded<Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withDefaultResolveQuery(options, this.defaultResolveQuery),
    );
  }

  unstable_merge<
    const R extends RefsToResolve<CoFeedInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    // @ts-expect-error
    return unstable_mergeBranchWithResolve(this.coValueClass, id, options);
  }

  subscribe(
    id: string,
    listener: (
      value: Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, true>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<
      CoFeedInstanceCoValuesMaybeLoaded<T>
      // @ts-expect-error
    > = EagerlyLoaded extends false ? true : this["defaultResolveQuery"],
  >(
    id: string,
    options: SubscribeListenerOptions<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
    listener: (
      value: Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe(id: string, ...args: any) {
    const { options, listener } = parseSubscribeRestArgs(args);
    return this.coValueClass.subscribe(
      id,
      // @ts-expect-error
      withDefaultResolveQuery(options, this.defaultResolveQuery),
      listener,
    );
  }

  getCoValueClass(): typeof CoFeed {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  resolved(): CoFeedSchema<T, true> {
    if (this.isEagerlyLoaded) {
      return this as CoFeedSchema<T, true>;
    }
    const copy = new CoFeedSchema<T, true>(this.element, this.coValueClass);
    copy.isEagerlyLoaded = true;
    return copy;
  }
}

export function createCoreCoFeedSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoFeedSchema<T> {
  return {
    collaborative: true as const,
    builtin: "CoFeed" as const,
    element,
    defaultResolveQuery: false,
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoFeedSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoFeed";
  element: T;
}

export type CoFeedInstance<T extends AnyZodOrCoValueSchema> = CoFeed<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoFeedInstanceCoValuesMaybeLoaded<T extends AnyZodOrCoValueSchema> =
  CoFeed<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>;

type DefaultResolveQuery<S> = S extends CoFeedSchema<
  infer ElementSchema,
  infer EagerlyLoaded
>
  ? EagerlyLoaded extends false
    ? false
    : DefaultResolveQueryOfSchema<ElementSchema> extends false
      ? true
      : { $each: DefaultResolveQueryOfSchema<ElementSchema> }
  : never;
