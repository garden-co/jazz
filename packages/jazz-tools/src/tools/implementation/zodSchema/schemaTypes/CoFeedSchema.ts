import {
  Account,
  AnyZodOrCoValueSchema,
  BranchDefinition,
  CoFeed,
  Group,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
  ResolveQuery,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFeedSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";

// TODO add type parameter for default resolve query
export class CoFeedSchema<T extends AnyZodOrCoValueSchema>
  implements CoreCoFeedSchema<T>
{
  collaborative = true as const;
  builtin = "CoFeed" as const;

  /**
   * The default resolve query to be used when loading instances of this schema.
   * Defaults to `false`, meaning that no resolve query will used by default.
   * @internal
   */
  public defaultResolveQuery: CoreResolveQuery = false;

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
    const R extends RefsToResolve<CoFeedInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<MaybeLoaded<Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(id, options);
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
    const R extends RefsToResolve<CoFeedInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options: SubscribeListenerOptions<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
    listener: (
      value: Resolved<CoFeedInstanceCoValuesMaybeLoaded<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe(...args: [any, ...any[]]) {
    // @ts-expect-error
    return this.coValueClass.subscribe(...args);
  }

  getCoValueClass(): typeof CoFeed {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  resolved(): CoFeedSchema<T> {
    const copy = new CoFeedSchema(this.element, this.coValueClass);
    copy.defaultResolveQuery = true;
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
