import {
  Account,
  AnyZodOrCoValueSchema,
  CoFeed,
  Group,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
  coOptionalDefiner,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoFeedSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export class CoFeedSchema<T extends AnyZodOrCoValueSchema>
  implements CoreCoFeedSchema<T>
{
  collaborative = true as const;
  builtin = "CoFeed" as const;

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

  load<const R extends RefsToResolve<CoFeedInstanceCoValuesNullable<T>> = true>(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesNullable<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoFeedInstanceCoValuesNullable<T>, R> | null> {
    // @ts-expect-error
    return this.coValueClass.load(id, options);
  }

  subscribe(
    id: string,
    listener: (
      value: Resolved<CoFeedInstanceCoValuesNullable<T>, true>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;
  subscribe<
    const R extends RefsToResolve<CoFeedInstanceCoValuesNullable<T>> = true,
  >(
    id: string,
    options: SubscribeListenerOptions<CoFeedInstanceCoValuesNullable<T>, R>,
    listener: (
      value: Resolved<CoFeedInstanceCoValuesNullable<T>, R>,
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
}

export function createCoreCoFeedSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoFeedSchema<T> {
  return {
    collaborative: true as const,
    builtin: "CoFeed" as const,
    element,
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

export type CoFeedInstanceCoValuesNullable<T extends AnyZodOrCoValueSchema> =
  CoFeed<InstanceOrPrimitiveOfSchemaCoValuesNullable<T>>;
