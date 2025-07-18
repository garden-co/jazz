import {
  Account,
  AnyZodOrCoValueSchema,
  CoFeed,
  Group,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  Simplify,
  SubscribeListenerOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { NotNull } from "../typeConverters/NotNull.js";
import { z } from "../zodReExport.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

type CoFeedInit<T extends AnyZodOrCoValueSchema> = Simplify<
  Array<NotNull<InstanceOrPrimitiveOfSchemaCoValuesNullable<T>>>
>;

export interface CoFeedSchema<T extends AnyZodOrCoValueSchema>
  extends CoreCoFeedSchema<T> {
  create(
    init: CoFeedInit<T>,
    options?: { owner: Account | Group } | Account | Group,
  ): CoFeedInstance<T>;

  load<const R extends RefsToResolve<CoFeedInstanceCoValuesNullable<T>> = true>(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoFeedInstanceCoValuesNullable<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoFeedInstanceCoValuesNullable<T>, R> | null>;

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

  getCoValueClass: () => typeof CoFeed;
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

export function enrichCoFeedSchema<T extends AnyZodOrCoValueSchema>(
  schema: CoreCoFeedSchema<T>,
  coValueClass: typeof CoFeed,
): CoFeedSchema<T> {
  return Object.assign(schema, {
    create: (...args: [any, ...any[]]) => {
      return coValueClass.create(...args);
    },
    load: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.load(...args);
    },
    subscribe: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    withHelpers: (helpers: (Self: CoreCoFeedSchema<T>) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as CoFeedSchema<T>;
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
