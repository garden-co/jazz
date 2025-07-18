import {
  Account,
  CoFeed,
  Group,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOfSchema } from "../typeConverters/InstanceOfSchema.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";

type CoFeedInit<T extends z.core.$ZodType> = Array<
  T extends z.core.$ZodOptional<any>
    ? InstanceOrPrimitiveOfSchemaCoValuesNullable<T>
    : NonNullable<InstanceOrPrimitiveOfSchemaCoValuesNullable<T>>
>;

export type CoFeedSchema<T extends z.core.$ZodType> = z.core.$ZodCustom<
  CoFeed<InstanceOfSchema<T>>,
  unknown
> & {
  collaborative: true;
  builtin: "CoFeed";
  element: T;

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
};

export function enrichCoFeedSchema<T extends z.core.$ZodType>(
  schema: AnyCoFeedSchema<T>,
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
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as CoFeedSchema<T>;
}

// less precise version to avoid circularity issues and allow matching against
export type AnyCoFeedSchema<T extends z.core.$ZodType = z.core.$ZodType> =
  z.core.$ZodCustom<any, unknown> & {
    collaborative: true;
    builtin: "CoFeed";
    element: T;
  };

export type CoFeedInstance<T extends z.core.$ZodType> = CoFeed<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoFeedInstanceCoValuesNullable<T extends z.core.$ZodType> = CoFeed<
  InstanceOrPrimitiveOfSchemaCoValuesNullable<T>
>;
