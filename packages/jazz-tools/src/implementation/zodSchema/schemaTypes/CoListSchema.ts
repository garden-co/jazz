import z from "zod/v4";
import {
  Account,
  CoList,
  Group,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { WithHelpers } from "../zodSchema.js";

export type CoListSchema<T extends z.core.$ZodType> = z.core.$ZodArray<T> & {
  collaborative: true;

  create: (
    items: InstanceOrPrimitiveOfSchema<T>[],
    options?: { owner: Account | Group } | Account | Group,
  ) => CoList<InstanceOrPrimitiveOfSchema<T>>;

  load<const R extends RefsToResolve<CoListInstanceCoValuesNullable<T>> = true>(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesNullable<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<Resolved<CoListInstanceCoValuesNullable<T>, R> | null>;

  subscribe<
    const R extends RefsToResolve<CoListInstanceCoValuesNullable<T>> = true,
  >(
    id: string,
    options: SubscribeListenerOptions<CoListInstanceCoValuesNullable<T>, R>,
    listener: (
      value: Resolved<CoListInstanceCoValuesNullable<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  withHelpers<S extends z.core.$ZodType, T extends object>(
    this: S,
    helpers: (Self: S) => T,
  ): WithHelpers<S, T, {}>;
};

// less precise verion to avoid circularity issues and allow matching against
export type AnyCoListSchema<T extends z.core.$ZodType = z.core.$ZodType> =
  z.core.$ZodArray<T> & { collaborative: true };

export type CoListInstance<T extends z.core.$ZodType> = CoList<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoListInstanceCoValuesNullable<T extends z.core.$ZodType> = CoList<
  InstanceOrPrimitiveOfSchemaCoValuesNullable<T>
>;
