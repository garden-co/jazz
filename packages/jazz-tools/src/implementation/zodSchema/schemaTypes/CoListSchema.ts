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
import { z } from "../zodReExport.js";
import { WithHelpers } from "../zodSchema.js";

type CoListInit<T extends z.core.$ZodType> = Array<
  T extends z.core.$ZodOptional<any>
    ? InstanceOrPrimitiveOfSchemaCoValuesNullable<T>
    : NonNullable<InstanceOrPrimitiveOfSchemaCoValuesNullable<T>>
>;

export type CoListSchema<T extends z.core.$ZodType> = z.core.$ZodArray<T> & {
  collaborative: true;

  create: (
    items: CoListInit<T>,
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

  /** @deprecated Define your helper methods separately, in standalone functions. */
  withHelpers<S extends z.core.$ZodType, T extends object>(
    this: S,
    helpers: (Self: S) => T,
  ): WithHelpers<S, T>;

  getCoSchema: () => typeof CoList;
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
