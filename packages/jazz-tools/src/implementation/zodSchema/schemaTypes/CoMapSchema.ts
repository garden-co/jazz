import { CoValueUniqueness } from "cojson";
import z from "zod/v4";
import {
  Account,
  CoMap,
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
import { WithHelpers } from "../zodSchema.js";

export type CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  Config extends z.core.$ZodObjectConfig = z.core.$ZodObjectConfig,
> = z.core.$ZodObject<Shape, Config> &
  z.$ZodTypeDiscriminable & {
    collaborative: true;

    create: (
      init: Simplify<CoMapInitZod<Shape>>,
      options?:
        | {
            owner: Account | Group;
            unique?: CoValueUniqueness["uniqueness"];
          }
        | Account
        | Group,
    ) => (Shape extends Record<string, never>
      ? {}
      : {
          -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<
            Shape[key]
          >;
        }) &
      (unknown extends Config["out"][string]
        ? {}
        : {
            [key: string]: Config["out"][string];
          }) &
      CoMap;

    load<
      const R extends RefsToResolve<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
      > = true,
    >(
      id: string,
      options?: {
        resolve?: RefsToResolveStrict<
          Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
          R
        >;
        loadAs?: Account | AnonymousJazzAgent;
      },
    ): Promise<Resolved<
      Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
      R
    > | null>;

    subscribe<
      const R extends RefsToResolve<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
      > = true,
    >(
      id: string,
      options: SubscribeListenerOptions<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
        R
      >,
      listener: (
        value: Resolved<
          Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
          R
        >,
        unsubscribe: () => void,
      ) => void,
    ): () => void;

    findUnique(
      unique: CoValueUniqueness["uniqueness"],
      ownerID: string,
      as?: Account | Group | AnonymousJazzAgent,
    ): string;

    catchall<T extends z.core.$ZodType>(
      schema: T,
    ): CoMapSchema<Shape, z.core.$catchall<T>>;

    withHelpers<S extends z.core.$ZodType, T extends object>(
      this: S,
      helpers: (Self: S) => T,
    ): WithHelpers<S, T>;
  };

export type optionalKeys<Shape extends z.core.$ZodLooseShape> = {
  [key in keyof Shape]: Shape[key] extends z.core.$ZodOptional<any>
    ? key
    : never;
}[keyof Shape];

export type requiredKeys<Shape extends z.core.$ZodLooseShape> = {
  [key in keyof Shape]: Shape[key] extends z.core.$ZodOptional<any>
    ? never
    : key;
}[keyof Shape];

export type CoMapInitZod<Shape extends z.core.$ZodLooseShape> = {
  [key in optionalKeys<Shape>]?: NonNullable<
    InstanceOrPrimitiveOfSchemaCoValuesNullable<Shape[key]>
  >;
} & {
  [key in requiredKeys<Shape>]: NonNullable<
    InstanceOrPrimitiveOfSchemaCoValuesNullable<Shape[key]>
  >;
} & { [key in keyof Shape]?: unknown };

// less precise verion to avoid circularity issues and allow matching against
export type AnyCoMapSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
  Config extends z.core.$ZodObjectConfig = z.core.$ZodObjectConfig,
> = z.core.$ZodObject<Shape, Config> & { collaborative: true };

export type CoMapInstance<Shape extends z.core.$ZodLooseShape> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & CoMap;

export type CoMapInstanceCoValuesNullable<Shape extends z.core.$ZodLooseShape> =
  {
    -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesNullable<
      Shape[key]
    >;
  };
