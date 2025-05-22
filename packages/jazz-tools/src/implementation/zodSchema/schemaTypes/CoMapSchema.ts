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
import {
  AddHelpers,
  FullyOrPartiallyLoaded,
  Loaded,
  ResolveQuery,
  WithHelpers,
} from "../zodSchema.js";

export type CoMapSchema<
  Shape extends z.core.$ZodLooseShape,
  Config extends z.core.$ZodObjectConfig = z.core.$ZodObjectConfig,
  Helpers extends object = {},
  InstanceHelpers extends object = {},
> = z.core.$ZodObject<Shape, Config> &
  z.$ZodTypeDiscriminable & {
    collaborative: true;

    create<S>(
      this: S,
      init: Simplify<CoMapInitZod<Shape>>,
      options?:
        | {
            owner: Account | Group;
            unique?: CoValueUniqueness["uniqueness"];
          }
        | Account
        | Group,
    ): AddHelpers<
      S,
      (Shape extends Record<string, never>
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
        CoMap
    >;

    load<
      S,
      const R extends RefsToResolve<
        Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap
      > = true,
    >(
      this: S,
      id: string,
      options?: {
        resolve?: RefsToResolveStrict<
          Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
          R
        >;
        loadAs?: Account | AnonymousJazzAgent;
      },
    ): Promise<
      AddHelpers<
        S,
        Resolved<
          Simplify<CoMapInstanceCoValuesNullable<Shape>> & CoMap,
          R
        > | null
      >
    >;

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
      helpers: (Base: S) => T,
    ): S extends WithHelpers<infer Base, infer Helpers, infer InstanceHelpers>
      ? WithHelpers<Base, Helpers & T, InstanceHelpers>
      : WithHelpers<S, T, InstanceHelpers>;

    withInstanceHelper<
      S extends CoMapSchema<Shape, Config>,
      R extends ResolveQuery<S>,
      H extends (this: Loaded<S, R>, ...args: any[]) => any,
      N extends string,
    >(
      this: S,
      name: N,
      minResolve: R,
      helper: H,
    ): S extends WithHelpers<infer Base, infer Helpers, infer InstanceHelpers>
      ? WithHelpers<Base, Helpers, InstanceHelpers & { [key in N]: H }>
      : WithHelpers<S, Helpers, { [key in N]: H }>;
  };

export type CoMapInitZod<Shape extends z.core.$ZodLooseShape> = {
  [key in keyof Shape as Shape[key] extends z.core.$ZodOptional<any>
    ? key
    : never]?: FullyOrPartiallyLoaded<Shape[key]>;
} & {
  [key in keyof Shape as Shape[key] extends z.core.$ZodOptional<any>
    ? never
    : key]: FullyOrPartiallyLoaded<Shape[key]>;
};

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
