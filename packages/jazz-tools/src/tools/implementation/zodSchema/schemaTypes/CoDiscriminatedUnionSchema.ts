import {
  Account,
  AnonymousJazzAgent,
  InstanceOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesNullable,
  Resolved,
  SchemaUnion,
  SchemaUnionConcreteSubclass,
  SubscribeListenerOptions,
  coOptionalDefiner,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export interface DiscriminableCoValueSchemaDefinition {
  discriminatorMap: z.core.$ZodDiscriminatedUnionInternals["propValues"];
}

export interface DiscriminableCoreCoValueSchema extends CoreCoValueSchema {
  getDefinition: () => DiscriminableCoValueSchemaDefinition;
}

export interface CoDiscriminatedUnionSchemaDefinition<
  Options extends DiscriminableCoValueSchemas,
> extends DiscriminableCoValueSchemaDefinition {
  discriminator: string;
  options: Options;
}

export type DiscriminableCoValueSchemas = [
  DiscriminableCoreCoValueSchema,
  ...DiscriminableCoreCoValueSchema[],
];

export interface CoreCoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas = DiscriminableCoValueSchemas,
> extends DiscriminableCoreCoValueSchema {
  builtin: "CoDiscriminatedUnion";
  getDefinition: () => CoDiscriminatedUnionSchemaDefinition<Options>;
}
export interface CoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas,
> extends CoreCoDiscriminatedUnionSchema<Options> {
  load(
    id: string,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
    },
  ): Promise<Resolved<
    CoDiscriminatedUnionInstanceCoValuesNullable<Options> & SchemaUnion,
    true
  > | null>;

  subscribe(
    id: string,
    options: SubscribeListenerOptions<
      CoDiscriminatedUnionInstanceCoValuesNullable<Options> & SchemaUnion,
      true
    >,
    listener: (
      value: Resolved<
        CoDiscriminatedUnionInstanceCoValuesNullable<Options> & SchemaUnion,
        true
      >,
      unsubscribe: () => void,
    ) => void,
  ): () => void;

  getCoValueClass: () => SchemaUnionConcreteSubclass<
    InstanceOfSchema<Options[number]>
  >;

  optional(): CoOptionalSchema<CoDiscriminatedUnionSchema<Options>>;
}

export function createCoreCoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas,
>(
  discriminator: string,
  schemas: Options,
): CoreCoDiscriminatedUnionSchema<Options> {
  const zodSchema = z.discriminatedUnion(discriminator, schemas as any);
  return Object.assign(zodSchema, {
    collaborative: true as const,
    builtin: "CoDiscriminatedUnion" as const,
    getDefinition: () => ({
      discriminator,
      get discriminatorMap() {
        const propValues: DiscriminableCoValueSchemaDefinition["discriminatorMap"] =
          {};
        for (const option of schemas) {
          const dm = option.getDefinition().discriminatorMap;
          if (!dm || Object.keys(dm).length === 0)
            throw new Error(
              `Invalid discriminated union option at index "${schemas.indexOf(option)}"`,
            );
          for (const [k, v] of Object.entries(dm)) {
            propValues[k] ??= new Set();
            for (const val of v) {
              propValues[k].add(val);
            }
          }
        }
        return propValues;
      },
      get options() {
        return schemas;
      },
    }),
  });
}

export function enrichCoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas,
>(
  schema: CoreCoDiscriminatedUnionSchema<Options>,
  coValueClass: SchemaUnionConcreteSubclass<InstanceOfSchema<Options[number]>>,
): CoDiscriminatedUnionSchema<Options> {
  const coValueSchema = Object.assign(schema, {
    load: (...args: [any, ...any]) => {
      return coValueClass.load(...args);
    },
    subscribe: (...args: [any, ...any[]]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    getCoValueClass: () => {
      return coValueClass;
    },

    optional: () => {
      return coOptionalDefiner(coValueSchema);
    },
  }) as unknown as CoDiscriminatedUnionSchema<Options>;
  return coValueSchema;
}

type CoDiscriminatedUnionInstanceCoValuesNullable<
  Options extends DiscriminableCoValueSchemas,
> = NonNullable<InstanceOrPrimitiveOfSchemaCoValuesNullable<Options[number]>>;
