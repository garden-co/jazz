import {
  Account,
  AnonymousJazzAgent,
  LoadedAndRequired,
  BranchDefinition,
  InstanceOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded,
  MaybeLoaded,
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
export class CoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas,
> implements CoreCoDiscriminatedUnionSchema<Options>
{
  readonly collaborative = true as const;
  readonly builtin = "CoDiscriminatedUnion" as const;
  readonly getDefinition: () => CoDiscriminatedUnionSchemaDefinition<Options>;

  constructor(
    coreSchema: CoreCoDiscriminatedUnionSchema<Options>,
    private coValueClass: SchemaUnionConcreteSubclass<
      InstanceOfSchema<Options[number]>
    >,
  ) {
    this.getDefinition = coreSchema.getDefinition;
  }

  load(
    id: string,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<
    MaybeLoaded<
      Resolved<
        CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
        true
      >
    >
  > {
    return this.coValueClass.load(id, options) as any;
  }

  subscribe(
    id: string,
    options: SubscribeListenerOptions<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
      true
    >,
    listener: (
      value: Resolved<
        CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
        true
      >,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    // @ts-expect-error
    return this.coValueClass.subscribe(id, options, listener);
  }

  getCoValueClass(): SchemaUnionConcreteSubclass<
    InstanceOfSchema<Options[number]>
  > {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }
}

export function createCoreCoDiscriminatedUnionSchema<
  Options extends DiscriminableCoValueSchemas,
>(
  discriminator: string,
  schemas: Options,
): CoreCoDiscriminatedUnionSchema<Options> {
  return {
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
  };
}

type CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<
  Options extends DiscriminableCoValueSchemas,
> = LoadedAndRequired<
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<Options[number]>
>;
