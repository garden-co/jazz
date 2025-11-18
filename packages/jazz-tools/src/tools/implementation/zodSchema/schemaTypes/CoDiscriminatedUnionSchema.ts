import {
  Account,
  AnonymousJazzAgent,
  LoadedAndRequired,
  BranchDefinition,
  InstanceOfSchema,
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SchemaUnion,
  SchemaUnionConcreteSubclass,
  SubscribeListenerOptions,
  coOptionalDefiner,
} from "../../../internal.js";
import { z } from "../zodReExport.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema, CoreResolveQuery } from "./CoValueSchema.js";
import { withSchemaResolveQuery } from "../../schemaUtils.js";

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
  DefaultResolveQuery extends CoreResolveQuery = true,
> implements CoreCoDiscriminatedUnionSchema<Options>
{
  readonly collaborative = true as const;
  readonly builtin = "CoDiscriminatedUnion" as const;
  readonly getDefinition: () => CoDiscriminatedUnionSchemaDefinition<Options>;

  /**
   * Default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   * @default true
   */
  resolveQuery: DefaultResolveQuery = true as DefaultResolveQuery;

  constructor(
    coreSchema: CoreCoDiscriminatedUnionSchema<Options>,
    private coValueClass: SchemaUnionConcreteSubclass<
      InstanceOfSchema<Options[number]>
    >,
  ) {
    this.getDefinition = coreSchema.getDefinition;
  }

  load<
    const R extends RefsToResolve<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion
      // @ts-expect-error
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<
        CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
        R
      >;
      loadAs?: Account | AnonymousJazzAgent;
      skipRetry?: boolean;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<
    MaybeLoaded<
      Resolved<
        CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
        R
      >
    >
  > {
    return this.coValueClass.load(
      id,
      // @ts-expect-error
      withSchemaResolveQuery(options, this.resolveQuery),
    ) as any;
  }

  subscribe<
    const R extends RefsToResolve<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion
      // @ts-expect-error
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
      R
    >,
    listener: (
      value: Resolved<
        CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
        R
      >,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    // @ts-expect-error
    return this.coValueClass.subscribe(
      id,
      withSchemaResolveQuery(options, this.resolveQuery),
      listener,
    );
  }

  getCoValueClass(): SchemaUnionConcreteSubclass<
    InstanceOfSchema<Options[number]>
  > {
    return this.coValueClass;
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<
    const R extends RefsToResolve<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion
    > = true,
  >(
    resolveQuery: RefsToResolveStrict<
      CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<Options> & SchemaUnion,
      R
    >,
  ): CoDiscriminatedUnionSchema<Options, R> {
    const definition = this.getDefinition();
    const coreSchema: CoreCoDiscriminatedUnionSchema<Options> =
      createCoreCoDiscriminatedUnionSchema(
        definition.discriminator,
        definition.options,
      );
    const copy = new CoDiscriminatedUnionSchema<Options, R>(
      coreSchema,
      this.coValueClass,
    );
    copy.resolveQuery = resolveQuery as R;
    return copy;
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
    resolveQuery: true as const,
  };
}

type CoDiscriminatedUnionInstanceCoValuesMaybeLoaded<
  Options extends DiscriminableCoValueSchemas,
> = LoadedAndRequired<
  InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<Options[number]>
>;
