import {
  Account,
  AnonymousJazzAgent,
  LoadedAndRequired,
  BranchDefinition,
  Settled,
  ResolveQuery,
  ResolveQueryStrict,
  Loaded,
  SchemaUnion,
  SubscribeListenerOptions,
  SubscribeRestArgs,
  coOptionalDefiner,
  loadCoValueWithoutMe,
  parseSubscribeRestArgs,
  subscribeToCoValueWithoutMe,
  CoreAccountSchema,
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
  DefaultResolveQuery extends ResolveQuery<
    CoreCoDiscriminatedUnionSchema<Options>
  > = true,
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
    private coValueClass: SchemaUnion,
  ) {
    this.getDefinition = coreSchema.getDefinition;
  }

  load<
    const R extends ResolveQuery<
      CoreCoDiscriminatedUnionSchema<Options>
    > = DefaultResolveQuery,
  >(
    id: string,
    options?: {
      resolve?: ResolveQueryStrict<CoreCoDiscriminatedUnionSchema<Options>, R>;
      loadAs?: Loaded<CoreAccountSchema, true> | AnonymousJazzAgent;
      skipRetry?: boolean;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<Settled<CoreCoDiscriminatedUnionSchema<Options>, R>> {
    return loadCoValueWithoutMe(
      this as CoreCoDiscriminatedUnionSchema<Options>,
      id,
      withSchemaResolveQuery(this, options),
    );
  }

  subscribe<
    const R extends ResolveQuery<
      CoreCoDiscriminatedUnionSchema<Options>
    > = DefaultResolveQuery,
  >(
    id: string,
    options: SubscribeListenerOptions<
      CoreCoDiscriminatedUnionSchema<Options>,
      R
    >,
    listener: (
      value: Loaded<CoreCoDiscriminatedUnionSchema<Options>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    return subscribeToCoValueWithoutMe(
      this as CoreCoDiscriminatedUnionSchema<Options>,
      id,
      withSchemaResolveQuery(this, options),
      listener as any,
    );
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }

  /**
   * Adds a default resolve query to be used when loading instances of this schema.
   * This resolve query will be used when no resolve query is provided to the load method.
   */
  resolved<
    const R extends ResolveQuery<
      CoreCoDiscriminatedUnionSchema<Options>
    > = true,
  >(
    resolveQuery: ResolveQueryStrict<
      CoreCoDiscriminatedUnionSchema<Options>,
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
